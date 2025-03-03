// Copyright 2024 BINARY Members
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package originium

import (
	"container/list"
	"errors"
	"os"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/B1NARY-GR0UP/originium/pkg/kway"
	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/types"
)

var errMkDir = errors.New("failed to create db dir")

type DB struct {
	mu sync.RWMutex

	config Config
	logger logger.Logger
	dir    string
	state  uint32

	memtable   *memtable
	immutables *list.List
	flushC     chan *memtable

	manager *levelManager

	closed chan struct{}
	closeC chan struct{}
}

type State uint32

const (
	_ State = iota
	StateInitialize
	StateOpened
	StateClosed
)

func Open(dir string, config Config) (*DB, error) {
	if err := config.validate(); err != nil {
		return nil, err
	}

	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, errMkDir
	}

	db := &DB{
		config:     config,
		dir:        dir,
		logger:     logger.GetLogger(),
		immutables: list.New(),
		flushC:     make(chan *memtable, config.ImmutableBuffer),
		closeC:     make(chan struct{}),
		closed:     make(chan struct{}),
	}

	atomic.StoreUint32(&db.state, uint32(StateInitialize))

	// recover from exist wal
	mt := newMemtable(dir, config.SkipListMaxLevel, config.SkipListP)
	mt.recover()

	// recover from exist db
	lm := newLevelManager(dir, config.L0TargetNum, config.LevelRatio, config.DataBlockByteThreshold)
	lm.recover()

	db.memtable = mt
	db.manager = lm

	go db.run()
	return db, nil
}

func (db *DB) Close() {
	defer atomic.StoreUint32(&db.state, uint32(StateClosed))
	db.closeC <- struct{}{}

	mt := db.memtable
	mt.freeze()
	if mt.size() > 0 {
		db.flushImmutable(mt)
	} else {
		if err := mt.wal.Delete(); err != nil {
			db.logger.Panicf("failed to delete immutable wal file: %v", err)
		}
	}

	<-db.closed
}

func (db *DB) Read(fn TxnFunc) error {
	return nil
}

func (db *DB) Write(fn TxnFunc) error {
	return nil
}

func (db *DB) NewTxn(write bool) *Txn {
	return nil
}

func (db *DB) State() State {
	return State(atomic.LoadUint32(&db.state))
}

func (db *DB) Set(key string, value []byte) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.rawset(types.Entry{
		Key:       key,
		Value:     value,
		Tombstone: false,
	})
}

func (db *DB) Delete(key string) {
	db.mu.Lock()
	defer db.mu.Unlock()

	db.rawset(types.Entry{
		Key:       key,
		Value:     []byte{},
		Tombstone: true,
	})
}

func (db *DB) Get(key string) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// search memtable
	mtEntry, ok := db.memtable.get(key)
	if ok {
		return value(mtEntry)
	}

	// search immutables
	for e := db.immutables.Back(); e != nil; e = e.Prev() {
		imt := e.Value.(*memtable)
		imtEntry, ok := imt.get(key)
		if ok {
			return value(imtEntry)
		}
	}

	// search sstables
	sstEntry, ok := db.manager.search(key)
	if ok {
		return value(sstEntry)
	}
	return nil, false
}

// Scan [start, end)
func (db *DB) Scan(start, end string) []types.KV {
	db.mu.RLock()
	defer db.mu.RUnlock()

	var scan [][]types.Entry

	// scan memtable
	scan = append(scan, db.memtable.scan(start, end))

	// scan immutables
	for e := db.immutables.Back(); e != nil; e = e.Prev() {
		imt := e.Value.(*memtable)
		scan = append(scan, imt.scan(start, end))
	}

	// scan sstables
	scan = append(scan, db.manager.scan(start, end))

	slices.Reverse(scan)
	// merge result
	return kvs(kway.Merge(scan...))
}

func (db *DB) rawset(entry types.Entry) {
	db.memtable.set(entry)

	if db.memtable.size() >= db.config.MemtableByteThreshold {
		db.memtable.freeze()
		imt := db.memtable

		db.flushC <- imt
		db.immutables.PushBack(imt)

		db.memtable = db.memtable.reset()
	}
}

func (db *DB) flushImmutable(imt *memtable) {
	// flush immutable memtable to L0
	if err := db.manager.flushToL0(imt.all()); err != nil {
		db.logger.Panicf("failed to flush immutable memtable: %v", err)
	}
	// delete wal file
	if err := imt.wal.Delete(); err != nil {
		db.logger.Panicf("failed to delete immutable wal file: %v", err)
	}
}

func (db *DB) run() {
	atomic.StoreUint32(&db.state, uint32(StateOpened))
	var closed bool
LOOP:
	for {
		select {
		case imt := <-db.flushC:
			db.flushImmutable(imt)
			db.manager.checkAndCompact()

			db.mu.Lock()
			db.immutables.Remove(db.immutables.Back())
			db.mu.Unlock()

			if closed && len(db.flushC) == 0 {
				break LOOP
			}
		case <-db.closeC:
			closed = true
			if len(db.flushC) > 0 {
				continue
			}
			break LOOP
		}
	}
	close(db.closed)
}

func value(entry types.Entry) ([]byte, bool) {
	if entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func kvs(entries []types.Entry) []types.KV {
	var res []types.KV
	for _, entry := range entries {
		if entry.Tombstone {
			continue
		}
		res = append(res, types.KV{
			K: entry.Key,
			V: entry.Value,
		})
	}
	return res
}
