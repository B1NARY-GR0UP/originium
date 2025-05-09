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
	"sync"
	"sync/atomic"

	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/types"
)

var (
	ErrMkDir    = errors.New("failed to create db dir")
	ErrDBClosed = errors.New("db closed")
)

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
	oracle  *oracle

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

	if err := os.MkdirAll(dir, config.FileMode); err != nil {
		return nil, ErrMkDir
	}

	db := &DB{
		config:     config,
		dir:        dir,
		logger:     logger.GetLogger(),
		immutables: list.New(),
		oracle:     newOracle(),
		flushC:     make(chan *memtable, config.ImmutableBuffer),
		closeC:     make(chan struct{}),
		closed:     make(chan struct{}),
	}

	atomic.StoreUint32(&db.state, uint32(StateInitialize))

	// recover from exist wal
	mt := newMemtable(dir, config.SkipListMaxLevel, config.SkipListP)
	walMaxVersion := mt.recover()

	// recover from exist data file
	lm := newLevelManager(db)
	dbMaxVersion := lm.recover()

	db.memtable = mt
	db.manager = lm

	// recover oracle
	maxTs := uint64(max(walMaxVersion, dbMaxVersion))
	db.oracle.readMark.Done(maxTs)
	db.oracle.commitMark.Done(maxTs)
	db.oracle.nextTs = maxTs + 1

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
			db.logger.Warnf("failed to delete immutable wal file: %v", err)
		}
	}

	<-db.closed
}

func (db *DB) View(fn TxnFunc) error {
	if db.State() == StateClosed {
		return ErrDBClosed
	}
	txn := db.Begin(false)
	defer txn.Discard()

	return fn(txn)
}

func (db *DB) Update(fn TxnFunc) error {
	if db.State() == StateClosed {
		return ErrDBClosed
	}
	txn := db.Begin(true)
	defer txn.Discard()

	if err := fn(txn); err != nil {
		return err
	}

	return txn.Commit()
}

func (db *DB) Begin(update bool) *Txn {
	txn := &Txn{
		readTs:   db.oracle.readTs(),
		readOnly: !update,
		db:       db,
	}

	if update {
		txn.pendingWrites = make(map[types.Key]types.Entry)
		txn.writesFp = make(map[uint64]struct{})
	}
	return txn
}

func (db *DB) State() State {
	return State(atomic.LoadUint32(&db.state))
}

func (db *DB) search(key types.Key) ([]byte, bool) {
	db.mu.RLock()
	defer db.mu.RUnlock()

	// search memtable
	mtEntry, ok := db.memtable.lowerBound(key)
	if ok && types.IsSameKey(key, mtEntry.Key) {
		return types.Value(mtEntry)
	}

	// search immutables
	for e := db.immutables.Back(); e != nil; e = e.Prev() {
		imt := e.Value.(*memtable)
		imtEntry, ok := imt.lowerBound(key)
		if ok && types.IsSameKey(key, imtEntry.Key) {
			return types.Value(imtEntry)
		}
	}

	// search sstables
	sstEntry, ok := db.manager.searchLowerBound(key)
	if ok && types.IsSameKey(key, sstEntry.Key) {
		return types.Value(sstEntry)
	}

	return nil, false
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
