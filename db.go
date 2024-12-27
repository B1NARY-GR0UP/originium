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
	"errors"
	"os"
	"sync"
	"sync/atomic"

	"github.com/B1NARY-GR0UP/originium/pkg/kway"
	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/pkg/types"
)

var errMkDir = errors.New("failed to create db dir")

type DB struct {
	once     sync.Once
	mu       sync.RWMutex
	config   Config
	dir      string
	memtable *memtable
	manager  *levelManager
	state    uint32
	logger   logger.Logger
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
		config: config,
		dir:    dir,
		logger: logger.GetLogger(),
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

	atomic.StoreUint32(&db.state, uint32(StateOpened))
	return db, nil
}

func (db *DB) Close() {
	db.once.Do(func() {
		imt := db.memtable.freeze()
		if imt.size() > 0 {
			db.flushImmutable(imt)
		} else {
			if err := imt.wal.Delete(); err != nil {
				db.logger.Panicf("failed to delete immutable wal file: %v", err)
			}
		}
		atomic.StoreUint32(&db.state, uint32(StateClosed))
	})
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

	// scan memtable
	mtScan := db.memtable.scan(start, end)

	// scan sstables
	sstScan := db.manager.scan(start, end)

	// merge result
	return kvs(kway.Merge(sstScan, mtScan))
}

func (db *DB) rawset(entry types.Entry) {
	db.memtable.set(entry)
	// TODO: optimize, do not block set
	if db.memtable.size() >= db.config.MemtableByteThreshold {
		imt := db.memtable.freeze()
		db.flushImmutable(imt)
		db.manager.checkAndCompact()
		// TODO: move in advance
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
