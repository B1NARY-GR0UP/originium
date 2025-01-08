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
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/pkg/skiplist"
	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/B1NARY-GR0UP/originium/pkg/utils"
	"github.com/B1NARY-GR0UP/originium/wal"
)

type memtable struct {
	mu       sync.RWMutex
	logger   logger.Logger
	skiplist *skiplist.SkipList
	wal      *wal.WAL
	dir      string
	readOnly bool
}

func newMemtable(dir string, maxLevel int, p float64) *memtable {
	l, err := wal.Create(dir)
	if err != nil {
		panic(err)
	}
	return &memtable{
		logger:   logger.GetLogger(),
		skiplist: skiplist.New(maxLevel, p),
		wal:      l,
		dir:      dir,
		readOnly: false,
	}
}

func (mt *memtable) recover() {
	mt.mu.Lock()
	defer mt.mu.Unlock()
	defer utils.Elapsed(time.Now(), mt.logger, "memtable recover")

	files, err := os.ReadDir(mt.dir)
	if err != nil {
		mt.logger.Panicf("read dir %v failed: %v", mt.dir, err)
	}

	var walFiles []string
	for _, file := range files {
		if !file.IsDir() && path.Ext(file.Name()) == ".log" && wal.CompareVersion(wal.ParseVersion(file.Name()), mt.wal.Version()) < 0 {
			walFiles = append(walFiles, path.Join(mt.dir, file.Name()))
		}
	}

	if len(walFiles) == 0 {
		return
	}

	slices.Sort(walFiles)

	mt.logger.Infof("found %d wal file, recovery start", len(walFiles))
	// merge wal files
	for _, file := range walFiles {
		l, err := wal.Open(file)
		if err != nil {
			mt.logger.Panicf("open wal %v failed: %v", file, err)
		}
		entries, err := l.Read()
		if err != nil {
			mt.logger.Panicf("read wal %v failed: %v", file, err)
		}
		for _, entry := range entries {
			mt.skiplist.Set(entry)
			if err = mt.wal.Write(entry); err != nil {
				mt.logger.Panicf("write wal failed: %v", err)
			}
		}
		if err = l.Delete(); err != nil {
			mt.logger.Panicf("delete wal %v failed: %v", file, err)
		}
	}
	mt.logger.Infof("recovery finished")
}

func (mt *memtable) set(entry types.Entry) {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if mt.readOnly {
		mt.logger.Panicf("write readonly memtable")
	}

	mt.skiplist.Set(entry)
	if err := mt.wal.Write(entry); err != nil {
		mt.logger.Panicf("write wal failed: %v", err)
	}
	mt.logger.Infof("memtable set [key: %v] [value: %v] [tombstone: %v]", entry.Key, string(entry.Value), entry.Tombstone)
}

func (mt *memtable) get(key types.Key) (types.Entry, bool) {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Get(key)
}

func (mt *memtable) scan(start, end types.Key) []types.Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Scan(start, end)
}

func (mt *memtable) all() []types.Entry {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.All()
}

func (mt *memtable) size() int {
	mt.mu.RLock()
	defer mt.mu.RUnlock()

	return mt.skiplist.Size()
}

func (mt *memtable) freeze() {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	if err := mt.wal.Close(); err != nil {
		mt.logger.Panicf("wal close failed: %v", err)
	}
	mt.readOnly = true
}

func (mt *memtable) reset() *memtable {
	mt.mu.Lock()
	defer mt.mu.Unlock()

	l, err := mt.wal.Reset()
	if err != nil {
		mt.logger.Panicf("wal reset failed: %v", err)
	}
	return &memtable{
		logger:   logger.GetLogger(),
		skiplist: mt.skiplist.Reset(),
		wal:      l,
		dir:      mt.dir,
		readOnly: false,
	}
}
