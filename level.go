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
	"fmt"
	"io"
	"os"
	"path"
	"slices"
	"sync"
	"time"

	"github.com/B1NARY-GR0UP/originium/pkg/filter"
	"github.com/B1NARY-GR0UP/originium/pkg/kway"
	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/table"
	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/B1NARY-GR0UP/originium/utils"
)

type levelManager struct {
	mu            sync.Mutex
	dir           string
	l0TargetNum   int
	ratio         int
	dataBlockSize int
	// list.Element: tableHandle
	levels []*list.List
	logger logger.Logger
}

type tableHandle struct {
	// list index of table within a level
	levelIdx int
	// bloom filter
	filter filter.Filter
	// index of data blocks in this sstable
	dataBlockIndex table.Index
}

func newLevelManager(dir string, l0TargetNum, ratio, blockSize int) *levelManager {
	return &levelManager{
		dir:           dir,
		l0TargetNum:   l0TargetNum,
		ratio:         ratio,
		dataBlockSize: blockSize,
		logger:        logger.GetLogger(),
	}
}

func (lm *levelManager) recover() {
	lm.mu.Lock()
	defer lm.mu.Unlock()
	defer utils.Elapsed(time.Now(), lm.logger, "level index recover")

	files, err := os.ReadDir(lm.dir)
	if err != nil {
		lm.logger.Panicf("read dir %v failed: %v", lm.dir, err)
	}

	var dbFiles []string
	for _, file := range files {
		if !file.IsDir() && path.Ext(file.Name()) == ".db" {
			dbFiles = append(dbFiles, file.Name())
		}
	}

	if len(dbFiles) == 0 {
		return
	}

	slices.Sort(dbFiles)

	for _, file := range dbFiles {
		level, idx, err := parseFileName(file)
		if err != nil {
			lm.logger.Panicf("failed to parse file name %s: %v", file, err)
		}

		fd, err := os.Open(path.Join(lm.dir, file))
		if err != nil {
			lm.logger.Panicf("failed to open file %s: %v", file, err)
		}

		// read and decode footer
		_, err = fd.Seek(-40, io.SeekEnd)
		if err != nil {
			lm.logger.Panicf("failed to seek footer: %v", err)
		}

		footerBytes := make([]byte, 40)
		_, err = fd.Read(footerBytes)
		if err != nil {
			lm.logger.Panicf("failed to read footer: %v", err)
		}

		var footer table.Footer
		if err = footer.Decode(footerBytes); err != nil {
			lm.logger.Panicf("failed to decode footer: %v", err)
		}

		// read and decode index block
		_, err = fd.Seek(int64(footer.IndexBlock.Offset), io.SeekStart)
		if err != nil {
			lm.logger.Panicf("failed to seek index: %v", err)
		}

		indexBytes := make([]byte, footer.IndexBlock.Length)
		_, err = fd.Read(indexBytes)
		if err != nil {
			lm.logger.Panicf("failed to read index: %v", err)
		}

		var index table.Index
		if err = index.Decode(indexBytes); err != nil {
			lm.logger.Panicf("failed to decode index: %v", err)
		}

		// read and decode data blocks
		_, err = fd.Seek(int64(index.DataBlock.Offset), io.SeekStart)
		if err != nil {
			lm.logger.Panicf("failed to seek data block: %v", err)
		}

		dataBlockBytes := make([]byte, index.DataBlock.Length)
		_, err = fd.Read(dataBlockBytes)
		if err != nil {
			lm.logger.Panicf("failed to read data block: %v", err)
		}

		var dataBlock table.Data
		if err = dataBlock.Decode(dataBlockBytes); err != nil {
			lm.logger.Panicf("failed to decode data block: %v", err)
		}

		// build bloom filter
		bf := filter.Build(dataBlock.Entries)

		for len(lm.levels) <= level {
			lm.levels = append(lm.levels, list.New())
		}

		th := tableHandle{
			levelIdx:       idx,
			filter:         *bf,
			dataBlockIndex: index,
		}

		lm.levels[level].PushBack(th)
	}
}

func (lm *levelManager) search(key types.Key) (types.Entry, bool) {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if len(lm.levels) == 0 {
		return types.Entry{}, false
	}

	for level, tables := range lm.levels {
		for e := tables.Front(); e != nil; e = e.Next() {
			th := e.Value.(tableHandle)

			// search bloom filter
			if !th.filter.Contains(key) {
				// not in this sstable, search next one
				continue
			}

			// determine which data block the key is in
			dataBlockHandle, ok := th.dataBlockIndex.Search(key)
			if !ok {
				// not in this sstable, search next one
				continue
			}

			// in this sstable, search according to data block
			entry, ok := lm.fetchAndSearch(key, level, th.levelIdx, dataBlockHandle)
			if ok {
				return entry, true
			}
		}
	}

	return types.Entry{}, false
}

func (lm *levelManager) scan(start, end types.Key) []types.Entry {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	if len(lm.levels) == 0 {
		return nil
	}

	var entriesList [][]types.Entry
	// scan L0 - LN
	// sort and merge result
	for level, tables := range lm.levels {
		// tables in same level
		var levelList [][]types.Entry
		for e := tables.Front(); e != nil; e = e.Next() {
			th := e.Value.(tableHandle)

			// search the data blocks where the range in
			dataBlockHandles := th.dataBlockIndex.Scan(start, end)

			for _, handle := range dataBlockHandles {
				entries := lm.fetchAndScan(start, end, level, th.levelIdx, handle)
				levelList = append(levelList, entries)
			}
		}
		slices.Reverse(levelList)
		entriesList = append(entriesList, levelList...)
	}
	slices.Reverse(entriesList)

	return kway.Merge(entriesList...)
}

func (lm *levelManager) flushToL0(kvs []types.Entry) error {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	// new and build bloom filter
	bf := filter.Build(kvs)
	// build sstable
	dataBlockIndex, tableBytes := table.Build(kvs, lm.dataBlockSize, 0)

	// lazy init
	if len(lm.levels) == 0 {
		lm.levels = append(lm.levels, list.New())
	}

	// table handle
	th := tableHandle{
		levelIdx:       lm.maxLevelIdx(0) + 1,
		filter:         *bf,
		dataBlockIndex: dataBlockIndex,
	}

	// l0 list
	lm.levels[0].PushBack(th)

	// file name format: level-idx.db
	fd, err := os.OpenFile(lm.fileName(0, th.levelIdx), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		return err
	}
	defer func() {
		if err = fd.Close(); err != nil {
			lm.logger.Errorf("failed to close file: %v", err)
		}
	}()

	// write sstable
	_, err = fd.Write(tableBytes)
	if err != nil {
		return err
	}

	return nil
}

func (lm *levelManager) checkAndCompact() {
	lm.mu.Lock()
	defer lm.mu.Unlock()

	for i, tables := range lm.levels {
		if tables.Len() > lm.l0TargetNum*utils.Pow(lm.ratio, i) {
			if i == 0 {
				lm.compactL0()
				continue
			}
			lm.compactLN(i)
		}
	}
}

func (lm *levelManager) fetch(level, idx int, handle table.BlockHandle) table.Data {
	fd, err := os.Open(lm.fileName(level, idx))
	if err != nil {
		lm.logger.Panicf("failed to open sstable: %v", err)
	}
	defer func() {
		if err = fd.Close(); err != nil {
			lm.logger.Errorf("failed to close file: %v", err)
		}
	}()

	_, err = fd.Seek(int64(handle.Offset), io.SeekStart)
	if err != nil {
		lm.logger.Panicf("failed to seek sstable: %v", err)
	}

	data := make([]byte, handle.Length)
	_, err = fd.Read(data)
	if err != nil {
		lm.logger.Panicf("failed to read sstable: %v", err)
	}

	var dataBlock table.Data
	if err = dataBlock.Decode(data); err != nil {
		lm.logger.Panicf("failed to decode data block: %v", err)
	}

	return dataBlock
}

func (lm *levelManager) fetchAndSearch(key types.Key, level, idx int, handle table.BlockHandle) (types.Entry, bool) {
	dataBlock := lm.fetch(level, idx, handle)
	return dataBlock.Search(key)
}

func (lm *levelManager) fetchAndScan(start, end types.Key, level, idx int, handle table.BlockHandle) []types.Entry {
	dataBlock := lm.fetch(level, idx, handle)
	return dataBlock.Scan(start, end)
}

// L0 -> L1
func (lm *levelManager) compactL0() {
	defer utils.Elapsed(time.Now(), lm.logger, "compact level 0")

	// lazy init
	if len(lm.levels)-1 < 1 {
		lm.levels = append(lm.levels, list.New())
	}

	// len(overlaps) >= 1
	// overlap sstables in level 0
	l0Tables := lm.overlapL0()

	// boundary from first table to last table in l0Tables
	start, end := boundary(l0Tables...)

	// overlap sstables in L1
	l1Tables := lm.overlapLN(1, start, end)

	// old -> new (append L1 first)
	var dataBlockList [][]types.Entry
	// L1 data block entries
	for _, tab := range l1Tables {
		th := tab.Value.(tableHandle)
		dataBlock := lm.fetch(1, th.levelIdx, th.dataBlockIndex.DataBlock)
		dataBlockList = append(dataBlockList, dataBlock.Entries)
	}
	// L0 data block entries
	for _, tab := range l0Tables {
		th := tab.Value.(tableHandle)
		dataBlock := lm.fetch(0, th.levelIdx, th.dataBlockIndex.DataBlock)
		dataBlockList = append(dataBlockList, dataBlock.Entries)
	}

	// merge sstables
	mergedEntries := kway.Merge(dataBlockList...)

	// build new bloom filter
	bf := filter.Build(mergedEntries)
	// build new sstable
	dataBlockIndex, tableBytes := table.Build(mergedEntries, lm.dataBlockSize, 1)

	// table handle
	th := tableHandle{
		levelIdx:       lm.maxLevelIdx(1) + 1,
		filter:         *bf,
		dataBlockIndex: dataBlockIndex,
	}

	// update index
	// add new index to L1
	lm.levels[1].PushBack(th)

	// remove old sstable index from L0
	for _, e := range l0Tables {
		lm.levels[0].Remove(e)
	}
	// remove old sstable index from L1
	for _, e := range l1Tables {
		lm.levels[1].Remove(e)
	}

	// delete old sstables from L0
	for _, e := range l0Tables {
		if err := os.Remove(lm.fileName(0, e.Value.(tableHandle).levelIdx)); err != nil {
			lm.logger.Panicf("failed to delete old sstable: %v", err)
		}
	}
	// delete old sstables from L1
	for _, e := range l1Tables {
		if err := os.Remove(lm.fileName(1, e.Value.(tableHandle).levelIdx)); err != nil {
			lm.logger.Panicf("failed to delete old sstable: %v", err)
		}
	}

	// write new sstable
	fd, err := os.OpenFile(lm.fileName(1, th.levelIdx), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		lm.logger.Panicf("failed to open sstable: %v", err)
	}
	defer func() {
		if err = fd.Close(); err != nil {
			lm.logger.Errorf("failed to close file: %v", err)
		}
	}()

	_, err = fd.Write(tableBytes)
	if err != nil {
		lm.logger.Panicf("failed to write sstable: %v", err)
	}
}

// LN -> LN+1
func (lm *levelManager) compactLN(n int) {
	defer utils.Elapsed(time.Now(), lm.logger, fmt.Sprintf("compact level %v", n))

	// lazy init
	if len(lm.levels)-1 < n+1 {
		lm.levels = append(lm.levels, list.New())
	}

	lnTable := lm.levels[n].Front()
	start, end := boundary(lnTable)

	// overlap sstables in LN+1
	ln1Tables := lm.overlapLN(n+1, start, end)

	// old -> new (append LN+1 first)
	var dataBlockList [][]types.Entry
	// LN+1 data block entries
	for _, tab := range ln1Tables {
		th := tab.Value.(tableHandle)
		dataBlockLN1 := lm.fetch(n+1, th.levelIdx, th.dataBlockIndex.DataBlock)
		dataBlockList = append(dataBlockList, dataBlockLN1.Entries)
	}
	// LN data block entries
	dataBlockLN := lm.fetch(n, lnTable.Value.(tableHandle).levelIdx, lnTable.Value.(tableHandle).dataBlockIndex.DataBlock)
	dataBlockList = append(dataBlockList, dataBlockLN.Entries)

	// merge sstables
	mergedEntries := kway.Merge(dataBlockList...)

	// build new bloom filter
	bf := filter.Build(mergedEntries)
	// build new sstable
	dataBlockIndex, tableBytes := table.Build(mergedEntries, lm.dataBlockSize, n+1)

	// table handle
	th := tableHandle{
		levelIdx:       lm.maxLevelIdx(n+1) + 1,
		filter:         *bf,
		dataBlockIndex: dataBlockIndex,
	}

	// update index
	// add new index to LN+1
	lm.levels[n+1].PushBack(th)

	// remove old sstable index from LN
	lm.levels[n].Remove(lnTable)
	// remove old sstable index from LN+1
	for _, e := range ln1Tables {
		lm.levels[n+1].Remove(e)
	}

	// delete old sstables from LN
	if err := os.Remove(lm.fileName(n, lnTable.Value.(tableHandle).levelIdx)); err != nil {
		lm.logger.Panicf("failed to delete old sstable: %v", err)
	}
	// delete old sstables from LN+1
	for _, e := range ln1Tables {
		if err := os.Remove(lm.fileName(n+1, e.Value.(tableHandle).levelIdx)); err != nil {
			lm.logger.Panicf("failed to delete old sstable: %v", err)
		}
	}

	// write new sstable
	fd, err := os.OpenFile(lm.fileName(n+1, th.levelIdx), os.O_CREATE|os.O_RDWR|os.O_TRUNC, 0600)
	if err != nil {
		lm.logger.Panicf("failed to open sstable: %v", err)
	}
	defer func() {
		if err = fd.Close(); err != nil {
			lm.logger.Errorf("failed to close file: %v", err)
		}
	}()

	_, err = fd.Write(tableBytes)
	if err != nil {
		lm.logger.Panicf("failed to write sstable: %v", err)
	}
}

func (lm *levelManager) overlapL0() []*list.Element {
	frontIndex := lm.levels[0].Front().Value.(tableHandle).dataBlockIndex

	startKey := frontIndex.Entries[0].StartKey
	endKey := frontIndex.Entries[len(frontIndex.Entries)-1].EndKey

	return lm.overlapLN(0, startKey, endKey)
}

func (lm *levelManager) overlapLN(level int, start, end string) []*list.Element {
	// check if LN+1 is not initialized
	if lm.levels[level].Len() == 0 {
		return nil
	}

	ln := lm.levels[level]

	var overlaps []*list.Element
	for e := ln.Front(); e != nil; e = e.Next() {
		index := e.Value.(tableHandle).dataBlockIndex
		if index.Entries[0].StartKey <= end && index.Entries[len(index.Entries)-1].EndKey >= start {
			overlaps = append(overlaps, e)
		}
	}

	return overlaps
}

func (lm *levelManager) fileName(level, idx int) string {
	return path.Join(lm.dir, fmt.Sprintf("%d-%d.db", level, idx))
}

// if no elements in this level, return -1
// else return max level idx
func (lm *levelManager) maxLevelIdx(level int) int {
	res := -1
	for e := lm.levels[level].Front(); e != nil; e = e.Next() {
		levelIdx := e.Value.(tableHandle).levelIdx
		if levelIdx > res {
			res = levelIdx
		}
	}
	return res
}

func parseFileName(name string) (int, int, error) {
	var level, idx int
	_, err := fmt.Sscanf(name, "%d-%d.db", &level, &idx)
	if err != nil {
		return 0, 0, err
	}
	return level, idx, nil
}

func boundary(list ...*list.Element) (string, string) {
	entries := list[0].Value.(tableHandle).dataBlockIndex.Entries
	start := entries[0].StartKey
	end := entries[len(entries)-1].EndKey

	for _, e := range list {
		index := e.Value.(tableHandle).dataBlockIndex
		currStart := index.Entries[0].StartKey
		currEnd := index.Entries[len(index.Entries)-1].EndKey

		if currStart < start {
			start = currStart
		}
		if currEnd > end {
			end = currEnd
		}
	}
	return start, end
}
