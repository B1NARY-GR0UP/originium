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
	"testing"

	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/stretchr/testify/assert"
)

func TestSearch(t *testing.T) {
	dir := t.TempDir()
	lm := &levelManager{
		dir:           dir,
		l0TargetNum:   4,
		ratio:         10,
		dataBlockSize: 4096,
		logger:        logger.GetLogger(),
	}

	kvs := []types.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
		{Key: "key4", Value: []byte("value4")},
		{Key: "key5", Value: []byte("value5"), Tombstone: true},
		{Key: "key6", Value: []byte("value6")},
	}

	err := lm.flushToL0(kvs)
	assert.NoError(t, err)

	entry, found := lm.searchLowerBound("key1")
	assert.True(t, found)
	assert.Equal(t, "key1", entry.Key)
	assert.Equal(t, []byte("value1"), entry.Value)

	entry, found = lm.searchLowerBound("key5")
	assert.True(t, found)
	assert.Equal(t, "key5", entry.Key)
	assert.Equal(t, []byte("value5"), entry.Value)
	assert.True(t, entry.Tombstone)

	entry, found = lm.searchLowerBound("key7")
	assert.Equal(t, types.Entry{}, entry)
	assert.False(t, found)
}

func TestManagerScan(t *testing.T) {
	dir := t.TempDir()
	lm := &levelManager{
		dir:           dir,
		l0TargetNum:   4,
		ratio:         10,
		dataBlockSize: 4096,
		logger:        logger.GetLogger(),
	}

	kvs := []types.Entry{
		{Key: "key1", Value: []byte("value1")},
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
		{Key: "key4", Value: []byte("value4")},
		{Key: "key5", Value: []byte("value5")},
		{Key: "key6", Value: []byte("value6")},
	}

	err := lm.flushToL0(kvs)
	assert.NoError(t, err)

	// Perform scan
	entries := lm.scan("key2", "key5")
	expectedEntries := []types.Entry{
		{Key: "key2", Value: []byte("value2")},
		{Key: "key3", Value: []byte("value3")},
		{Key: "key4", Value: []byte("value4")},
	}

	assert.Equal(t, expectedEntries, entries)

	// Test scan with no results
	entries = lm.scan("key7", "key8")
	assert.Empty(t, entries)
}

//func TestCompact(t *testing.T) {
//	lm := &levelManager{
//		dir:           t.TempDir(),
//		l0TargetNum:   1,
//		ratio:         2,
//		dataBlockSize: 500,
//		logger:        logger.GetLogger(),
//	}
//
//	// First flush: key100-key200
//	kvs1 := make([]types.Entry, 0)
//	for i := 100; i <= 200; i++ {
//		kvs1 = append(kvs1, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err := lm.flushToL0(kvs1)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	// Second flush: key150-key300
//	kvs2 := make([]types.Entry, 0)
//	for i := 150; i <= 300; i++ {
//		kvs2 = append(kvs2, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err = lm.flushToL0(kvs2)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	// Third flush: key250-key400
//	kvs3 := make([]types.Entry, 0)
//	for i := 250; i <= 400; i++ {
//		kvs3 = append(kvs3, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err = lm.flushToL0(kvs3)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	kvs4 := make([]types.Entry, 0)
//	for i := 500; i <= 600; i++ {
//		kvs4 = append(kvs4, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err = lm.flushToL0(kvs4)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	kvs5 := make([]types.Entry, 0)
//	for i := 700; i <= 800; i++ {
//		kvs5 = append(kvs5, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err = lm.flushToL0(kvs5)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	kvs6 := make([]types.Entry, 0)
//	for i := 900; i <= 1000; i++ {
//		kvs6 = append(kvs6, types.Entry{
//			Key:   fmt.Sprintf("key%d", i),
//			Value: []byte(fmt.Sprintf("value%d", i)),
//		})
//	}
//	err = lm.flushToL0(kvs6)
//	assert.NoError(t, err)
//
//	// Perform compaction
//	lm.checkAndCompact()
//
//	// Verify the compaction result
//	for i := 100; i <= 400; i++ {
//		entry, found := lm.searchLowerBound(fmt.Sprintf("key%d", i))
//		assert.True(t, found)
//		assert.Equal(t, fmt.Sprintf("key%d", i), entry.Key)
//		assert.Equal(t, []byte(fmt.Sprintf("value%d", i)), entry.Value)
//	}
//}
