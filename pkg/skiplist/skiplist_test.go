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

package skiplist

import (
	"testing"

	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/stretchr/testify/assert"
)

func TestNew(t *testing.T) {
	sl := New(4, 0.5)
	assert.NotNil(t, sl)
	assert.Equal(t, 4, sl.maxLevel)
	assert.Equal(t, 0.5, sl.p)
	assert.Equal(t, 1, sl.level)
	assert.Equal(t, 0, sl.size)
	assert.NotNil(t, sl.head)
	assert.Equal(t, _head, sl.head.Key)
}

func TestSetAndGet(t *testing.T) {
	sl := New(4, 0.5)
	entry := types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false}
	sl.Set(entry)

	result, found := sl.Get("key1")
	assert.True(t, found)
	assert.Equal(t, entry, result)

	// Test updating the entry
	entry.Value = []byte("value2")
	sl.Set(entry)
	result, found = sl.Get("key1")
	assert.True(t, found)
	assert.Equal(t, entry, result)
}

func TestRange(t *testing.T) {
	sl := New(4, 0.5)
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1"), Tombstone: false},
		{Key: "key2", Value: []byte("value2"), Tombstone: false},
		{Key: "key3", Value: []byte("value3"), Tombstone: false},
		{Key: "key4", Value: []byte("value4"), Tombstone: false},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	tests := []struct {
		start, end string
		expected   []types.Entry
	}{
		{"key1", "key3", entries[:2]},
		{"key2", "key4", entries[1:3]},
		{"key1", "key5", entries},
		{"key3", "key3", nil},
		{"key0", "key1", nil},
	}

	for _, tt := range tests {
		result := sl.Scan(tt.start, tt.end)
		assert.Equal(t, tt.expected, result)
	}
}

func TestGetNonExistent(t *testing.T) {
	sl := New(4, 0.5)
	result, found := sl.Get("nonexistent")
	assert.False(t, found)
	assert.Equal(t, types.Entry{}, result)
}

func TestDelete(t *testing.T) {
	sl := New(4, 0.5)
	entry1 := types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false}
	entry2 := types.Entry{Key: "key2", Value: []byte("value2"), Tombstone: false}
	sl.Set(entry1)
	sl.Set(entry2)

	// Delete an existing entry
	deleted := sl.Delete("key1")
	assert.True(t, deleted)

	// Verify the entry is deleted
	_, found := sl.Get("key1")
	assert.False(t, found)

	// Verify the other entry still exists
	result, found := sl.Get("key2")
	assert.True(t, found)
	assert.Equal(t, entry2, result)

	// Try to delete a non-existent entry
	deleted = sl.Delete("nonexistent")
	assert.False(t, deleted)
}

func TestAll(t *testing.T) {
	sl := New(4, 0.5)
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1"), Tombstone: false},
		{Key: "key2", Value: nil, Tombstone: true},
		{Key: "key3", Value: []byte("value3"), Tombstone: false},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	allEntries := sl.All()
	assert.Equal(t, len(entries), len(allEntries))
	for i, entry := range entries {
		assert.Equal(t, entry, allEntries[i])
	}
}

func TestReset(t *testing.T) {
	sl := New(4, 0.5)
	entry := types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false}
	sl.Set(entry)

	sl = sl.Reset()
	assert.Equal(t, 0, sl.size)
	assert.Equal(t, 1, sl.level)
	assert.Nil(t, sl.head.next[0])
}

func TestMultiVersionEntries(t *testing.T) {
	sl := New(4, 0.5)

	key := "testKey"
	entries := []types.Entry{
		{Key: types.KeyWithTs(key, 1), Value: []byte("value1"), Tombstone: false, Version: 1},
		{Key: types.KeyWithTs(key, 2), Value: []byte("value2"), Tombstone: false, Version: 2},
		{Key: types.KeyWithTs(key, 3), Value: []byte("value3"), Tombstone: false, Version: 3},
	}

	sl.Set(entries[2])
	sl.Set(entries[0])
	sl.Set(entries[1])

	for _, entry := range entries {
		result, found := sl.Get(entry.Key)
		assert.True(t, found)
		assert.Equal(t, entry, result)
	}

	allEntries := sl.All()
	assert.Equal(t, 3, len(allEntries))

	assert.Equal(t, entries[2].Key, allEntries[0].Key)
	assert.Equal(t, entries[1].Key, allEntries[1].Key)
	assert.Equal(t, entries[0].Key, allEntries[2].Key)
}

func TestGreaterOrEqual(t *testing.T) {
	sl := New(4, 0.5)

	entries := []types.Entry{
		{Key: types.KeyWithTs("a", 1), Value: []byte("a1"), Version: 1},
		{Key: types.KeyWithTs("a", 2), Value: []byte("a2"), Version: 2},
		{Key: types.KeyWithTs("b", 1), Value: []byte("b1"), Version: 1},
		{Key: types.KeyWithTs("c", 3), Value: []byte("c3"), Version: 3},
		{Key: types.KeyWithTs("c", 1), Value: []byte("c1"), Version: 1},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	tests := []struct {
		searchKey string
		expectKey string
		found     bool
	}{
		{types.KeyWithTs("a", 2), types.KeyWithTs("a", 2), true},
		{types.KeyWithTs("a", 3), types.KeyWithTs("a", 2), true},
		{types.KeyWithTs("a", 0), types.KeyWithTs("b", 1), true},
		{"aa", types.KeyWithTs("b", 1), true},
		{types.KeyWithTs("d", 1), "", false},
	}

	for _, tt := range tests {
		result, found := sl.LowerBound(tt.searchKey)
		assert.Equal(t, tt.found, found, "search key %s", tt.searchKey)
		if found {
			assert.Equal(t, tt.expectKey, result.Key, "search key %s should return %s, but got %s",
				tt.searchKey, tt.expectKey, result.Key)
		}
	}

	// Verify returning the latest version when searching by base key name
	entry, found := sl.LowerBound("a")
	assert.True(t, found)
	assert.Equal(t, types.KeyWithTs("a", 2), entry.Key) // Should return the latest version of a: a@2

	entry, found = sl.LowerBound("c")
	assert.True(t, found)
	assert.Equal(t, types.KeyWithTs("c", 3), entry.Key) // Should return the latest version of c: c@3
}

func TestScanWithVersions(t *testing.T) {
	sl := New(4, 0.5)

	// Create multiple versions for multiple keys
	entries := []types.Entry{
		{Key: types.KeyWithTs("a", 3), Value: []byte("a3"), Version: 3},
		{Key: types.KeyWithTs("a", 1), Value: []byte("a1"), Version: 1},
		{Key: types.KeyWithTs("b", 2), Value: []byte("b2"), Version: 2},
		{Key: types.KeyWithTs("b", 1), Value: []byte("b1"), Version: 1},
		{Key: types.KeyWithTs("c", 1), Value: []byte("c1"), Version: 1},
	}

	for _, entry := range entries {
		sl.Set(entry)
	}

	// Test range query
	results := sl.Scan("a", "c")
	assert.Equal(t, 4, len(results))

	// Due to CompareKeys sorting, results should be sorted by key name ascending, and by timestamp descending for the same key
	assert.Equal(t, types.KeyWithTs("a", 3), results[0].Key)
	assert.Equal(t, types.KeyWithTs("a", 1), results[1].Key)
	assert.Equal(t, types.KeyWithTs("b", 2), results[2].Key)
	assert.Equal(t, types.KeyWithTs("b", 1), results[3].Key)
}
