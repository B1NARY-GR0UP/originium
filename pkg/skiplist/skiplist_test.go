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
