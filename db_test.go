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
	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            4,
		LevelRatio:             10,
		DataBlockByteThreshold: 4096,
		MemtableByteThreshold:  1024,
	}

	db, err := Open(dir, config)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	assert.Equal(t, StateOpened, db.State())
	db.Close()
}

func TestClose(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            4,
		LevelRatio:             10,
		DataBlockByteThreshold: 4096,
		MemtableByteThreshold:  1024,
	}

	db, err := Open(dir, config)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	db.Close()
	assert.Equal(t, StateClosed, db.State())
}

func TestSetAndGet(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            4,
		LevelRatio:             10,
		DataBlockByteThreshold: 4096,
		MemtableByteThreshold:  1024,
	}

	db, err := Open(dir, config)
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	key := "key1"
	value := []byte("value1")

	db.Set(key, value)
	result, found := db.Get(key)
	assert.True(t, found)
	assert.Equal(t, value, result)
}

func TestScan(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            1,
		LevelRatio:             2,
		DataBlockByteThreshold: 10,
		MemtableByteThreshold:  50,
	}

	db, err := Open(dir, config)
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// Insert test data
	entries := []types.Entry{
		{Key: "key1", Value: []byte("value1"), Tombstone: false},
		{Key: "key2", Value: []byte("value2"), Tombstone: false},
		{Key: "key3", Value: []byte("value3"), Tombstone: false},
		{Key: "key4", Value: []byte("value4"), Tombstone: false},
		{Key: "key5", Value: []byte("value5"), Tombstone: false},
	}

	for _, entry := range entries {
		db.Set(entry.Key, entry.Value)
	}

	tests := []struct {
		start    string
		end      string
		expected []types.Entry
	}{
		{"key1", "key3", []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: false},
		}},
		{"key2", "key5", []types.Entry{
			{Key: "key2", Value: []byte("value2"), Tombstone: false},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
		}},
		{"key3", "key6", []types.Entry{
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
			{Key: "key5", Value: []byte("value5"), Tombstone: false},
		}},
		{"key0", "key6", []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: false},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
			{Key: "key5", Value: []byte("value5"), Tombstone: false},
		}},
		{"key6", "key7", nil},
	}

	for _, tt := range tests {
		result := db.Scan(tt.start, tt.end)
		assert.Equal(t, tt.expected, result)
	}
}

func TestDelete(t *testing.T) {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            4,
		LevelRatio:             10,
		DataBlockByteThreshold: 4096,
		MemtableByteThreshold:  1024,
	}

	db, err := Open(dir, config)
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	key := "key1"
	value := []byte("value1")

	db.Set(key, value)
	db.Delete(key)
	result, found := db.Get(key)
	assert.False(t, found)
	assert.Nil(t, result)
}
