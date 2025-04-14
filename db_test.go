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
	"time"

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
	assert.Equal(t, StateInitialize, db.State())
	time.Sleep(time.Second * 1)
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
		MemtableByteThreshold:  5,
		ImmutableBuffer:        10,
	}

	db, err := Open(dir, config)
	defer db.Close()
	assert.NoError(t, err)
	assert.NotNil(t, db)

	key := "key1"
	value := []byte("value1")

	if err = db.Update(func(txn *Txn) error {
		err = txn.Set(key, value)
		if err != nil {
			return err
		}
		res, found := txn.Get(key)

		assert.True(t, found)
		assert.Equal(t, value, res)
		return nil
	}); err != nil {
		t.Fatal(err)
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

	err = db.Update(func(txn *Txn) error {
		err = txn.Set(key, value)
		assert.NoError(t, err)

		result, found := txn.Get(key)
		assert.Equal(t, value, result)
		assert.True(t, found)

		err = txn.Delete(key)
		assert.NoError(t, err)

		result, found = txn.Get(key)
		assert.False(t, found)
		assert.Nil(t, result)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}
