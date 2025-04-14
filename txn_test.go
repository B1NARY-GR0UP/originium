// Copyright 2025 BINARY Members
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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// Initialize a test database
func setupTestDB(t *testing.T) *DB {
	dir := t.TempDir()
	config := Config{
		SkipListMaxLevel:       4,
		SkipListP:              0.5,
		L0TargetNum:            4,
		LevelRatio:             10,
		DataBlockByteThreshold: 4096,
		MemtableByteThreshold:  1024,
		ImmutableBuffer:        10,
	}

	db, err := Open(dir, config)
	assert.NoError(t, err)
	assert.NotNil(t, db)
	return db
}

// Test basic transaction read and write functionality
func TestTxnBasicOperations(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Test write transaction
	err := db.Update(func(txn *Txn) error {
		err := txn.Set("key1", []byte("value1"))
		assert.NoError(t, err)

		val, found := txn.Get("key1")
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), val)

		return nil
	})
	assert.NoError(t, err)

	// Test read transaction
	err = db.View(func(txn *Txn) error {
		val, found := txn.Get("key1")
		assert.True(t, found)
		assert.Equal(t, []byte("value1"), val)

		// Read transaction cannot write
		err := txn.Set("key2", []byte("value2"))
		assert.Equal(t, ErrReadOnlyTxn, err)

		return nil
	})
	assert.NoError(t, err)
}

// Test transaction isolation
func TestTxnIsolation(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// First write initial data
	err := db.Update(func(txn *Txn) error {
		return txn.Set("counter", []byte("5"))
	})
	assert.NoError(t, err)

	// Transaction 1 starts, reads but doesn't commit immediately
	txn1 := db.Begin(true)
	val1, found := txn1.Get("counter")
	assert.True(t, found)
	assert.Equal(t, []byte("5"), val1)

	// Transaction 2 starts, updates value and commits
	err = db.Update(func(txn *Txn) error {
		_, found := txn.Get("counter")
		assert.True(t, found)
		return txn.Set("counter", []byte("10"))
	})
	assert.NoError(t, err)

	// Transaction 1 updates and tries to commit, should detect conflict
	err = txn1.Set("counter", []byte("8"))
	assert.NoError(t, err)
	err = txn1.Commit()
	assert.Equal(t, ErrConflictTxn, err)

	// Verify the final value is the one set by transaction 2
	err = db.View(func(txn *Txn) error {
		val, found := txn.Get("counter")
		assert.True(t, found)
		assert.Equal(t, []byte("10"), val)
		return nil
	})
	assert.NoError(t, err)
}

// Test various transaction conflict scenarios
func TestTxnConflictScenarios(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Scenario 1: Read-write conflict
	err := db.Update(func(txn *Txn) error {
		return txn.Set("key1", []byte("initial"))
	})
	assert.NoError(t, err)

	txn1 := db.Begin(true)
	_, found := txn1.Get("key1") // Record read mark
	assert.True(t, found)

	// Another transaction modifies the same key
	err = db.Update(func(txn *Txn) error {
		return txn.Set("key1", []byte("modified"))
	})
	assert.NoError(t, err)

	// txn1 tries to commit, should fail
	err = txn1.Set("key2", []byte("value2")) // Update a different key
	assert.NoError(t, err)
	err = txn1.Commit()
	assert.Equal(t, ErrConflictTxn, err)

	// Scenario 2: Write-write conflict but no read conflict
	err = db.Update(func(txn *Txn) error {
		err := txn.Set("keyA", []byte("valueA"))
		assert.NoError(t, err)
		return nil
	})
	assert.NoError(t, err)

	txn2 := db.Begin(true)
	err = txn2.Set("keyA", []byte("newValueA"))
	assert.NoError(t, err)

	// Another transaction modifies a different key
	err = db.Update(func(txn *Txn) error {
		return txn.Set("keyB", []byte("valueB"))
	})
	assert.NoError(t, err)

	// txn2 should commit successfully, as there's no read conflict
	err = txn2.Commit()
	assert.NoError(t, err)

	// Verify value
	err = db.View(func(txn *Txn) error {
		val, found := txn.Get("keyA")
		assert.True(t, found)
		assert.Equal(t, []byte("newValueA"), val)
		return nil
	})
	assert.NoError(t, err)
}

// Test behavior after transaction discard
func TestTxnDiscard(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	txn := db.Begin(true)
	err := txn.Set("key", []byte("value"))
	assert.NoError(t, err)

	// Discard transaction
	txn.Discard()

	// Verify operations were discarded
	err = db.View(func(txn *Txn) error {
		_, found := txn.Get("key")
		assert.False(t, found)
		return nil
	})
	assert.NoError(t, err)

	// Try to operate on a discarded transaction
	err = txn.Set("key2", []byte("value2"))
	assert.Equal(t, ErrDiscardedTxn, err)

	_, found := txn.Get("key")
	assert.False(t, found)
}

// Test concurrent transactions
func TestConcurrentTxns(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	// Initialize counter
	err := db.Update(func(txn *Txn) error {
		return txn.Set("counter", []byte("0"))
	})
	assert.NoError(t, err)

	var wg sync.WaitGroup
	concurrentTxns := 10
	successCount := 0
	var mu sync.Mutex

	for i := 0; i < concurrentTxns; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			for attempt := 0; attempt < 3; attempt++ {
				err := db.Update(func(txn *Txn) error {
					// Read current value
					val, found := txn.Get("counter")
					if !found {
						t.Error("Counter not found")
						return nil
					}

					// Update value
					currentVal := string(val)
					newVal := []byte(currentVal + "1") // Simple string concatenation as update
					err := txn.Set("counter", newVal)
					if err != nil {
						return err
					}

					return nil
				})

				if err == nil {
					mu.Lock()
					successCount++
					mu.Unlock()
					break
				}

				// If conflict occurs, wait a bit and retry
				if errors.Is(err, ErrConflictTxn) {
					time.Sleep(10 * time.Millisecond)
					continue
				}

				// Other errors
				t.Errorf("Unexpected error: %v", err)
				break
			}
		}()
	}

	wg.Wait()

	// Check final result
	err = db.View(func(txn *Txn) error {
		val, found := txn.Get("counter")
		assert.True(t, found)

		// Verify string length equals number of successful transactions
		assert.Equal(t, successCount+1, len(val)) // +1 is for initial "0"
		return nil
	})
	assert.NoError(t, err)
}

// Test empty key and error handling
func TestTxnErrorHandling(t *testing.T) {
	db := setupTestDB(t)
	defer db.Close()

	err := db.Update(func(txn *Txn) error {
		// Test empty key
		err := txn.Set("", []byte("value"))
		assert.Equal(t, ErrEmptyKey, err)

		// Test normal key
		err = txn.Set("valid-key", []byte("value"))
		assert.NoError(t, err)

		// Test deletion
		err = txn.Delete("valid-key")
		assert.NoError(t, err)
		_, found := txn.Get("valid-key")
		assert.False(t, found)

		return nil
	})
	assert.NoError(t, err)
}
