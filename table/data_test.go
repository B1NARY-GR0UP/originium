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

package table

import (
	"bytes"
	"testing"

	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/stretchr/testify/assert"
)

func TestDataEncodeDecode(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
		},
	}

	// Test Encode
	encoded, err := data.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	// Test Decode
	var decodedData Data
	err = decodedData.Decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, data, decodedData)
}

func TestSearch(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
		},
	}

	tests := []struct {
		key      string
		expected types.Entry
		found    bool
	}{
		{"key1", types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false}, true},
		{"key2", types.Entry{Key: "key2", Value: []byte("value2"), Tombstone: true}, true},
		{"key3", types.Entry{Key: "key3", Value: []byte("value3"), Tombstone: false}, true},
		{"key4", types.Entry{}, false},
	}

	for _, tt := range tests {
		entry, found := data.Search(tt.key)
		assert.Equal(t, tt.found, found)
		assert.Equal(t, tt.expected, entry)
	}
}

func TestDataEncodeDecodeMultiple(t *testing.T) {
	entries := []types.Entry{
		{Key: "asy1", Value: []byte("value1"), Tombstone: false},
		{Key: "kssdy2", Value: []byte("value2"), Tombstone: true},
		{Key: "keyiiwadc", Value: []byte("value3"), Tombstone: false},
		{Key: "y4", Value: []byte{}, Tombstone: true},
		{Key: "sdasey1", Value: []byte("value1"), Tombstone: false},
		{Key: "ooiney2", Value: []byte("value2"), Tombstone: true},
		{Key: "iinnisaksady3", Value: []byte("value3"), Tombstone: false},
		{Key: "kiiwadc", Value: []byte("value3"), Tombstone: false},
		{Key: "4", Value: []byte{}, Tombstone: true},
		{Key: "asy1", Value: []byte("value1"), Tombstone: false},
		{Key: "ooey2", Value: []byte("value2"), Tombstone: true},
		{Key: "iiissady3", Value: []byte("value3"), Tombstone: false},
	}

	// Create multiple Data objects
	dataList := []Data{
		{Entries: entries[:1]},
		{Entries: entries[1:6]},
		{Entries: entries[6:]},
	}

	var buf bytes.Buffer
	// Encode each Data object separately
	for _, data := range dataList {
		encoded, err := data.Encode()
		assert.NoError(t, err)
		assert.NotNil(t, encoded)
		buf.Write(encoded)
	}

	var data Data
	err := data.Decode(buf.Bytes())
	assert.NoError(t, err)
	assert.Equal(t, entries, data.Entries)
}

func TestScan(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
			{Key: "key5", Value: []byte("value5"), Tombstone: true},
		},
	}

	tests := []struct {
		start    string
		end      string
		expected []types.Entry
	}{
		{"key1", "key3", []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
		}},
		{"key2", "key5", []types.Entry{
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
		}},
		{"key3", "key6", []types.Entry{
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
			{Key: "key5", Value: []byte("value5"), Tombstone: true},
		}},
		{"key0", "key6", []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false},
			{Key: "key2", Value: []byte("value2"), Tombstone: true},
			{Key: "key3", Value: []byte("value3"), Tombstone: false},
			{Key: "key4", Value: []byte("value4"), Tombstone: false},
			{Key: "key5", Value: []byte("value5"), Tombstone: true},
		}},
		{"key6", "key7", nil},
	}

	for _, tt := range tests {
		result := data.Scan(tt.start, tt.end)
		assert.Equal(t, tt.expected, result)
	}
}

func TestDataEncodeDecodeWithVersion(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false, Version: 1},
			{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 2},
		},
	}

	// Test Encode
	encoded, err := data.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	// Test Decode
	var decodedData Data
	err = decodedData.Decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, data.Entries[0].Version, decodedData.Entries[0].Version)
	assert.Equal(t, data.Entries[1].Version, decodedData.Entries[1].Version)
	assert.Equal(t, data, decodedData)
}

func TestSearchWithVersion(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false, Version: 10},
			{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 20},
			{Key: "key3", Value: []byte("value3"), Tombstone: false, Version: 30},
		},
	}

	tests := []struct {
		key      string
		expected types.Entry
		found    bool
	}{
		{"key1", types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false, Version: 10}, true},
		{"key2", types.Entry{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 20}, true},
		{"key3", types.Entry{Key: "key3", Value: []byte("value3"), Tombstone: false, Version: 30}, true},
		{"key4", types.Entry{}, false},
	}

	for _, tt := range tests {
		entry, found := data.Search(tt.key)
		assert.Equal(t, tt.found, found)
		assert.Equal(t, tt.expected, entry)
	}
}

func TestDataEncodeDecodeMultipleWithVersion(t *testing.T) {
	entries := []types.Entry{
		{Key: "asy1", Value: []byte("value1"), Tombstone: false, Version: 1},
		{Key: "kssdy2", Value: []byte("value2"), Tombstone: true, Version: 2},
		{Key: "keyiiwadc", Value: []byte("value3"), Tombstone: false, Version: 3},
		{Key: "y4", Value: []byte{}, Tombstone: true, Version: 4},
		{Key: "sdasey1", Value: []byte("value1"), Tombstone: false, Version: 5},
		{Key: "ooiney2", Value: []byte("value2"), Tombstone: true, Version: 6},
		{Key: "iinnisaksady3", Value: []byte("value3"), Tombstone: false, Version: 7},
		{Key: "kiiwadc", Value: []byte("value3"), Tombstone: false, Version: 8},
		{Key: "4", Value: []byte{}, Tombstone: true, Version: 9},
		{Key: "asy1", Value: []byte("value1"), Tombstone: false, Version: 10},
		{Key: "ooey2", Value: []byte("value2"), Tombstone: true, Version: 11},
		{Key: "iiissady3", Value: []byte("value3"), Tombstone: false, Version: 12},
	}

	// Create multiple Data objects
	dataList := []Data{
		{Entries: entries[:1]},
		{Entries: entries[1:6]},
		{Entries: entries[6:]},
	}

	var buf bytes.Buffer
	// Encode each Data object separately
	for _, data := range dataList {
		encoded, err := data.Encode()
		assert.NoError(t, err)
		assert.NotNil(t, encoded)
		buf.Write(encoded)
	}

	var data Data
	err := data.Decode(buf.Bytes())
	assert.NoError(t, err)

	// Check length
	assert.Equal(t, len(entries), len(data.Entries))

	// Check all entries including version
	for i, entry := range entries {
		assert.Equal(t, entry.Key, data.Entries[i].Key)
		assert.Equal(t, entry.Value, data.Entries[i].Value)
		assert.Equal(t, entry.Tombstone, data.Entries[i].Tombstone)
		assert.Equal(t, entry.Version, data.Entries[i].Version)
	}
}

func TestScanWithVersion(t *testing.T) {
	data := Data{
		Entries: []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false, Version: 100},
			{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 200},
			{Key: "key3", Value: []byte("value3"), Tombstone: false, Version: 300},
			{Key: "key4", Value: []byte("value4"), Tombstone: false, Version: 400},
			{Key: "key5", Value: []byte("value5"), Tombstone: true, Version: 500},
		},
	}

	tests := []struct {
		start    string
		end      string
		expected []types.Entry
	}{
		{"key1", "key3", []types.Entry{
			{Key: "key1", Value: []byte("value1"), Tombstone: false, Version: 100},
			{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 200},
		}},
		{"key2", "key5", []types.Entry{
			{Key: "key2", Value: []byte("value2"), Tombstone: true, Version: 200},
			{Key: "key3", Value: []byte("value3"), Tombstone: false, Version: 300},
			{Key: "key4", Value: []byte("value4"), Tombstone: false, Version: 400},
		}},
	}

	for _, tt := range tests {
		result := data.Scan(tt.start, tt.end)
		assert.Equal(t, tt.expected, result)
	}
}

func TestLowerBound(t *testing.T) {
	// Basic test with multiple versions of a single key
	data1 := Data{
		Entries: []types.Entry{
			{Key: types.KeyWithTs("key1", 30), Value: []byte("value1-v30"), Tombstone: false, Version: 30},
			{Key: types.KeyWithTs("key1", 20), Value: []byte("value1-v20"), Tombstone: false, Version: 20},
			{Key: types.KeyWithTs("key1", 10), Value: []byte("value1-v10"), Tombstone: false, Version: 10},
			{Key: types.KeyWithTs("key2", 20), Value: []byte("value2-v20"), Tombstone: false, Version: 20},
			{Key: types.KeyWithTs("key3", 10), Value: []byte("value3-v10"), Tombstone: false, Version: 10},
		},
	}

	// Multiple versions of multiple keys
	data2 := Data{
		Entries: []types.Entry{
			{Key: types.KeyWithTs("apple", 30), Value: []byte("apple-v30"), Tombstone: false, Version: 30},
			{Key: types.KeyWithTs("apple", 20), Value: []byte("apple-v20"), Tombstone: false, Version: 20},
			{Key: types.KeyWithTs("banana", 40), Value: []byte("banana-v40"), Tombstone: false, Version: 40},
			{Key: types.KeyWithTs("banana", 30), Value: []byte("banana-v30"), Tombstone: false, Version: 30},
			{Key: types.KeyWithTs("cherry", 10), Value: []byte("cherry-v10"), Tombstone: false, Version: 10},
		},
	}

	// Empty data
	data3 := Data{Entries: []types.Entry{}}

	// Multiple versions of a single key with tombstone
	data4 := Data{
		Entries: []types.Entry{
			{Key: types.KeyWithTs("key1", 40), Value: []byte{}, Tombstone: true, Version: 40},
			{Key: types.KeyWithTs("key1", 30), Value: []byte("value1-v30"), Tombstone: false, Version: 30},
			{Key: types.KeyWithTs("key1", 20), Value: []byte("value1-v20"), Tombstone: false, Version: 20},
		},
	}

	tests := []struct {
		name     string
		data     Data
		key      string
		expected types.Entry
		found    bool
	}{
		// data1 test cases
		{
			name:     "Find existing key with highest version",
			data:     data1,
			key:      types.KeyWithTs("key1", 15),
			expected: types.Entry{Key: types.KeyWithTs("key1", 10), Value: []byte("value1-v10"), Tombstone: false, Version: 10},
			found:    true,
		},
		{
			name:     "Find existing key with specific version",
			data:     data1,
			key:      types.KeyWithTs("key1", 25),
			expected: types.Entry{Key: types.KeyWithTs("key1", 20), Value: []byte("value1-v20"), Tombstone: false, Version: 20},
			found:    true,
		},
		{
			name:     "Find existing key with lowest version",
			data:     data1,
			key:      types.KeyWithTs("key1", 5),
			expected: types.Entry{Key: types.KeyWithTs("key2", 20), Value: []byte("value2-v20"), Tombstone: false, Version: 20},
			found:    true,
		},
		{
			name:     "Find different key",
			data:     data1,
			key:      types.KeyWithTs("key2", 10),
			expected: types.Entry{Key: types.KeyWithTs("key3", 10), Value: []byte("value3-v10"), Tombstone: false, Version: 10},
			found:    true,
		},
		{
			name:     "Find key greater than all keys",
			data:     data1,
			key:      types.KeyWithTs("key4", 10),
			expected: types.Entry{},
			found:    false,
		},

		// data2 test cases
		{
			name:     "Find first key in multiple keys",
			data:     data2,
			key:      types.KeyWithTs("apple", 25),
			expected: types.Entry{Key: types.KeyWithTs("apple", 20), Value: []byte("apple-v20"), Tombstone: false, Version: 20},
			found:    true,
		},
		{
			name:     "Find middle key in multiple keys",
			data:     data2,
			key:      types.KeyWithTs("banana", 30),
			expected: types.Entry{Key: types.KeyWithTs("banana", 30), Value: []byte("banana-v30"), Tombstone: false, Version: 30},
			found:    true,
		},
		{
			name:     "Find key between two keys",
			data:     data2,
			key:      types.KeyWithTs("avocado", 10),
			expected: types.Entry{Key: types.KeyWithTs("banana", 40), Value: []byte("banana-v40"), Tombstone: false, Version: 40},
			found:    true,
		},

		// data3 test cases
		{
			name:     "Empty dataset",
			data:     data3,
			key:      types.KeyWithTs("key", 10),
			expected: types.Entry{},
			found:    false,
		},

		// data4 test cases
		{
			name:     "Find key with tombstone",
			data:     data4,
			key:      types.KeyWithTs("key1", 50),
			expected: types.Entry{Key: types.KeyWithTs("key1", 40), Value: []byte{}, Tombstone: true, Version: 40},
			found:    true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			entry, found := tt.data.LowerBound(tt.key)
			assert.Equal(t, tt.found, found)
			if tt.found {
				assert.Equal(t, tt.expected, entry)
			}
		})
	}
}
