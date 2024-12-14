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

package sstable

import (
	"bytes"
	"testing"

	"github.com/B1NARY-GR0UP/originium/pkg/types"
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
