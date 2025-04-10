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

package types

import (
	"sort"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestKeyWithTs(t *testing.T) {
	tests := []struct {
		key    string
		ts     uint64
		expect string
	}{
		{"k1", 1, "k1@1"},
		{"hello", 12345, "hello@12345"},
		{"", 0, "@0"},
	}

	for _, test := range tests {
		result := KeyWithTs(test.key, test.ts)
		assert.Equal(t, test.expect, result, "KeyWithTs(%s, %d) should be %s", test.key, test.ts, test.expect)
	}
}

func TestParseTs(t *testing.T) {
	tests := []struct {
		key    string
		expect uint64
	}{
		{"k1@1", 1},
		{"hello@12345", 12345},
		{"@0", 0},
		{"invalid", 0},
		{"", 0},
	}

	for _, test := range tests {
		result := ParseTs(test.key)
		assert.Equal(t, test.expect, result, "ParseTs(%s) should be %d", test.key, test.expect)
	}
}

func TestParseKey(t *testing.T) {
	tests := []struct {
		key    string
		expect string
	}{
		{"k1@1", "k1"},
		{"hello@12345", "hello"},
		{"@0", ""},
	}

	for _, test := range tests {
		result := ParseKey(test.key)
		assert.Equal(t, test.expect, result, "ParseKey(%s) should be %s", test.key, test.expect)
	}
}

func TestCompareKeys(t *testing.T) {
	tests := []struct {
		key1   string
		key2   string
		expect int
	}{
		{"k1@1", "k1@1", 0},
		{"k1@12", "k1@1", -1},
		{"k1@1", "k1@12", 1},
		{"k2@1", "k1@1", 1},
		{"k1@1", "k2@1", -1},
		{"k1@5", "k2@10", -1},
		{"k2@10", "k1@5", 1},
	}

	for _, test := range tests {
		result := CompareKeys(test.key1, test.key2)
		assert.Equal(t, test.expect, result, "CompareKeys(%s, %s) should be %d", test.key1, test.key2, test.expect)
	}
}

func TestSortingWithCompareKeys(t *testing.T) {
	keys := []string{"k1@1", "k1@12", "k1@5", "k2@1", "k2@10", "k3@7"}
	expectedOrder := []string{"k1@12", "k1@5", "k1@1", "k2@10", "k2@1", "k3@7"}

	sort.Slice(keys, func(i, j int) bool {
		return CompareKeys(keys[i], keys[j]) < 0
	})

	assert.Equal(t, expectedOrder, keys, "Keys should be sorted correctly")
}

func TestKVStruct(t *testing.T) {
	kv := KV{
		K: "testkey",
		V: []byte("testvalue"),
	}

	assert.Equal(t, "testkey", kv.K)
	assert.Equal(t, []byte("testvalue"), kv.V)
}
