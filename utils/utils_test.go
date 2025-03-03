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

package utils

import (
	"encoding/json"
	"github.com/B1NARY-GR0UP/originium/types"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLCP(t *testing.T) {
	tests := []struct {
		a, b     string
		expected int
	}{
		{"", "", 0},
		{"abc", "abc", 3},
		{"abc", "abd", 2},
		{"abc", "a", 1},
		{"abc", "xyz", 0},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			result := LCP(tt.a, tt.b)
			assert.Equal(t, tt.expected, result)
		})
	}
}

func BenchmarkThriftAndJSON(b *testing.B) {
	entry := &types.Entry{
		Key:       "exampleKey",
		Value:     []byte("exampleValue"),
		Tombstone: false,
	}
	thriftData, err := TMarshal(entry)
	require.NoError(b, err, "Failed to marshal Thrift data")

	jsonData, err := json.Marshal(entry)
	require.NoError(b, err, "Failed to marshal JSON data")

	b.Run("TMarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := TMarshal(entry)
			require.NoError(b, err, "TMarshal failed")
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
	})

	b.Run("JSONMarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			data, err := json.Marshal(entry)
			require.NoError(b, err, "JSONMarshal failed")
			b.ReportMetric(float64(len(data)), "bytes/op")
		}
	})

	b.Run("TUnmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			newEntry := &types.Entry{}
			require.NoError(b, TUnmarshal(thriftData, newEntry), "TUnmarshal failed")
		}
	})

	b.Run("JSONUnmarshal", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			newEntry := &types.Entry{}
			require.NoError(b, json.Unmarshal(jsonData, newEntry), "JSONUnmarshal failed")
		}
	})
}

func TestMagic(t *testing.T) {
	var m uint64 = 0x5bc2aa5766250562
	assert.Equal(t, m, Magic("foiver/originium"))
}
