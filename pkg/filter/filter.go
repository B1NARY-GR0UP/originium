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

package filter

import (
	"hash"
	"math"

	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/spaolacci/murmur3"
)

const _defaultP = 0.01

type Filter struct {
	bitset  []bool
	hashFns []hash.Hash32
	m       int
}

// New creates a new BloomFilter with the given size and number of hash functions.
// n: expected nums of elements
// p: expected rate of false errors
func New(n int, p float64) *Filter {
	// size of bitset
	// m = -(n * ln(p)) / (ln(2)^2)
	m := int(math.Ceil(-float64(n) * math.Log(p) / math.Pow(math.Log(2), 2)))
	// nums of hash functions used
	// k = (m/n) * ln(2)
	k := int(math.Round((float64(m) / float64(n)) * math.Log(2)))

	hashFns := make([]hash.Hash32, k)
	for i := range k {
		hashFns[i] = murmur3.New32WithSeed(uint32(i))
	}

	return &Filter{
		bitset:  make([]bool, m),
		hashFns: hashFns,
		m:       m,
	}
}

func Build(kvs []types.Entry) *Filter {
	filter := New(len(kvs), _defaultP)
	for _, e := range kvs {
		filter.Add(e.Key)
	}
	return filter
}

// Add adds an element to the BloomFilter.
func (f *Filter) Add(key string) {
	for _, fn := range f.hashFns {
		_, _ = fn.Write([]byte(key))
		index := int(fn.Sum32()) % f.m
		f.bitset[index] = true
		fn.Reset()
	}
}

// Contains checks if an element is in the BloomFilter.
func (f *Filter) Contains(key string) bool {
	for _, fn := range f.hashFns {
		_, _ = fn.Write([]byte(key))
		index := int(fn.Sum32()) % f.m
		fn.Reset()
		if !f.bitset[index] {
			return false
		}
	}
	return true
}
