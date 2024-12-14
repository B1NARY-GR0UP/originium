package filter

import (
	"hash"
	"math"

	"github.com/spaolacci/murmur3"
)

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
