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
	"math/rand"
	"time"
	"unsafe"

	"github.com/B1NARY-GR0UP/originium/types"
)

const _head = "HEAD"

// SkipList
//
// Level 3:       3 ----------- 9 ----------- 21 --------- 26
// Level 2:       3 ----- 6 ---- 9 ------ 19 -- 21 ---- 25 -- 26
// Level 1:       3 -- 6 -- 7 -- 9 -- 12 -- 19 -- 21 -- 25 -- 26
// next of Element 3 [ ->6, ->6, ->9 ]
// next of Element 6 [ ->7, ->9 ]
// next of head [ ->3, ->3, ->3 ]
//
// Element with same key only has one instance within the skip list
type SkipList struct {
	maxLevel int
	p        float64
	level    int
	rand     *rand.Rand
	size     int
	head     *Element
}

type Element struct {
	types.Entry
	next []*Element
}

func New(maxLevel int, p float64) *SkipList {
	return &SkipList{
		maxLevel: maxLevel,
		p:        p,
		level:    1,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		size:     0,
		head: &Element{
			Entry: types.Entry{
				Key:       _head,
				Value:     nil,
				Tombstone: false,
				Version:   0,
			},
			next: make([]*Element, maxLevel),
		},
	}
}

func (s *SkipList) Reset() *SkipList {
	return New(s.maxLevel, s.p)
}

func (s *SkipList) Size() int {
	return s.size
}

func (s *SkipList) Set(entry types.Entry) {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && types.CompareKeys(curr.next[i].Key, entry.Key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// update entry
	if curr.next[0] != nil && types.CompareKeys(curr.next[0].Key, entry.Key) == 0 {
		s.size += len(entry.Value) - len(curr.next[0].Value)

		// update value and tombstone
		curr.next[0].Value = entry.Value
		curr.next[0].Tombstone = entry.Tombstone
		return
	}

	// add entry
	level := s.randomLevel()

	if level > s.level {
		for i := s.level; i < level; i++ {
			update[i] = s.head
		}
		s.level = level
	}

	e := &Element{
		Entry: types.Entry{
			Key:       entry.Key,
			Value:     entry.Value,
			Tombstone: entry.Tombstone,
			Version:   entry.Version,
		},
		next: make([]*Element, level),
	}

	for i := range level {
		e.next[i] = update[i].next[i]
		update[i].next[i] = e
	}
	s.size += len(entry.Key) + len(entry.Value) +
		int(unsafe.Sizeof(entry.Tombstone)) +
		int(unsafe.Sizeof(entry.Version)) +
		len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
}

func (s *SkipList) Get(key types.Key) (types.Entry, bool) {
	curr := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && types.CompareKeys(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	if curr != nil && types.CompareKeys(curr.Key, key) == 0 {
		return types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		}, true
	}

	return types.Entry{}, false
}

func (s *SkipList) LowerBound(key types.Key) (types.Entry, bool) {
	curr := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && types.CompareKeys(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	if curr != nil {
		return types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		}, true
	}

	return types.Entry{}, false
}

// Scan [start, end)
func (s *SkipList) Scan(start, end types.Key) []types.Entry {
	var res []types.Entry
	curr := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && types.CompareKeys(curr.next[i].Key, start) < 0 {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	for curr != nil && types.CompareKeys(curr.Key, end) < 0 {
		res = append(res, types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		})
		curr = curr.next[0]
	}

	return res
}

func (s *SkipList) All() []types.Entry {
	var all []types.Entry

	for curr := s.head.next[0]; curr != nil; curr = curr.next[0] {
		all = append(all, types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
			Version:   curr.Version,
		})
	}

	return all
}

// Delete won't be used, use tombstone in set instead
func (s *SkipList) Delete(key types.Key) bool {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && types.CompareKeys(curr.next[i].Key, key) < 0 {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	curr = curr.next[0]

	if curr != nil && types.CompareKeys(curr.Key, key) == 0 {
		for i := range s.level {
			if update[i].next[i] != curr {
				break
			}
			update[i].next[i] = curr.next[i]
		}
		s.size -= len(curr.Key) + len(curr.Value) +
			int(unsafe.Sizeof(curr.Tombstone)) +
			int(unsafe.Sizeof(curr.Version)) +
			len(curr.next)*int(unsafe.Sizeof((*Element)(nil)))

		for s.level > 1 && s.head.next[s.level-1] == nil {
			s.level--
		}
		return true
	}
	return false
}

// n < MaxLevel, return level == n has probability P^n
func (s *SkipList) randomLevel() int {
	level := 1
	for s.rand.Float64() < s.p && level < s.maxLevel {
		level++
	}
	return level
}
