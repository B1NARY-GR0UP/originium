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

	"github.com/B1NARY-GR0UP/originium/pkg/types"
)

const _head = "HEAD"

// SkipList
// Level 3:       3 ----------- 9 ----------- 21 --------- 26
// Level 2:       3 ----- 6 ---- 9 ------ 19 -- 21 ---- 25 -- 26
// Level 1:       3 -- 6 -- 7 -- 9 -- 12 -- 19 -- 21 -- 25 -- 26
// next of Element 3 [ ->6, ->6, ->9 ]
// next of Element 6 [ ->7, ->9 ]
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
		for curr.next[i] != nil && curr.next[i].Key < entry.Key {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	// update
	if curr.next[0] != nil && curr.next[0].Key == entry.Key {
		s.size += len(entry.Value) - len(curr.next[0].Value)

		// update value and tombstone
		curr.next[0].Value = entry.Value
		curr.next[0].Tombstone = entry.Tombstone
		return
	}

	// add
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
		},
		next: make([]*Element, level),
	}

	for i := range level {
		e.next[i] = update[i].next[i]
		update[i].next[i] = e
	}
	s.size += len(entry.Key) + len(entry.Value) + int(unsafe.Sizeof(entry.Tombstone)) + len(e.next)*int(unsafe.Sizeof((*Element)(nil)))
}

func (s *SkipList) Get(key string) (types.Entry, bool) {
	curr := s.head

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].Key < key {
			curr = curr.next[i]
		}
	}

	curr = curr.next[0]

	if curr != nil && curr.Key == key {
		return types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
		}, true
	}
	return types.Entry{}, false
}

func (s *SkipList) All() []types.Entry {
	var all []types.Entry
	curr := s.head.next[0]
	for curr != nil {
		all = append(all, types.Entry{
			Key:       curr.Key,
			Value:     curr.Value,
			Tombstone: curr.Tombstone,
		})
		curr = curr.next[0]
	}
	return all
}

// Delete won't be used, use tombstone instead
func (s *SkipList) Delete(key string) bool {
	curr := s.head
	update := make([]*Element, s.maxLevel)

	for i := s.maxLevel - 1; i >= 0; i-- {
		for curr.next[i] != nil && curr.next[i].Key < key {
			curr = curr.next[i]
		}
		update[i] = curr
	}

	curr = curr.next[0]

	if curr != nil && curr.Key == key {
		for i := range s.level {
			if update[i].next[i] != curr {
				break
			}
			update[i].next[i] = curr.next[i]
		}
		s.size -= len(curr.Key) + len(curr.Value) + int(unsafe.Sizeof(curr.Tombstone)) + len(curr.next)*int(unsafe.Sizeof((*Element)(nil)))

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
