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

package kway

import (
	"cmp"
	"container/heap"
	"slices"

	"github.com/B1NARY-GR0UP/originium/types"
)

func Merge(lists ...[]types.Entry) []types.Entry {
	h := &Heap{}
	heap.Init(h)

	// push first element of each list
	for i, list := range lists {
		if len(list) > 0 {
			heap.Push(h, Element{
				Entry: list[0],
				LI:    i,
			})
			lists[i] = list[1:]
		}
	}

	latest := make(map[string]types.Entry)

	for h.Len() > 0 {
		// pop minimum element
		e := heap.Pop(h).(Element)
		latest[e.Key] = e.Entry
		// push next element
		if len(lists[e.LI]) > 0 {
			heap.Push(h, Element{
				Entry: lists[e.LI][0],
				LI:    e.LI,
			})
			lists[e.LI] = lists[e.LI][1:]
		}
	}

	var merged []types.Entry

	for _, entry := range latest {
		if entry.Tombstone {
			continue
		}
		merged = append(merged, entry)
	}

	slices.SortFunc(merged, func(a, b types.Entry) int {
		return cmp.Compare(a.Key, b.Key)
	})

	return merged
}
