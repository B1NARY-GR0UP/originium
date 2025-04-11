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
	"github.com/B1NARY-GR0UP/originium/types"
)

type Element struct {
	types.Entry
	// list index
	// NOTE: the larger the index, the newer the value
	LI int
}

// Heap min heap
type Heap []Element

func (h *Heap) Len() int {
	return len(*h)
}

func (h *Heap) Less(i, j int) bool {
	cmp := types.CompareKeys((*h)[i].Key, (*h)[j].Key)
	if cmp != 0 {
		return cmp < 0
	}
	return (*h)[i].LI < (*h)[j].LI
}

func (h *Heap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *Heap) Push(x any) {
	*h = append(*h, x.(Element))
}

// Pop the minimum element in heap
// 1. move the minimum element to the end of slice
// 2. pop it (what this method does)
// 3. heapify
func (h *Heap) Pop() any {
	curr := *h
	n := len(curr)
	e := curr[n-1]
	*h = curr[0 : n-1]
	return e
}
