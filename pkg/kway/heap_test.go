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
	"container/heap"
	"testing"

	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestHeap(t *testing.T) {
	h := &Heap{}
	heap.Init(h)

	entries := []types.Entry{
		{Key: "c", Value: []byte("3")},
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
	}

	for _, entry := range entries {
		heap.Push(h, Element{Entry: entry, LI: 0})
	}

	expectedOrder := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
	}

	for _, expected := range expectedOrder {
		e := heap.Pop(h).(Element)
		assert.Equal(t, expected, e.Entry)
	}
}
