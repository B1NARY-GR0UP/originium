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
	"testing"

	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/stretchr/testify/assert"
)

func TestMerge(t *testing.T) {
	list1 := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "c", Value: []byte("3")},
	}
	list2 := []types.Entry{
		{Key: "b", Value: []byte("2")},
		{Key: "d", Value: []byte("4")},
	}

	expected := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
		{Key: "d", Value: []byte("4")},
	}

	result := Merge(list1, list2)
	assert.Equal(t, expected, result)
}

func TestMergeDuplicate(t *testing.T) {
	list1 := []types.Entry{
		{Key: "a", Value: []byte("10")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("10")},
		{Key: "d", Value: []byte("4")},
	}
	list2 := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "c", Value: []byte("3")},
	}

	expected := []types.Entry{
		{Key: "a", Value: []byte("1")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("3")},
		{Key: "d", Value: []byte("4")},
	}

	result := Merge(list1, list2)
	assert.Equal(t, expected, result)
}

func TestMergeTombstone(t *testing.T) {
	list1 := []types.Entry{
		{Key: "a", Value: []byte("10")},
		{Key: "b", Value: []byte("2")},
		{Key: "c", Value: []byte("10")},
		{Key: "d", Value: []byte("4")},
	}
	list2 := []types.Entry{
		{Key: "a", Value: []byte("1"), Tombstone: true},
		{Key: "c", Value: []byte("3"), Tombstone: true},
	}

	expected := []types.Entry{
		{Key: "b", Value: []byte("2")},
		{Key: "d", Value: []byte("4")},
	}

	result := Merge(list1, list2)
	assert.Equal(t, expected, result)
}
