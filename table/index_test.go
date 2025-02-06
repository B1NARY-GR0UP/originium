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

package table

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexSearch(t *testing.T) {
	index := Index{
		Entries: []IndexEntry{
			{
				StartKey: "b",
				DataHandle: BlockHandle{
					Offset: 2,
					Length: 1,
				},
			},
			{
				StartKey: "c",
				DataHandle: BlockHandle{
					Offset: 3,
					Length: 1,
				},
			},
			{
				StartKey: "d",
				DataHandle: BlockHandle{
					Offset: 4,
					Length: 1,
				},
			},
			{
				StartKey: "f",
				EndKey:   "h",
				DataHandle: BlockHandle{
					Offset: 6,
					Length: 1,
				},
			},
		},
	}

	dataH, found := index.Search("b")
	assert.True(t, found)
	assert.Equal(t, uint64(2), dataH.Offset)

	dataH, found = index.Search("e")
	assert.True(t, found)
	assert.Equal(t, uint64(4), dataH.Offset)

	dataH, found = index.Search("a")
	assert.False(t, found)
	assert.Equal(t, uint64(0), dataH.Offset)

	dataH, found = index.Search("f")
	assert.True(t, found)
	assert.Equal(t, uint64(6), dataH.Offset)

	dataH, found = index.Search("g")
	assert.True(t, found)
	assert.Equal(t, uint64(6), dataH.Offset)

	dataH, found = index.Search("i")
	assert.False(t, found)
	assert.Equal(t, uint64(0), dataH.Offset)
}

func TestIndexEncodeDecode(t *testing.T) {
	index := Index{
		DataBlock: BlockHandle{
			Offset: 0,
			Length: 100,
		},
		Entries: []IndexEntry{
			{
				StartKey: "a",
				EndKey:   "q",
				DataHandle: BlockHandle{
					Offset: 1,
					Length: 1,
				},
			},
			{
				StartKey: "b",
				EndKey:   "w",
				DataHandle: BlockHandle{
					Offset: 2,
					Length: 1,
				},
			},
			{
				StartKey: "c",
				EndKey:   "e",
				DataHandle: BlockHandle{
					Offset: 3,
					Length: 1,
				},
			},
		},
	}

	encoded, err := index.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	var decodedIndex Index
	err = decodedIndex.Decode(encoded)
	assert.NoError(t, err)
	assert.Equal(t, index, decodedIndex)
}
