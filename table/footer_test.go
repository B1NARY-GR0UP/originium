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

func TestFooterEncodeDecode(t *testing.T) {
	footer := &Footer{
		MetaBlock:  BlockHandle{Offset: 123, Length: 456},
		IndexBlock: BlockHandle{Offset: 789, Length: 1011},
		Magic:      _magic,
	}

	encoded, err := footer.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	decodedFooter := &Footer{}
	err = decodedFooter.Decode(encoded)
	assert.NoError(t, err)

	assert.Equal(t, footer.MetaBlock, decodedFooter.MetaBlock)
	assert.Equal(t, footer.IndexBlock, decodedFooter.IndexBlock)
	assert.Equal(t, footer.Magic, decodedFooter.Magic)
}

func TestFooterInvalidMagic(t *testing.T) {
	footer := &Footer{
		MetaBlock:  BlockHandle{Offset: 123, Length: 456},
		IndexBlock: BlockHandle{Offset: 789, Length: 1011},
		Magic:      0x1234567890abcdef, // Invalid magic number
	}

	encoded, err := footer.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	decodedFooter := &Footer{}
	err = decodedFooter.Decode(encoded)
	assert.Error(t, err)
	assert.Equal(t, ErrInvalidMagic, err)
}
