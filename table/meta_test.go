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
	"time"

	"github.com/stretchr/testify/assert"
)

func TestMetaEncodeDecode(t *testing.T) {
	meta := &Meta{
		CreatedUnix: time.Now().Unix(),
		Level:       3,
	}

	encoded, err := meta.Encode()
	assert.NoError(t, err)
	assert.NotNil(t, encoded)

	decodedMeta := &Meta{}
	err = decodedMeta.Decode(encoded)
	assert.NoError(t, err)

	assert.Equal(t, meta.CreatedUnix, decodedMeta.CreatedUnix)
	assert.Equal(t, meta.Level, decodedMeta.Level)
}
