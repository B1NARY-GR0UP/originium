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

package originium

import (
	"testing"

	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestMemtableSetAndGet(t *testing.T) {
	dir := t.TempDir()
	mt := newMemtable(dir, 4, 0.5)

	entry := types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: false}

	mt.set(entry)
	gotValue, ok := mt.get(entry.Key)
	assert.True(t, ok)
	assert.Equal(t, entry, gotValue)

	got, ok := mt.get("hello")
	assert.Equal(t, types.Entry{}, got)
	assert.False(t, ok)

	err := mt.wal.Delete()
	assert.NoError(t, err)
}
