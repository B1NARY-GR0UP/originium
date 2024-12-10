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

func TestMemtableFreezeAndReset(t *testing.T) {
	dir := t.TempDir()
	mt := newMemtable(dir, 4, 0.5)

	entry := types.Entry{Key: "key1", Value: []byte("value1"), Tombstone: true}
	mt.set(entry)

	frozenMt := mt.freeze()

	assert.True(t, frozenMt.readOnly)

	newMt := mt.reset()

	gotValue, ok := frozenMt.get(entry.Key)
	assert.True(t, ok)
	assert.Equal(t, entry, gotValue)

	assert.False(t, newMt.readOnly)

	gotValue, ok = newMt.get(entry.Key)
	assert.False(t, ok)
	assert.Equal(t, types.Entry{}, gotValue)

	entry2 := types.Entry{Key: "key2", Value: []byte("value2"), Tombstone: false}
	newMt.set(entry2)

	gotValue, ok = newMt.get(entry2.Key)
	assert.True(t, ok)
	assert.Equal(t, entry2, gotValue)

	gotValue, ok = frozenMt.get(entry2.Key)
	assert.False(t, ok)
	assert.Equal(t, types.Entry{}, gotValue)

	err := frozenMt.wal.Delete()
	assert.NoError(t, err)
	err = newMt.wal.Delete()
	assert.NoError(t, err)
}
