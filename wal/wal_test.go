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

package wal

import (
	"os"
	"testing"

	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/stretchr/testify/assert"
)

func TestCreateAndDelete(t *testing.T) {
	dir := t.TempDir()
	wal, err := Create(dir)
	assert.NoError(t, err)
	assert.NotNil(t, wal)

	err = wal.Close()
	assert.NoError(t, err)

	err = wal.Delete()
	assert.NoError(t, err)

	_, err = os.Stat(wal.path)
	assert.True(t, os.IsNotExist(err))
}

func TestOpen(t *testing.T) {
	dir := t.TempDir()
	wal, err := Create(dir)
	assert.NoError(t, err)
	assert.NotNil(t, wal)
	err = wal.fd.Close()
	assert.NotNil(t, wal)

	wal2, err := Open(wal.path)
	assert.NoError(t, err)

	err = wal2.Delete()
	assert.NoError(t, err)

	_, err = os.Stat(wal.path)
	assert.True(t, os.IsNotExist(err))
}

func TestWriteAndRead(t *testing.T) {
	dir := t.TempDir()
	wal, err := Create(dir)
	assert.NoError(t, err)
	assert.NotNil(t, wal)

	entries := []types.Entry{
		{
			Key:       "hello",
			Value:     []byte("world"),
			Tombstone: false,
		},
		{
			Key:       "foo",
			Value:     []byte("bar"),
			Tombstone: true,
		},
		{
			Key:       "foiver",
			Value:     []byte("originium"),
			Tombstone: false,
		},
	}

	err = wal.Write(entries...)
	assert.NoError(t, err)

	readEntries, err := wal.Read()
	assert.NoError(t, err)
	assert.Equal(t, entries, readEntries)

	err = wal.Delete()
	assert.NoError(t, err)
}
