// Copyright 2025 BINARY Members
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
	"errors"

	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/B1NARY-GR0UP/originium/utils"
)

// TODO: OCC + MVCC

var (
	ErrReadOnlyTxn  = errors.New("transaction is read-only")
	ErrDiscardedTxn = errors.New("transaction has been discarded")
	ErrConflictTxn  = errors.New("transaction has a conflict")
	ErrEmptyKey     = errors.New("key is empty")
)

type Txn struct {
	readOnly  bool
	discarded bool
	doneRead  bool

	db *DB

	readTs   uint64
	commitTs uint64

	readsFp  []uint64
	writesFp map[uint64]struct{}

	pendingWrites map[types.Key]types.Entry
}

type TxnFunc func(*Txn) error

func (t *Txn) Commit() error {
	// pre-check
	if t.discarded {
		return ErrDiscardedTxn
	}

	if len(t.pendingWrites) == 0 {
		t.Discard()
		return nil
	}

	orc := t.db.oracle

	orc.writeLock.Lock()
	defer orc.writeLock.Unlock()

	commitTs, hasConflict := orc.newCommitTs(t)
	if hasConflict {
		return ErrConflictTxn
	}

	// TODO: support txn crush recovery (txnEnt and txnFin)

	for _, v := range t.pendingWrites {
		t.db.rawset(types.Entry{
			Key:       v.Key,
			Value:     v.Value,
			Tombstone: v.Tombstone,
			Version:   int64(commitTs),
		})
	}

	orc.commitMark.Done(commitTs)

	return nil
}

func (t *Txn) Discard() {
	if t.discarded {
		return
	}
	t.db.oracle.doneRead(t)
	t.discarded = true
}

func (t *Txn) Get(key string) error {
	// TODO
	return nil
}

func (t *Txn) Set(key string, value []byte) error {
	return t.SetEntry(types.Entry{
		Key:   key,
		Value: value,
	})
}

func (t *Txn) Delete(key string) error {
	return t.SetEntry(types.Entry{
		Key:       key,
		Value:     []byte{},
		Tombstone: true,
	})
}

func (t *Txn) SetEntry(e types.Entry) error {
	return t.modify(e)
}

func (t *Txn) modify(e types.Entry) error {
	switch {
	case t.readOnly:
		return ErrReadOnlyTxn
	case t.discarded:
		return ErrDiscardedTxn
	case e.Key == "":
		return ErrEmptyKey
	}

	// record key fingerprint
	t.writesFp[utils.Hash(e.Key)] = struct{}{}
	// memory storage writer buffer
	t.pendingWrites[e.Key] = e
	return nil
}
