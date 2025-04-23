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
	"context"
	"sync"

	"github.com/B1NARY-GR0UP/originium/pkg/watermark"
)

type oracle struct {
	sync.Mutex
	// used to ensure that transactions go to the write
	// channel in the same order as their commit timestamps.
	writeLock sync.Mutex

	nextTs        uint64
	lastCleanUpTs uint64

	// used to track active read txn
	//
	// readMark.DoneUntil:
	// All read transactions with timestamps less than or equal to this value have completed.
	// In other words, this value represents the timestamp of the oldest active read transaction in the system minus one.
	// For any timestamp less than or equal to this value, there are no ongoing read transactions.
	//
	// When a read transaction begins, it waits for commitMark to reach its read timestamp,
	// ensuring all data that should be visible to it has been written.
	// This guarantees that the read transaction can see all modifications made by transactions committed before its read timestamp.
	readMark *watermark.WaterMark
	// used to track committing txn
	//
	// commitMark.DoneUntil
	// All transactions with timestamps less than or equal to this value have completed their commit processing.
	// This implies that all transactions with timestamps less than or equal to this value have finished writing data,
	// and new read transactions can safely read this data.
	commitMark *watermark.WaterMark

	committedTxns []committedTxn
}

type committedTxn struct {
	ts       uint64
	writesFp map[uint64]struct{}
}

func newOracle() *oracle {
	return &oracle{
		readMark:   watermark.New(),
		commitMark: watermark.New(),
	}
}

func (o *oracle) Stop() {
	o.readMark.Stop()
	o.commitMark.Stop()
}

func (o *oracle) readTs() uint64 {
	o.Lock()
	readTs := o.nextTs - 1
	o.readMark.Begin(readTs)
	o.Unlock()

	// ensure current txn can read the latest value of txn at ts <= readTs
	if err := o.commitMark.WaitForMark(context.Background(), readTs); err != nil {
		panic(err)
	}
	return readTs
}

func (o *oracle) newCommitTs(txn *Txn) (uint64, bool) {
	o.Lock()
	defer o.Unlock()

	if o.hasConflict(txn) {
		return 0, true
	}

	o.doneRead(txn)
	o.cleanUpCommittedTxns()

	ts := o.nextTs
	o.nextTs++
	o.commitMark.Begin(ts)

	o.committedTxns = append(o.committedTxns, committedTxn{
		ts:       ts,
		writesFp: txn.writesFp,
	})

	return ts, false
}

func (o *oracle) doneRead(txn *Txn) {
	if txn.doneRead {
		return
	}
	o.readMark.Done(txn.readTs)
	txn.doneRead = true
}

func (o *oracle) doneCommit(ts uint64) {
	o.commitMark.Done(ts)
}

// cleanUpCommittedTxns
// NOTE: call with lock
func (o *oracle) cleanUpCommittedTxns() {
	maxReadTs := o.readMark.DoneUntil()

	if maxReadTs < o.lastCleanUpTs {
		panic("clean up ts must be monotone increasing")
	}
	if maxReadTs == o.lastCleanUpTs {
		return
	}

	o.lastCleanUpTs = maxReadTs

	// A new slice with a length of 0 but the same capacity as the original slice is created. The key points are:
	// - It reuses the underlying array of the original slice.
	// - Setting the length to 0 means all elements are cleared, but the original capacity is preserved.
	// - No new memory space is allocated.
	temp := o.committedTxns[:0]
	for _, committed := range o.committedTxns {
		if committed.ts <= maxReadTs {
			continue
		}
		temp = append(temp, committed)
	}
	o.committedTxns = temp
}

// discardAtOrBelow
// NOTE: call with lock
//
// For each key, retain all versions with timestamps > discardAtOrBelow()
// For versions with timestamps ≤ discardAtOrBelow(), only keep the one with the largest timestamp, Delete all other versions.
// Because no read transaction will ever need these older versions again.
//
// Since all active read transaction timestamps are > DoneUntil, why can't we delete all versions with timestamps ≤ DoneUntil?
// The reason is: The continuity of the historical view needs to be preserved.
//
// Consider the following scenario:
// The key "user:1" was created at ts=10.
// It was updated at ts=30.
// There were no intermediate updates.
//
// If discardAtOrBelow() returns 20:
// If we delete all versions with ts ≤ 20, the version at ts=10 will be deleted.
// Then, a transaction with readTs=25 would not be able to see the key "user:1," because it can only see versions with ts ≤ 25.
//
// But in reality, "user:1" already existed at ts=10 and should be visible to the transaction with ts=25.
// Therefore, we must retain the largest version among those with timestamps ≤ D to ensure the correctness of the historical view.
func (o *oracle) discardAtOrBelow() uint64 {
	return o.readMark.DoneUntil()
}

// hasConflict should be call with lock
// ensure that reads by curr txn has not been modified by other concurrent txn
//
// 1. Txn1 start:
// - get readTs 100
// - read key=counter, value=5, record counter fingerprint to readsFp
//
// 2. Txn2 start:
// - get readTs 101
// - read key=counter, value=5
// - modify key=counter, value=6
// - commit txn, commitTs 102
// - oracle.committedTxn
// - oracle.committedTxns add record: {ts: 102, conflictKeys: {counter fingerprint}}
//
// 3. Txn1 try commit:
// - call oracle.hasConflict
// - committedTxns has a record: {ts: 102, conflictKeys: {counter fingerprint}}
// - ts=102 > txn1.readTs 100
// - conflictKeys include the fp of key=counter
// - return err conflict
func (o *oracle) hasConflict(txn *Txn) bool {
	if len(txn.readsFp) == 0 {
		return false
	}
	for _, ct := range o.committedTxns {
		if ct.ts <= txn.readTs {
			continue
		}

		for _, fp := range txn.readsFp {
			// a conflict occurred when curr txn read a key that be modified by a committed txn
			if _, ok := ct.writesFp[fp]; ok {
				return true
			}
		}
	}
	return false
}
