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

package watermark

import (
	"container/heap"
	"context"
	"sync"
	"sync/atomic"
)

const _markCBufferSize = 100

type WaterMark struct {
	wg sync.WaitGroup

	doneUntil atomic.Uint64

	markC chan mark
	stopC chan struct{}
}

type mark struct {
	ts     uint64
	done   bool
	waiter chan struct{}
}

func New() *WaterMark {
	w := &WaterMark{
		markC: make(chan mark, _markCBufferSize),
		stopC: make(chan struct{}),
	}

	w.wg.Add(1)
	go w.process()

	return w
}

// Stop WaterMark do not used with Begin
func (w *WaterMark) Stop() {
	close(w.stopC)
	w.wg.Wait()
}

func (w *WaterMark) Begin(ts uint64) {
	w.markC <- mark{
		ts: ts,
	}
}

func (w *WaterMark) Done(ts uint64) {
	w.markC <- mark{
		ts:   ts,
		done: true,
	}
}

func (w *WaterMark) DoneUntil() uint64 {
	return w.doneUntil.Load()
}

func (w *WaterMark) WaitForMark(ctx context.Context, ts uint64) error {
	if w.DoneUntil() >= ts {
		return nil
	}

	waiter := make(chan struct{})
	w.markC <- mark{
		ts:     ts,
		waiter: waiter,
	}

	select {
	case <-waiter:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *WaterMark) process() {
	defer w.wg.Done()

	var timeStamps lowHeap
	pending := make(map[uint64]int)
	waiters := make(map[uint64][]chan struct{})

	heap.Init(&timeStamps)
	for {
		select {
		case <-w.stopC:
			close(w.markC)
			return
		case m := <-w.markC:
			if m.waiter != nil {
				// handle wait
				if w.DoneUntil() >= m.ts {
					close(m.waiter)
				} else {
					waiters[m.ts] = append(waiters[m.ts], m.waiter)
				}
			} else {
				// handle begin and done
				ts := m.ts
				prev, ok := pending[ts]
				if !ok {
					heap.Push(&timeStamps, ts)
				}

				cnt := 1
				if m.done {
					cnt = -1
				}
				pending[ts] = prev + cnt
				currDoneUntil := w.DoneUntil()
				doneUntil := currDoneUntil

				// check if done
				for timeStamps.Len() > 0 {
					minTs := timeStamps[0]
					if done := pending[minTs]; done > 0 {
						// still have work to do
						break
					}

					// ts has done
					heap.Pop(&timeStamps)
					delete(pending, minTs)
					doneUntil = minTs
				}

				if doneUntil > currDoneUntil {
					w.doneUntil.Store(doneUntil)

					for t, cs := range waiters {
						if t <= doneUntil {
							for _, ch := range cs {
								close(ch)
							}
							delete(waiters, t)
						}
					}
				}

			}
		}
	}
}

type lowHeap []uint64

func (h *lowHeap) Len() int {
	return len(*h)
}

func (h *lowHeap) Less(i, j int) bool {
	return (*h)[i] < (*h)[j]
}

func (h *lowHeap) Swap(i, j int) {
	(*h)[i], (*h)[j] = (*h)[j], (*h)[i]
}

func (h *lowHeap) Push(x any) {
	*h = append(*h, x.(uint64))
}

// Pop the minimum element in heap
// 1. move the minimum element to the end of slice
// 2. pop it (what this method does)
// 3. heapify
func (h *lowHeap) Pop() any {
	curr := *h
	n := len(curr)
	e := curr[n-1]
	*h = curr[0 : n-1]
	return e
}
