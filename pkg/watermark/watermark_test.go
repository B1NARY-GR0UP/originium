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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestWaterMarkBasic(t *testing.T) {
	w := New()
	defer w.Stop()

	assert.Equal(t, uint64(0), w.DoneUntil())
}

func TestWaterMarkBeginDone(t *testing.T) {
	w := New()
	defer w.Stop()

	w.Begin(100)
	assert.Equal(t, uint64(0), w.DoneUntil())
	w.Done(100)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(100), w.DoneUntil())
}

func TestWaterMarkMultipleMarks(t *testing.T) {
	w := New()
	defer w.Stop()

	w.Begin(100)
	w.Begin(100)
	w.Begin(200)
	w.Begin(300)

	w.Done(100)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), w.DoneUntil())

	w.Done(200)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), w.DoneUntil())

	w.Done(300)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), w.DoneUntil())

	w.Done(100)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(300), w.DoneUntil())
}

func TestWaterMarkWaitForMark(t *testing.T) {
	w := New()
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()

	w.Begin(100)

	var wg sync.WaitGroup
	wg.Add(1)

	go func() {
		defer wg.Done()
		err := w.WaitForMark(ctx, 100)
		assert.NoError(t, err)
	}()

	time.Sleep(50 * time.Millisecond)
	w.Done(100)

	wg.Wait()
}

func TestWaterMarkWaitTimeout(t *testing.T) {
	w := New()
	defer w.Stop()

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	w.Begin(100)

	err := w.WaitForMark(ctx, 100)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "context deadline exceeded")
}

func TestWaterMarkOutOfOrderCompletion(t *testing.T) {
	w := New()
	defer w.Stop()

	w.Begin(300)
	w.Begin(200)
	w.Begin(100)

	w.Done(200)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), w.DoneUntil())

	w.Done(100)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(200), w.DoneUntil())

	w.Done(300)
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(300), w.DoneUntil())
}

func TestWaterMarkConcurrentOperations(t *testing.T) {
	w := New()
	defer w.Stop()

	const numMarks = 100
	var wg sync.WaitGroup

	for i := 1; i <= numMarks; i++ {
		ts := uint64(i)
		wg.Add(1)
		go func(timestamp uint64) {
			defer wg.Done()
			w.Begin(timestamp)
			time.Sleep(time.Millisecond)
			w.Done(timestamp)
		}(ts)
	}

	wg.Wait()
	time.Sleep(50 * time.Millisecond)

	assert.Equal(t, uint64(numMarks), w.DoneUntil())
}

func TestWaterMarkWaitForPastMark(t *testing.T) {
	w := New()
	defer w.Stop()

	w.Begin(100)
	w.Done(100)
	time.Sleep(10 * time.Millisecond)

	err := w.WaitForMark(context.Background(), 50)
	assert.NoError(t, err)

	err = w.WaitForMark(context.Background(), 100)
	assert.NoError(t, err)
}
