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

package filter

import (
	"strconv"
	"testing"
)

func TestFalsePositiveRate(t *testing.T) {
	n := 1000
	p := 0.01
	bf := New(n, p)

	for i := 0; i < n; i++ {
		bf.Add(strconv.Itoa(i))
	}

	falsePositives := 0
	testSize := 10000

	for i := n; i < n+testSize; i++ {
		if bf.Contains(strconv.Itoa(i)) {
			falsePositives++
		}
	}

	actualP := float64(falsePositives) / float64(testSize)
	t.Log(actualP)
}
