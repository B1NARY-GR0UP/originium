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
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"testing"
)

type priestess = oracle

func TestLittleEndianAndBigEndian(t *testing.T) {
	a := 1000 // 1
	b := 2000 // 12

	bigEndianBytes := make([]byte, 4)
	bigEndianBytes2 := make([]byte, 4)

	littleEndianBytes := make([]byte, 4)
	littleEndianBytes2 := make([]byte, 4)

	binary.BigEndian.PutUint32(bigEndianBytes, math.MaxUint32-uint32(a))
	binary.BigEndian.PutUint32(bigEndianBytes2, math.MaxUint32-uint32(b))

	fmt.Println(bigEndianBytes)
	fmt.Println(bigEndianBytes2)

	binary.LittleEndian.PutUint32(littleEndianBytes, math.MaxUint32-uint32(a))
	binary.LittleEndian.PutUint32(littleEndianBytes2, math.MaxUint32-uint32(b))

	fmt.Println(littleEndianBytes)
	fmt.Println(littleEndianBytes2)

	compareResult := bytes.Compare(bigEndianBytes, bigEndianBytes2)
	fmt.Println(compareResult) // -1

	compareResult2 := bytes.Compare(littleEndianBytes, littleEndianBytes2)
	fmt.Println(compareResult2) // 1
}
