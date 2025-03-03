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
	"bytes"
	"encoding/binary"
	"errors"

	"github.com/B1NARY-GR0UP/originium/pkg/bufferpool"
	"github.com/B1NARY-GR0UP/originium/utils"
)

const _magic uint64 = 0x5bc2aa5766250562

var ErrInvalidMagic = errors.New("error invalid magic")

// Footer
// 16 + 16 + 8 = 40 bytes
type Footer struct {
	MetaBlock  BlockHandle
	IndexBlock BlockHandle
	Magic      uint64
}

func (f *Footer) Encode() ([]byte, error) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	w := utils.NewErrorWriter(buf)
	w.Write(binary.LittleEndian, f.MetaBlock.Offset)
	w.Write(binary.LittleEndian, f.MetaBlock.Length)
	w.Write(binary.LittleEndian, f.IndexBlock.Offset)
	w.Write(binary.LittleEndian, f.IndexBlock.Length)
	w.Write(binary.LittleEndian, f.Magic)

	if w.Error() != nil {
		return nil, w.Error()
	}
	return buf.Bytes(), nil
}

func (f *Footer) Decode(footer []byte) error {
	reader := bytes.NewReader(footer)
	r := utils.NewErrorReader(reader)

	var metaOffset, metaLength, indexOffset, indexLength, magic uint64
	r.Read(binary.LittleEndian, &metaOffset)
	r.Read(binary.LittleEndian, &metaLength)
	r.Read(binary.LittleEndian, &indexOffset)
	r.Read(binary.LittleEndian, &indexLength)
	r.Read(binary.LittleEndian, &magic)

	if r.Error() != nil {
		return r.Error()
	}
	if magic != _magic {
		return ErrInvalidMagic
	}

	f.Magic = magic
	f.MetaBlock.Offset = metaOffset
	f.MetaBlock.Length = metaLength
	f.IndexBlock.Offset = indexOffset
	f.IndexBlock.Length = indexLength
	return nil
}
