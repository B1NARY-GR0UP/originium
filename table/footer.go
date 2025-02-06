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

	if err := binary.Write(buf, binary.LittleEndian, f.MetaBlock.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, f.MetaBlock.Length); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, f.IndexBlock.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, f.IndexBlock.Length); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, f.Magic); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (f *Footer) Decode(footer []byte) error {
	reader := bytes.NewReader(footer)
	var metaOffset uint64
	if err := binary.Read(reader, binary.LittleEndian, &metaOffset); err != nil {
		return err
	}
	var metaLength uint64
	if err := binary.Read(reader, binary.LittleEndian, &metaLength); err != nil {
		return err
	}
	var indexOffset uint64
	if err := binary.Read(reader, binary.LittleEndian, &indexOffset); err != nil {
		return err
	}
	var indexLength uint64
	if err := binary.Read(reader, binary.LittleEndian, &indexLength); err != nil {
		return err
	}
	var magic uint64
	if err := binary.Read(reader, binary.LittleEndian, &magic); err != nil {
		return err
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
