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

	"github.com/B1NARY-GR0UP/originium/pkg/bufferpool"
)

// Meta Block
type Meta struct {
	CreatedUnix int64
	Level       uint64
}

func (m *Meta) Encode() ([]byte, error) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	if err := binary.Write(buf, binary.LittleEndian, m.CreatedUnix); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, m.Level); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (m *Meta) Decode(data []byte) error {
	reader := bytes.NewReader(data)
	var createdUnix int64
	if err := binary.Read(reader, binary.LittleEndian, &createdUnix); err != nil {
		return err
	}
	var level uint64
	if err := binary.Read(reader, binary.LittleEndian, &level); err != nil {
		return err
	}
	m.CreatedUnix = createdUnix
	m.Level = level
	return nil
}
