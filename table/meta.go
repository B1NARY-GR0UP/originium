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
	"github.com/B1NARY-GR0UP/originium/utils"
)

// Meta Block
type Meta struct {
	CreatedUnix int64
	Level       uint64
}

func (m *Meta) Encode() ([]byte, error) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	w := utils.NewErrorWriter(buf)

	w.Write(binary.LittleEndian, m.CreatedUnix)
	w.Write(binary.LittleEndian, m.Level)

	if err := w.Error(); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (m *Meta) Decode(data []byte) error {
	reader := bytes.NewReader(data)

	r := utils.NewErrorReader(reader)

	var createdUnix int64
	var level uint64
	r.Read(binary.LittleEndian, &createdUnix)
	r.Read(binary.LittleEndian, &level)

	if err := r.Error(); err != nil {
		return err
	}

	m.CreatedUnix = createdUnix
	m.Level = level
	return nil
}
