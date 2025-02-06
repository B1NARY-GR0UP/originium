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
	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/B1NARY-GR0UP/originium/pkg/utils"
)

// Data Block
type Data struct {
	Entries []types.Entry
}

func (d *Data) Search(key types.Key) (types.Entry, bool) {
	low, high := 0, len(d.Entries)-1
	for low <= high {
		mid := low + ((high - low) >> 1)
		if d.Entries[mid].Key < key {
			low = mid + 1
		} else if d.Entries[mid].Key > key {
			high = mid - 1
		} else {
			return d.Entries[mid], true
		}
	}
	return types.Entry{}, false
}

func (d *Data) Scan(start, end types.Key) []types.Entry {
	var res []types.Entry
	var found bool
	low, high := 0, len(d.Entries)-1

	// find the first key >= start
	var mid int
	for low <= high {
		mid = low + ((high - low) >> 1)
		if d.Entries[mid].Key >= start {
			if mid == 0 || d.Entries[mid-1].Key < start {
				// used as return
				found = true
				break
			}
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	for i := mid; i < len(d.Entries) && d.Entries[i].Key < end && found; i++ {
		res = append(res, d.Entries[i])
	}

	return res
}

func (d *Data) Encode() ([]byte, error) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	var prevKey string
	for _, entry := range d.Entries {
		lcp := utils.LCP(entry.Key, prevKey)
		suffix := entry.Key[lcp:]

		// lcp
		if err := binary.Write(buf, binary.LittleEndian, uint16(lcp)); err != nil {
			return nil, err
		}

		// suffix length
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(suffix))); err != nil {
			return nil, err
		}
		// suffix
		if err := binary.Write(buf, binary.LittleEndian, []byte(suffix)); err != nil {
			return nil, err
		}

		// value length
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(entry.Value))); err != nil {
			return nil, err
		}
		// value
		if err := binary.Write(buf, binary.LittleEndian, entry.Value); err != nil {
			return nil, err
		}

		// tombstone
		tombstone := uint8(0)
		if entry.Tombstone {
			tombstone = 1
		}
		if err := binary.Write(buf, binary.LittleEndian, tombstone); err != nil {
			return nil, err
		}

		prevKey = entry.Key
	}

	compressed := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(compressed)

	if err := utils.Compress(buf, compressed); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func (d *Data) Decode(data []byte) error {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	if err := utils.Decompress(bytes.NewReader(data), buf); err != nil {
		return err
	}

	reader := bytes.NewReader(buf.Bytes())
	var prevKey string
	for reader.Len() > 0 {
		// lcp
		var lcp uint16
		if err := binary.Read(reader, binary.LittleEndian, &lcp); err != nil {
			return err
		}

		// suffix length
		var suffixLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &suffixLen); err != nil {
			return err
		}
		// suffix
		suffix := make([]byte, suffixLen)
		if err := binary.Read(reader, binary.LittleEndian, &suffix); err != nil {
			return err
		}

		// value length
		var valueLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &valueLen); err != nil {
			return err
		}
		// value
		value := make([]byte, valueLen)
		if err := binary.Read(reader, binary.LittleEndian, &value); err != nil {
			return err
		}

		var tombstone uint8
		if err := binary.Read(reader, binary.LittleEndian, &tombstone); err != nil {
			return err
		}

		key := prevKey[:lcp] + string(suffix)
		d.Entries = append(d.Entries, types.Entry{
			Key:       key,
			Value:     value,
			Tombstone: tombstone == 1,
		})

		prevKey = key
	}
	return nil
}
