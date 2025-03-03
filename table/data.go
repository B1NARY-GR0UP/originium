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
	"github.com/B1NARY-GR0UP/originium/types"
	"github.com/B1NARY-GR0UP/originium/utils"
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

	w := utils.NewErrorWriter(buf)
	var prevKey string
	for _, entry := range d.Entries {
		lcp := utils.LCP(entry.Key, prevKey)
		suffix := entry.Key[lcp:]

		// lcp
		w.Write(binary.LittleEndian, uint16(lcp))

		// suffix length
		w.Write(binary.LittleEndian, uint16(len(suffix)))

		// suffix
		w.Write(binary.LittleEndian, []byte(suffix))

		// value length
		w.Write(binary.LittleEndian, uint16(len(entry.Value)))

		// value
		w.Write(binary.LittleEndian, entry.Value)

		// tombstone
		tombstone := uint8(0)
		if entry.Tombstone {
			tombstone = 1
		}
		w.Write(binary.LittleEndian, tombstone)

		if w.Error() != nil {
			return nil, w.Error()
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
	r := utils.NewErrorReader(reader)

	var prevKey string
	for reader.Len() > 0 {
		// lcp
		var lcp uint16
		r.Read(binary.LittleEndian, &lcp)

		// suffix length
		var suffixLen uint16
		r.Read(binary.LittleEndian, &suffixLen)

		// suffix
		suffix := make([]byte, suffixLen)
		r.Read(binary.LittleEndian, &suffix)

		// value length
		var valueLen uint16
		r.Read(binary.LittleEndian, &valueLen)

		// value
		value := make([]byte, valueLen)
		r.Read(binary.LittleEndian, &value)

		// tombstone
		var tombstone uint8
		r.Read(binary.LittleEndian, &tombstone)

		if r.Error() != nil {
			return r.Error()
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
