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
		if types.CompareKeys(d.Entries[mid].Key, key) < 0 {
			low = mid + 1
		} else if types.CompareKeys(d.Entries[mid].Key, key) > 0 {
			high = mid - 1
		} else {
			return d.Entries[mid], true
		}
	}
	return types.Entry{}, false
}

func (d *Data) LowerBound(key types.Key) (types.Entry, bool) {
	low, high := 0, len(d.Entries)-1
	for low <= high {
		mid := low + ((high - low) >> 1)
		if types.CompareKeys(d.Entries[mid].Key, key) >= 0 {
			if mid == 0 || types.CompareKeys(d.Entries[mid-1].Key, key) < 0 {
				return d.Entries[mid], true
			}
			high = mid - 1
		} else {
			low = mid + 1
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
		if types.CompareKeys(d.Entries[mid].Key, start) >= 0 {
			if mid == 0 || types.CompareKeys(d.Entries[mid-1].Key, start) < 0 {
				// used as return
				found = true
				break
			}
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	for i := mid; i < len(d.Entries) && types.CompareKeys(d.Entries[i].Key, end) < 0 && found; i++ {
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

		// version
		version := uint64(entry.Version)
		w.Write(binary.LittleEndian, version)

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

		var version uint64
		r.Read(binary.LittleEndian, &version)

		if r.Error() != nil {
			return r.Error()
		}

		key := prevKey[:lcp] + string(suffix)
		d.Entries = append(d.Entries, types.Entry{
			Key:       key,
			Value:     value,
			Tombstone: tombstone == 1,
			Version:   int64(version),
		})

		prevKey = key
	}
	return nil
}
