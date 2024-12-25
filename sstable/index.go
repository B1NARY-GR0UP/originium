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

package sstable

import (
	"bytes"
	"encoding/binary"

	"github.com/B1NARY-GR0UP/originium/pkg/bufferpool"
	"github.com/B1NARY-GR0UP/originium/pkg/types"
	"github.com/B1NARY-GR0UP/originium/pkg/utils"
)

// Index Block
type Index struct {
	// BlockHandle of all data blocks of this sstable
	DataBlock BlockHandle
	Entries   []IndexEntry
}

// IndexEntry include index of a sstable data block
type IndexEntry struct {
	// StartKey of each Data block
	StartKey string
	// EndKey of each Data block
	EndKey string
	// offset and length of each data block
	DataHandle BlockHandle
}

// Search data block included the key
func (i *Index) Search(key types.Key) (BlockHandle, bool) {
	n := len(i.Entries)
	if n == 0 {
		return BlockHandle{}, false
	}

	// check if the key is beyond this sstable
	if key > i.Entries[n-1].EndKey {
		return BlockHandle{}, false
	}

	low, high := 0, n-1
	for low <= high {
		mid := low + ((high - low) >> 1)
		if i.Entries[mid].StartKey > key {
			high = mid - 1
		} else {
			if mid == n-1 || i.Entries[mid+1].StartKey > key {
				return i.Entries[mid].DataHandle, true
			}
			low = mid + 1
		}
	}
	return BlockHandle{}, false
}

func (i *Index) Scan(start, end types.Key) []BlockHandle {
	var res []BlockHandle
	for _, entry := range i.Entries {
		if entry.EndKey >= start && entry.StartKey <= end {
			res = append(res, entry.DataHandle)
		}
	}
	return res
}

func (i *Index) Encode() ([]byte, error) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	if err := binary.Write(buf, binary.LittleEndian, i.DataBlock.Offset); err != nil {
		return nil, err
	}
	if err := binary.Write(buf, binary.LittleEndian, i.DataBlock.Length); err != nil {
		return nil, err
	}

	for _, entry := range i.Entries {
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(entry.StartKey))); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, []byte(entry.StartKey)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, uint16(len(entry.EndKey))); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, []byte(entry.EndKey)); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.DataHandle.Offset); err != nil {
			return nil, err
		}
		if err := binary.Write(buf, binary.LittleEndian, entry.DataHandle.Length); err != nil {
			return nil, err
		}
	}

	compressed := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(compressed)

	if err := utils.Compress(buf, compressed); err != nil {
		return nil, err
	}
	return compressed.Bytes(), nil
}

func (i *Index) Decode(index []byte) error {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	if err := utils.Decompress(bytes.NewReader(index), buf); err != nil {
		return err
	}

	reader := bytes.NewReader(buf.Bytes())

	if err := binary.Read(reader, binary.LittleEndian, &i.DataBlock.Offset); err != nil {
		return err
	}
	if err := binary.Read(reader, binary.LittleEndian, &i.DataBlock.Length); err != nil {
		return err
	}

	for reader.Len() > 0 {
		var startKeyLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &startKeyLen); err != nil {
			return err
		}
		startKey := make([]byte, startKeyLen)
		if err := binary.Read(reader, binary.LittleEndian, &startKey); err != nil {
			return err
		}
		var endKeyLen uint16
		if err := binary.Read(reader, binary.LittleEndian, &endKeyLen); err != nil {
			return err
		}
		endKey := make([]byte, endKeyLen)
		if err := binary.Read(reader, binary.LittleEndian, &endKey); err != nil {
			return err
		}
		var offset uint64
		if err := binary.Read(reader, binary.LittleEndian, &offset); err != nil {
			return err
		}
		var length uint64
		if err := binary.Read(reader, binary.LittleEndian, &length); err != nil {
			return err
		}
		i.Entries = append(i.Entries, IndexEntry{
			StartKey: string(startKey),
			EndKey:   string(endKey),
			DataHandle: BlockHandle{
				Offset: offset,
				Length: length,
			},
		})
	}
	return nil
}
