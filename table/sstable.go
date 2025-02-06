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
	"time"

	"github.com/B1NARY-GR0UP/originium/pkg/bufferpool"
	"github.com/B1NARY-GR0UP/originium/pkg/types"
)

type SSTable struct {
	DataBlocks []Data
	MetaBlock  Meta
	IndexBlock Index
	Footer     Footer
}

type BlockHandle struct {
	Offset uint64
	Length uint64
}

func Build(entries []types.Entry, dataBlockSize, level int) (Index, []byte) {
	buf := bufferpool.Pool.Get()
	defer bufferpool.Pool.Put(buf)

	// build data blocks
	var dataBlocks []Data
	var currSize int
	var data Data
	for _, entry := range entries {
		if currSize > dataBlockSize {
			dataBlocks = append(dataBlocks, data)
			// reset
			data = Data{}
			currSize = 0
		}
		// key, value, tombstone byte sizes
		entrySize := len(entry.Key) + len(entry.Value) + 1
		currSize += entrySize
		data.Entries = append(data.Entries, entry)
	}
	if len(data.Entries) > 0 {
		dataBlocks = append(dataBlocks, data)
	}

	// build index block
	var indexBlock Index
	var offset uint64
	for _, block := range dataBlocks {
		dataBytes, err := block.Encode()
		if err != nil {
			panic(err)
		}
		length := uint64(len(dataBytes))
		indexBlock.Entries = append(indexBlock.Entries, IndexEntry{
			StartKey: block.Entries[0].Key,
			EndKey:   block.Entries[len(block.Entries)-1].Key,
			DataHandle: BlockHandle{
				Offset: offset,
				Length: length,
			},
		})
		offset += length

		// write data blocks
		if _, err = buf.Write(dataBytes); err != nil {
			panic(err)
		}
	}
	indexBlock.DataBlock = BlockHandle{
		Offset: 0,
		Length: offset,
	}

	// build meta block
	metaBlock := Meta{
		CreatedUnix: time.Now().Unix(),
		Level:       uint64(level),
	}
	metaBytes, err := metaBlock.Encode()
	if err != nil {
		panic(err)
	}
	metaOffset := offset
	metaLength := uint64(len(metaBytes))

	// write meta block
	if _, err = buf.Write(metaBytes); err != nil {
		panic(err)
	}

	// build footer
	indexBytes, err := indexBlock.Encode()
	if err != nil {
		panic(err)
	}
	indexOffset := metaOffset + metaLength
	indexLength := uint64(len(indexBytes))

	// write index block
	if _, err = buf.Write(indexBytes); err != nil {
		panic(err)
	}

	footer := Footer{
		MetaBlock: BlockHandle{
			Offset: metaOffset,
			Length: metaLength,
		},
		IndexBlock: BlockHandle{
			Offset: indexOffset,
			Length: indexLength,
		},
		Magic: _magic,
	}
	footerBytes, err := footer.Encode()
	if err != nil {
		panic(err)
	}

	// write footer
	if _, err = buf.Write(footerBytes); err != nil {
		panic(err)
	}

	return indexBlock, buf.Bytes()
}
