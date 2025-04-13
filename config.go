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

package originium

import "os"

const (
	_kb = 1024
	_mb = 1024 * _kb
)

type Config struct {
	// SkipList Config
	SkipListMaxLevel int
	SkipListP        float64

	// Memtable Config
	// memtable size threshold of turning to an immutable memtable
	MemtableByteThreshold int
	ImmutableBuffer       int

	// SSTable Config
	DataBlockByteThreshold int

	// Level Config
	L0TargetNum int
	LevelRatio  int

	FileMode os.FileMode
}

var DefaultConfig = Config{
	SkipListMaxLevel:       9,
	SkipListP:              0.5,
	MemtableByteThreshold:  4 * _mb,
	ImmutableBuffer:        10,
	DataBlockByteThreshold: 4 * _kb,
	L0TargetNum:            5,
	LevelRatio:             10,
	FileMode:               0755,
}

func (c *Config) validate() error {
	if c.SkipListMaxLevel <= 0 {
		c.SkipListMaxLevel = DefaultConfig.SkipListMaxLevel
	}
	if c.SkipListP <= 0 {
		c.SkipListP = DefaultConfig.SkipListP
	}
	if c.MemtableByteThreshold <= 0 {
		c.MemtableByteThreshold = DefaultConfig.MemtableByteThreshold
	}
	if c.DataBlockByteThreshold <= 0 {
		c.DataBlockByteThreshold = DefaultConfig.DataBlockByteThreshold
	}
	if c.L0TargetNum <= 0 {
		c.L0TargetNum = DefaultConfig.L0TargetNum
	}
	if c.LevelRatio <= 0 {
		c.LevelRatio = DefaultConfig.LevelRatio
	}
	if c.FileMode <= 0 {
		c.FileMode = DefaultConfig.FileMode
	}
	return nil
}
