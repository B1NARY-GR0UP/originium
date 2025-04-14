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

package types

import (
	"strconv"
	"strings"
)

type KV struct {
	K string
	V []byte
}

func KVs(entries []Entry) []KV {
	var res []KV
	for _, entry := range entries {
		if entry.Tombstone {
			continue
		}
		res = append(res, KV{
			K: entry.Key,
			V: entry.Value,
		})
	}
	return res
}

type Key = string

func Value(entry Entry) ([]byte, bool) {
	if entry.Tombstone {
		return nil, false
	}
	return entry.Value, true
}

func KeyWithTs(key string, ts uint64) string {
	return key + "@" + strconv.FormatUint(ts, 10)
}

func IsSameKey(key1, key2 string) bool {
	return ParseKey(key1) == ParseKey(key2)
}

func ParseKey(key string) string {
	return key[:strings.LastIndex(key, "@")]
}

func ParseTs(key string) uint64 {
	if key == "" {
		return 0
	}

	ts, err := strconv.ParseUint(key[strings.LastIndex(key, "@")+1:], 10, 64)
	if err != nil {
		return 0
	}

	return ts
}

func CompareKeys(key1, key2 string) int {
	if cmp := strings.Compare(ParseKey(key1), ParseKey(key2)); cmp != 0 {
		return cmp
	}

	ts1 := ParseTs(key1)
	ts2 := ParseTs(key2)

	if ts1 < ts2 {
		return 1
	} else if ts1 > ts2 {
		return -1
	}
	return 0
}
