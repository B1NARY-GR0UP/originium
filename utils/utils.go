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

package utils

import (
	"crypto/sha1"
	"encoding/binary"
	"io"
	"time"

	"github.com/B1NARY-GR0UP/originium/pkg/logger"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/cloudwego/frugal"
	"github.com/klauspost/compress/s2"
)

func Elapsed(now time.Time, logger logger.Logger, msg string) {
	logger.Infof("%s elapsed: %s", msg, time.Since(now))
}

func TMarshal(data thrift.TStruct) ([]byte, error) {
	buf := make([]byte, frugal.EncodedSize(data))
	if _, err := frugal.EncodeObject(buf, nil, data); err != nil {
		return nil, err
	}
	return buf, nil
}

func TUnmarshal(data []byte, v thrift.TStruct) error {
	if _, err := frugal.DecodeObject(data, v); err != nil {
		return err
	}
	return nil
}

// LCP length of Longest Common Prefix
func LCP(a, b string) int {
	n := min(len(a), len(b))
	var i int
	for i < n && a[i] == b[i] {
		i++
	}
	return i
}

func Pow(x, n int) int {
	res := 1
	for range n {
		res *= x
	}
	return res
}

func Compress(src io.Reader, dst io.Writer) error {
	enc := s2.NewWriter(dst)
	_, err := io.Copy(enc, src)
	if err != nil {
		_ = enc.Close()
		return err
	}
	return enc.Close()
}

func Decompress(src io.Reader, dst io.Writer) error {
	dec := s2.NewReader(src)
	_, err := io.Copy(dst, dec)
	return err
}

func Magic(input string) uint64 {
	hash := sha1.Sum([]byte(input))
	return binary.BigEndian.Uint64(hash[:8])
}
