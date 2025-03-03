// Copyright 2025 BINARY Members
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
	"bytes"
	"encoding/binary"
	"io"
)

type ErrorWriter struct {
	buf *bytes.Buffer
	err error
}

func NewErrorWriter(buf *bytes.Buffer) *ErrorWriter {
	return &ErrorWriter{
		buf: buf,
		err: nil,
	}
}

func (w *ErrorWriter) Write(order binary.ByteOrder, data any) {
	if w.err != nil {
		return
	}
	w.err = binary.Write(w.buf, order, data)
}

func (w *ErrorWriter) Error() error {
	return w.err
}

type ErrorReader struct {
	r   io.Reader
	err error
}

func NewErrorReader(r io.Reader) *ErrorReader {
	return &ErrorReader{
		r:   r,
		err: nil,
	}
}

func (r *ErrorReader) Read(order binary.ByteOrder, data any) {
	if r.err != nil {
		return
	}
	r.err = binary.Read(r.r, order, data)
}

func (r *ErrorReader) Error() error {
	return r.err
}
