// Copyright (c) 2018 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package fs

import (
	"io"

	"github.com/couchbase/vellum"
)

// fstWriter is a writer to help construct an FST.
type fstWriter struct {
	bytesWritten uint64
	writer       io.Writer
	builder      *vellum.Builder
}

func newFSTWriter() *fstWriter {
	return &fstWriter{}
}

func (f *fstWriter) Clear() {
	f.bytesWritten = 0
	f.writer = nil
	f.builder = nil
}

func (f *fstWriter) Write(p []byte) (int, error) {
	n, err := f.writer.Write(p)
	if err != nil {
		return 0, err
	}
	f.bytesWritten += uint64(n)
	return n, nil
}

func (f *fstWriter) Reset(w io.Writer) error {
	f.Clear()
	f.writer = w

	// TODO(prateek): builderopts for vellum
	builder, err := vellum.New(f, nil)
	if err != nil {
		return err
	}
	f.builder = builder
	return nil
}

func (f *fstWriter) Add(b []byte, v uint64) error {
	return f.builder.Insert(b, v)
}

func (f *fstWriter) Close() (uint64, error) {
	err := f.builder.Close()
	if err != nil {
		return 0, nil
	}
	bytesWritten := f.bytesWritten
	f.Clear()
	return bytesWritten, nil
}
