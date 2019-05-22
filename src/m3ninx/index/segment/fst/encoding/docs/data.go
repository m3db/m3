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
// all copies or substantial portions of the Softwarw.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package docs

import (
	"fmt"
	"io"

	"github.com/m3db/m3/src/m3ninx/doc"
)

const initialDataEncoderLen = 1024

// rawDataWriter writes the data file for documents.
type rawDataWriter struct {
	writer        io.Writer
	enc           *Encoder
	currentOffset offset
}

// newRawDataWriter returns a new rawDataWriter.
func newRawDataWriter(w io.Writer) *rawDataWriter {
	return &rawDataWriter{
		writer: w,
		enc:    NewEncoder(initialDataEncoderLen),
	}
}

func (w *rawDataWriter) Write(d doc.Document) (offset, error) {
	bytes, err := w.enc.Encode(d)
	if err != nil {
		return offset{}, err
	}

	n, err := w.writer.Write(bytes)
	if err != nil {
		return offset{}, err
	}

	if n < len(bytes) {
		return offset{}, io.ErrShortWrite
	}

	current := w.currentOffset
	w.currentOffset.docOffset += uint64(n)
	return current, nil
}

func (w *rawDataWriter) Flush() error {
	return nil
}

// Reset resets the rawDataWriter.
func (w *rawDataWriter) Reset(wr io.Writer) {
	w.writer = wr
	w.enc.Reset()
	w.currentOffset = offset{}
}

// rawDataReader is a reader for the data file for documents.
type rawDataReader struct {
	data []byte
}

// newRawDataReader returns a new rawDataReader.
func newRawDataReader(data []byte) dataReader {
	return &rawDataReader{
		data: data,
	}
}

// Read reads the document at the provided offset.
func (r *rawDataReader) Read(o offset) (doc.Document, error) {
	if o.docOffset >= uint64(len(r.data)) {
		return doc.Document{}, fmt.Errorf("invalid offset: %v is past the end of the data file", o.docOffset)
	}
	return NewDecoder().Decode(r.data[int(o.docOffset):])
}

func (r *rawDataReader) Decompressor() (Decompressor, bool) {
	return nil, false
}
