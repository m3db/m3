// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
)

// compressedDataWriter writes the data file for documents.
type compressedDataWriter struct {
	writer        io.Writer
	accumulator   *encoding.Encoder
	docEncoder    *Encoder
	compressor    Compressor
	currentOffset offset

	copts CompressionOptions
}

// newCompressedDataWriter returns a new compressedDataWriter.
func newCompressedDataWriter(co CompressionOptions, w io.Writer) (dataWriter, error) {
	c, err := co.newCompressor()
	if err != nil {
		return nil, err
	}
	return &compressedDataWriter{
		writer:      w,
		accumulator: encoding.NewEncoder(co.PageSize),
		docEncoder:  NewEncoder(co.PageSize),
		compressor:  c,
		copts:       co,
	}, nil
}

func (w *compressedDataWriter) Write(d doc.Document) (offset, error) {
	docBytes, err := w.docEncoder.Encode(d)
	if err != nil {
		return offset{}, err
	}

	// check if adding this document overflows the page.
	if w.currentOffset.docOffset+uint64(len(docBytes)) > uint64(w.copts.PageSize) {
		// flush accumulated docs.
		if err := w.compressAndFlush(); err != nil {
			return offset{}, err
		}
	}

	writeOffset := w.currentOffset
	w.accumulator.PutRawBytes(docBytes)
	w.currentOffset.docOffset += uint64(len(docBytes))
	return writeOffset, nil
}

func (w *compressedDataWriter) compressAndFlush() error {
	// compress the accumulated bytes.
	original := w.accumulator.Bytes()
	originalSize := len(original)
	if originalSize == 0 {
		return nil
	}

	compressed, err := w.compressor.Compress(original)
	if err != nil {
		return err
	}

	// write the compressed bytes to the writer.
	w.accumulator.Reset()
	w.accumulator.PutBytes(compressed)
	bytes := w.accumulator.Bytes()

	if _, err := w.writer.Write(bytes); err != nil {
		return err
	}

	// update offsets and reset accumulator.
	w.currentOffset.pageOffset += uint64(len(bytes))
	w.currentOffset.docOffset = 0
	w.accumulator.Reset()
	return nil
}

// Flush flushes the compressedDataWriter.
func (w *compressedDataWriter) Flush() error {
	return w.compressAndFlush()
}

// Reset resets the compressedDataWriter.
func (w *compressedDataWriter) Reset(wr io.Writer) {
	w.writer = wr
	w.compressor.Reset()
	w.accumulator.Reset()
	w.docEncoder.Reset()
	w.currentOffset = offset{}
}

// compressedDataReader is a reader for the data file for documents.
type compressedDataReader struct {
	data  []byte
	ropts ReaderOptions
}

// newCompressedDataReader returns a new compressedDataReader.
func newCompressedDataReader(ropts ReaderOptions, data []byte) (dataReader, error) {
	return &compressedDataReader{
		data:  data,
		ropts: ropts,
	}, nil
}

func (r *compressedDataReader) Read(o offset) (doc.Document, error) {
	var err error
	decompressor := r.ropts.Decompressor
	if decompressor == nil {
		// TODO(prateek): take this is as an argument to avoid repeated allocations
		decompressor, err = r.ropts.CompressionOptions.newDecompressor()
		if err != nil {
			return doc.Document{}, err
		}
	}

	if o.pageOffset >= uint64(len(r.data)) {
		return doc.Document{}, fmt.Errorf("page offset: %v is past the end of the data file", o)
	}

	dec := encoding.NewDecoder(r.data[int(o.pageOffset):])
	compressedBytes, err := dec.Bytes()
	if err != nil {
		return doc.Document{}, err
	}

	decompressedBytes, err := decompressor.Decompress(compressedBytes)
	if err != nil {
		return doc.Document{}, err
	}

	if o.docOffset >= uint64(len(decompressedBytes)) {
		return doc.Document{}, fmt.Errorf("doc offset: %v is past the end of the data file", o)
	}

	return NewDecoder().Decode(decompressedBytes[int(o.docOffset):])
}

func (r *compressedDataReader) Decompressor() (Decompressor, bool) {
	dec := r.ropts.Decompressor
	return dec, dec != nil
}
