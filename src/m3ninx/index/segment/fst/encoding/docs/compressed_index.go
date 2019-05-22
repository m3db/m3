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
// all copies or substantial portions of the Software.
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

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/postings"
)

// compressedIndexWriter is a writer for the index file for documents.
type compressedIndexWriter struct {
	writer io.Writer
	enc    *encoding.Encoder
	ready  bool
	prev   postings.ID
}

// newCompressedIndexWriter returns a new compressedIndexWriter.
func newCompressedIndexWriter(w io.Writer) indexWriter {
	iw := &compressedIndexWriter{
		writer: w,
		enc:    encoding.NewEncoder(initialIndexEncoderLen),
	}
	return iw
}

// Write writes the offset for an id. IDs must be written in increasing order but can be
// non-contiguous.
func (w *compressedIndexWriter) Write(id postings.ID, o offset) error {
	if !w.ready {
		w.writeMetadata(id)
		w.ready = true
	} else {
		if id <= w.prev {
			return fmt.Errorf("postings IDs must be monotonically increasing: received %v but previous ID was %v", id, w.prev)
		}
		for i := 0; i < int(id-w.prev)-1; i++ {
			w.enc.PutUint64(emptyID)
			w.enc.PutUint64(emptyID)
		}
	}

	w.enc.PutUint64(o.pageOffset)
	w.enc.PutUint64(o.docOffset)
	w.prev = id

	return w.write()
}

func (w *compressedIndexWriter) writeMetadata(id postings.ID) {
	w.enc.PutUint64(uint64(id))
}

func (w *compressedIndexWriter) write() error {
	b := w.enc.Bytes()
	n, err := w.writer.Write(b)
	if err != nil {
		return err
	}
	if n < len(b) {
		return io.ErrShortWrite
	}
	w.enc.Reset()
	return nil
}

// Reset resets the compressedIndexWriter.
func (w *compressedIndexWriter) Reset(wr io.Writer) {
	w.writer = wr
	w.enc.Reset()
	w.ready = false
}

// compressedIndexReader is a reader for the index file for documents.
type compressedIndexReader struct {
	data  []byte
	base  postings.ID
	limit postings.ID
	len   int
}

// newCompressedIndexReader returns a new compressedIndexReader.
func newCompressedIndexReader(data []byte) (indexReader, error) {
	if len(data) == 0 {
		return &compressedIndexReader{}, nil
	}

	if len(data) < indexMetadataSize {
		return nil, io.ErrShortBuffer
	}

	payloadLen := len(data) - indexMetadataSize
	if payloadLen%16 != 0 {
		return nil, fmt.Errorf("stored fields index payload should be a multiple of 16, found %v", payloadLen%16)
	}
	count := payloadLen / 16

	r := &compressedIndexReader{
		data: data,
	}

	dec := encoding.NewDecoder(data[:8])
	base, err := dec.Uint64()
	if err != nil {
		return nil, fmt.Errorf("could not read base postings ID: %v", err)
	}
	r.base = postings.ID(base)
	r.limit = r.base + postings.ID(count)
	r.len = count
	return r, nil
}

func (r *compressedIndexReader) Read(id postings.ID) (offset, error) {
	if id < r.base || id >= r.limit {
		return offset{}, index.ErrDocNotFound
	}

	idx := r.index(id)
	dec := encoding.NewDecoder(r.data[idx:])
	pageOffset, err := dec.Uint64()
	if err != nil {
		return offset{}, err
	}
	docOffset, err := dec.Uint64()
	if err != nil {
		return offset{}, err
	}

	return offset{
		pageOffset: pageOffset,
		docOffset:   docOffset,
	}, nil
}

// Base returns the base postings ID.
func (r *compressedIndexReader) Base() postings.ID {
	return r.base
}

// Len returns the number of postings IDs.
func (r *compressedIndexReader) Len() int {
	return r.len
}

func (r *compressedIndexReader) index(id postings.ID) int {
	return (int(id-r.base) * 16) + indexMetadataSize
}
