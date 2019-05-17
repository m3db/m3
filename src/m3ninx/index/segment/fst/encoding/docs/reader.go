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
	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/postings"
)

type reader struct {
	copts     CompressionOptions
	docsData  []byte
	indexData []byte

	data  dataReader
	index indexReader
}

var _ Reader = &reader{}

// NewReader returns a new reader.
func NewReader(ro ReaderOptions, docData, docIndex []byte) (Reader, error) {
	r := &reader{}
	if err := r.Reset(ro, docData, docIndex); err != nil {
		return nil, err
	}
	return r, nil
}

func (r *reader) ReaderOptions() ReaderOptions {
	ro := ReaderOptions{
		CompressionOptions: r.copts,
	}
	if dec, ok := r.data.Decompressor(); ok {
		ro.Decompressor = dec
	}
	return ro
}

func (r *reader) Reset(ro ReaderOptions, docData, indexData []byte) error {
	var (
		dataReader  dataReader
		indexReader indexReader
		err         error
	)
	if ro.CompressionOptions.Type == NoneCompressionType {
		dataReader = newRawDataReader(docData)
		indexReader, err = newRawIndexReader(indexData)
	} else {
		dataReader, err = newCompressedDataReader(ro, docData)
		if err != nil {
			return err
		}
		indexReader, err = newCompressedIndexReader(indexData)
	}
	if err != nil {
		return err
	}
	r.data = dataReader
	r.index = indexReader
	r.copts = ro.CompressionOptions
	r.docsData = docData
	r.indexData = indexData
	return nil
}

func (r *reader) Range() (postings.ID, postings.ID) {
	// NB(jeromefroe): Currently we assume the postings IDs are contiguous.
	startInclusive := r.index.Base()
	endExclusive := startInclusive + postings.ID(r.index.Len())
	return startInclusive, endExclusive
}

func (r *reader) Read(id postings.ID) (doc.Document, error) {
	offset, err := r.index.Read(id)
	if err != nil {
		return doc.Document{}, err
	}
	return r.data.Read(offset)
}
