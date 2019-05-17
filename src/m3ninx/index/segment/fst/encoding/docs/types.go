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
	"io"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/postings"
)

// CompressionType configures the type of compression to be used.
type CompressionType uint

const (
	NoneCompressionType CompressionType = iota
	SnappyCompressionType
	DeflateCompressionType
	LZ4CompressionType
)

var (
	ValidCompressionTypes = []CompressionType{
		SnappyCompressionType,
		DeflateCompressionType,
		LZ4CompressionType,
	}
)

// String returns the string equivalent of the CompressionType.
func (c CompressionType) String() string {
	switch c {
	case NoneCompressionType:
		return "none"
	case SnappyCompressionType:
		return "snappy"
	case DeflateCompressionType:
		return "deflate"
	case LZ4CompressionType:
		return "lz4"
	}
	return "unknown"
}

// CompressionOptions configures the compression knobs.
type CompressionOptions struct {
	Type     CompressionType
	PageSize int
}

// Writer writes documents data and index to provided io.Writer(s).
type Writer interface {
	// Reset resets the writer to write the `size` documents.
	Reset(size int64)

	// WriteData writes the provided docs to the given writer.
	WriteData(iter index.IDDocIterator, w io.Writer) error

	// WriteIndex writes the documents index to the given writer.
	WriteIndex(w io.Writer) error
}

// ReaderOptions configures a Reader.
type ReaderOptions struct {
	// CompressionOptions controls the decompression knobs used
	// by the Reader.
	CompressionOptions CompressionOptions
	// Decompressor is an optional field to allow readers to avoid
	// allocations in case it's already stored.
	Decompressor Decompressor
}

// Reader is a document reader.
type Reader interface {
	// Reset resets the document reader with the provided data.
	Reset(ro ReaderOptions, docsData, idxData []byte) error

	// ReaderOptions returns the ReaderOptions backing the Reader.
	ReaderOptions() ReaderOptions

	// Range returns the range of postings.IDs the Reader provides.
	Range() (startInclusive, endExclusive postings.ID)

	// Read returns the provided document.
	Read(id postings.ID) (doc.Document, error)
}

// Compressor compresses the provided bytes.
type Compressor interface {
	// Compress compresses the provided bytes.
	Compress(b []byte) ([]byte, error)

	// Reset resets the bytes compressor.
	Reset()
}

// Decompressor decompresses the provided bytes.
type Decompressor interface {
	// CompressionOptions returns the CompressionOptions for the Decompressor.
	CompressionOptions() CompressionOptions

	// Decompress decompresses the provided bytes.
	Decompress(b []byte) ([]byte, error)

	// Reset resets the bytes decompressor.
	Reset()
}

// package local types below this line.

type offset struct {
	postings.ID
	// docOffset is the offset of the doc within the decompressed page.
	docOffset uint64
	// pageOffset is the offset of the current page.
	pageOffset uint64
}

type dataWriter interface {
	Reset(wr io.Writer)
	Flush() error
	Write(d doc.Document) (offset, error)
}

type indexWriter interface {
	Reset(wr io.Writer)
	Write(id postings.ID, o offset) error
}

type dataReader interface {
	Read(o offset) (doc.Document, error)
	Decompressor() (Decompressor, bool)
}

type indexReader interface {
	Read(id postings.ID) (offset, error)
	Base() postings.ID
	Len() int
}
