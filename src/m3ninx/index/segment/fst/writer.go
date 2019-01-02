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

package fst

import (
	"errors"
	"fmt"
	"io"

	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/pilosa"
	"github.com/m3db/m3/src/m3ninx/x"

	"github.com/pilosa/pilosa/roaring"
)

var (
	defaultInitialPostingsOffsetsMapSize = 1024
	defaultInitialFSTTermsOffsetsMapSize = 1024
	defaultInitialDocOffsetsSize         = 1024
	defaultInitialIntEncoderSize         = 128

	errUnableToFindPostingsOffset = errors.New("internal error: unable to find postings offset")
	errUnableToFindFSTTermsOffset = errors.New("internal error: unable to find fst terms offset")
)

type writer struct {
	builder sgmt.Builder
	size    int64

	intEncoder      *encoding.Encoder
	postingsEncoder *pilosa.Encoder
	fstWriter       *fstWriter
	docDataWriter   *docs.DataWriter
	docIndexWriter  *docs.IndexWriter

	metadata            []byte
	docsDataFileWritten bool
	postingsFileWritten bool
	fstTermsFileWritten bool
	postingsOffsets     *roaring.Bitmap
	fstTermsOffsets     *roaring.Bitmap
	docOffsets          []docOffset
}

// NewWriter returns a new writer.
func NewWriter() Writer {
	return &writer{
		intEncoder:      encoding.NewEncoder(defaultInitialIntEncoderSize),
		postingsEncoder: pilosa.NewEncoder(),
		fstWriter:       newFSTWriter(),
		docDataWriter:   docs.NewDataWriter(nil),
		docIndexWriter:  docs.NewIndexWriter(nil),
		postingsOffsets: roaring.NewBitmap(),
		fstTermsOffsets: roaring.NewBitmap(),
		docOffsets:      make([]docOffset, 0, defaultInitialDocOffsetsSize),
	}
}

func (w *writer) clear() {
	w.builder = nil

	w.fstWriter.Reset(nil)
	w.intEncoder.Reset()
	w.postingsEncoder.Reset()
	w.docDataWriter.Reset(nil)
	w.docIndexWriter.Reset(nil)

	w.metadata = nil
	w.docsDataFileWritten = false
	w.postingsFileWritten = false
	w.fstTermsFileWritten = false
	// NB(r): Use a call to reset here instead of creating a new bitmaps
	// when roaring supports a call to reset.
	w.postingsOffsets = roaring.NewBitmap()
	w.fstTermsOffsets = roaring.NewBitmap()
	w.docOffsets = w.docOffsets[:0]
}

func (w *writer) Reset(b sgmt.Builder) error {
	w.clear()

	if b == nil {
		return nil
	}

	numDocs := len(b.Docs())
	metadata := defaultV1Metadata()
	metadata.NumDocs = int64(numDocs)
	metadataBytes, err := metadata.Marshal()
	if err != nil {
		return err
	}

	w.metadata = metadataBytes
	w.builder = b
	w.size = int64(numDocs)
	return nil
}

func (w *writer) MajorVersion() int {
	return MajorVersion
}

func (w *writer) MinorVersion() int {
	return MinorVersion
}

func (w *writer) Metadata() []byte {
	return w.metadata
}

func (w *writer) WriteDocumentsData(iow io.Writer) error {
	w.docDataWriter.Reset(iow)

	iter, err := w.builder.AllDocs()
	closer := x.NewSafeCloser(iter)
	defer closer.Close()
	if err != nil {
		return err
	}

	var currOffset uint64
	if int64(cap(w.docOffsets)) < w.size {
		w.docOffsets = make([]docOffset, 0, w.size)
	}
	for iter.Next() {
		id, doc := iter.PostingsID(), iter.Current()
		n, err := w.docDataWriter.Write(doc)
		if err != nil {
			return err
		}
		w.docOffsets = append(w.docOffsets, docOffset{ID: id, offset: currOffset})
		currOffset += uint64(n)
	}

	w.docsDataFileWritten = true
	return closer.Close()
}

func (w *writer) WriteDocumentsIndex(iow io.Writer) error {
	if !w.docsDataFileWritten {
		return fmt.Errorf("documents data file has to be written before documents index file")
	}

	w.docIndexWriter.Reset(iow)

	for _, do := range w.docOffsets {
		if err := w.docIndexWriter.Write(do.ID, do.offset); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) WritePostingsOffsets(iow io.Writer) error {
	currentOffset := uint64(0)

	// retrieve known fields
	fields, err := w.builder.FieldsIterable().Fields()
	if err != nil {
		return err
	}

	// for each known field
	for fields.Next() {
		f := fields.Current()
		// retrieve known terms for current field
		terms, err := w.builder.TermsIterable().Terms(f)
		if err != nil {
			return err
		}

		// for each term corresponding to the current field
		for terms.Next() {
			_, pl := terms.Current()

			// serialize the postings list
			w.postingsEncoder.Reset()
			postingsBytes, err := w.postingsEncoder.Encode(pl)
			if err != nil {
				return err
			}

			n, err := w.writePayloadAndSizeAndMagicNumber(iow, postingsBytes)
			if err != nil {
				return err
			}

			// update offset with the number of bytes we've written
			currentOffset += n

			// track current offset as the offset for the current field/term
			if err := w.addPostingsOffset(currentOffset); err != nil {
				return err
			}
		}

		if err := terms.Err(); err != nil {
			return err
		}

		if err := terms.Close(); err != nil {
			return err
		}
	}

	if err := fields.Err(); err != nil {
		return err
	}

	if err := fields.Close(); err != nil {
		return err
	}

	w.postingsFileWritten = true
	return nil
}

func (w *writer) WriteFSTTerms(iow io.Writer) error {
	if !w.postingsFileWritten {
		return fmt.Errorf("postings offsets have to be written before fst terms can be written")
	}

	// track offset of writes into `iow`.
	currentOffset := uint64(0)

	// retrieve all known fields
	fields, err := w.builder.FieldsIterable().Fields()
	if err != nil {
		return err
	}

	// iterate postings offsets
	offsets := w.postingsOffsets.Iterator()

	// build a fst for each field's terms
	for fields.Next() {
		f := fields.Current()
		// reset writer for this field's fst
		if err := w.fstWriter.Reset(iow); err != nil {
			return err
		}

		// retrieve all terms for this field
		terms, err := w.builder.TermsIterable().Terms(f)
		if err != nil {
			return err
		}

		// for each term corresponding to this field
		for terms.Next() {
			t, _ := terms.Current()

			// retieve postsings offset for the current field,term
			po, eof := offsets.Next()
			if eof {
				return fmt.Errorf("postings offset not found for: field=%s, term=%s", f, t)
			}

			// add the term -> posting offset into the term's fst
			if err := w.fstWriter.Add(t, po); err != nil {
				return err
			}
		}
		if err := terms.Err(); err != nil {
			return err
		}

		if err := terms.Close(); err != nil {
			return err
		}

		// retrieve a serialized representation of the field's fst
		numBytesFST, err := w.fstWriter.Close()
		if err != nil {
			return err
		}

		// serialize the size of the fst
		n, err := w.writeSizeAndMagicNumber(iow, numBytesFST)
		if err != nil {
			return err
		}

		// update offset with the number of bytes we've written
		currentOffset += numBytesFST + n

		// track current offset as the offset for the current field's fst
		if err := w.addFSTTermsOffset(currentOffset); err != nil {
			return err
		}
	}

	if err := fields.Err(); err != nil {
		return err
	}

	if err := fields.Close(); err != nil {
		return err
	}

	// make sure we consumed all the postings offsets
	if _, eof := offsets.Next(); !eof {
		numUnexpected := 0
		for eof {
			numUnexpected++
			_, eof = offsets.Next()
		}
		return fmt.Errorf("postings offsets remain at end of terms: remaining=%d",
			numUnexpected)
	}

	// all good!
	w.fstTermsFileWritten = true
	return nil
}

func (w *writer) WriteFSTFields(iow io.Writer) error {
	if !w.fstTermsFileWritten {
		return fmt.Errorf("fst terms files have to be written before fst fields can be written")
	}

	// reset fst writer
	if err := w.fstWriter.Reset(iow); err != nil {
		return err
	}

	// iterate field offsets
	offsets := w.fstTermsOffsets.Iterator()

	// retrieve all known fields
	fields, err := w.builder.FieldsIterable().Fields()
	if err != nil {
		return err
	}

	// insert each field into fst
	for fields.Next() {
		f := fields.Current()

		// get offset for this field's term fst
		offset, eof := offsets.Next()
		if eof {
			return fmt.Errorf("fst field offset not found for: field=%s", f)
		}

		// add field, offset into fst
		if err := w.fstWriter.Add(f, offset); err != nil {
			return err
		}
	}

	if err := fields.Err(); err != nil {
		return err
	}

	if err := fields.Close(); err != nil {
		return err
	}

	// flush the fst writer
	_, err = w.fstWriter.Close()

	// make sure we consumed all the postings offsets
	if _, eof := offsets.Next(); !eof {
		numUnexpected := 0
		for eof {
			numUnexpected++
			_, eof = offsets.Next()
		}
		return fmt.Errorf("field offsets remain at end of fields: remaining=%d",
			numUnexpected)
	}

	return err
}

// given a payload []byte, and io.Writer; this method writes the following data out to the writer
// | payload - len(payload) bytes | 8 bytes for uint64 (size of payload) | 8 bytes for `magicNumber` |
func (w *writer) writePayloadAndSizeAndMagicNumber(iow io.Writer, payload []byte) (uint64, error) {
	numBytesWritten := uint64(0)
	size, err := iow.Write(payload)
	if err != nil {
		return 0, err
	}
	numBytesWritten += uint64(size)
	n, err := w.writeSizeAndMagicNumber(iow, uint64(size))
	if err != nil {
		return 0, err
	}
	numBytesWritten += n
	return numBytesWritten, nil
}

func (w *writer) writeSizeAndMagicNumber(iow io.Writer, size uint64) (uint64, error) {
	// serialize the size, magicNumber
	w.intEncoder.Reset()
	w.intEncoder.PutUint64(size)
	w.intEncoder.PutUint64(uint64(magicNumber))
	sizeBytes := w.intEncoder.Bytes()

	// write out the size
	n, err := iow.Write(sizeBytes)
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
}

func (w *writer) addFSTTermsOffset(offset uint64) error {
	if !w.fstTermsOffsets.DirectAdd(offset) {
		return fmt.Errorf("fst terms offset already exists: %d", offset)
	}
	return nil
}

func (w *writer) addPostingsOffset(offset uint64) error {
	if !w.postingsOffsets.DirectAdd(offset) {
		return fmt.Errorf("postings offset already exists: %d", offset)
	}
	return nil
}

func defaultV1Metadata() fswriter.Metadata {
	return fswriter.Metadata{
		PostingsFormat: fswriter.PostingsFormat_PILOSAV1_POSTINGS_FORMAT,
	}
}

type docOffset struct {
	postings.ID
	offset uint64
}
