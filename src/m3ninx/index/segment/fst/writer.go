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
	"fmt"
	"io"

	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/pilosa"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/x"
)

var (
	defaultInitialPostingsOffsetsSize = 1024
	defaultInitialFSTTermsOffsetsSize = 1024
	defaultInitialDocOffsetsSize      = 1024
	defaultInitialIntEncoderSize      = 128
)

type writer struct {
	version Version
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
	docOffsets          []docOffset
	fstTermsOffsets     []uint64
	termPostingsOffsets []uint64

	// only used by versions >= 1.1
	fieldPostingsOffsets []uint64
}

// WriterOptions is a set of options used when writing an FST.
type WriterOptions struct {
	// DisableRegistry disables the FST builder node registry cache which can
	// de-duplicate transitions that are an exact match of each other during
	// a final compilation phase, this helps compress the FST by a significant
	// amount (e.g. 2x). You can disable this to speed up high fixed cost
	// lookups to during building of the FST however.
	DisableRegistry bool

	// ForTestsOnly_Version allows users to override behaviour of the
	// writer. NB: this is intended to be used only during tests.
	ForTestsOnly_Version *Version
}

// NewWriter returns a new writer.
func NewWriter(opts WriterOptions) (Writer, error) {
	v := CurrentVersion
	if opts.ForTestsOnly_Version != nil {
		v = *opts.ForTestsOnly_Version
	}
	if err := v.Validate(); err != nil {
		return nil, err
	}

	return &writer{
		version:              v,
		intEncoder:           encoding.NewEncoder(defaultInitialIntEncoderSize),
		postingsEncoder:      pilosa.NewEncoder(),
		fstWriter:            newFSTWriter(opts),
		docDataWriter:        docs.NewDataWriter(nil),
		docIndexWriter:       docs.NewIndexWriter(nil),
		docOffsets:           make([]docOffset, 0, defaultInitialDocOffsetsSize),
		fstTermsOffsets:      make([]uint64, 0, defaultInitialFSTTermsOffsetsSize),
		termPostingsOffsets:  make([]uint64, 0, defaultInitialPostingsOffsetsSize),
		fieldPostingsOffsets: make([]uint64, 0, defaultInitialPostingsOffsetsSize),
	}, nil
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
	w.docOffsets = w.docOffsets[:0]
	w.fstTermsOffsets = w.fstTermsOffsets[:0]
	w.termPostingsOffsets = w.termPostingsOffsets[:0]
	w.fieldPostingsOffsets = w.fieldPostingsOffsets[:0]
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
	return w.version.Major
}

func (w *writer) MinorVersion() int {
	return w.version.Minor
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
	var (
		writeFieldsPostingList = w.version.supportsFieldPostingsList()
		currentOffset          = uint64(0)
	)
	writePL := func(pl postings.List) error { // helper method
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
		return nil
	}

	// retrieve known fields
	fields, err := w.builder.Fields()
	if err != nil {
		return err
	}

	// for each known field
	for fields.Next() {
		f := fields.Current()
		// retrieve known terms for current field
		terms, err := w.builder.Terms(f)
		if err != nil {
			return err
		}

		fieldsPL := roaring.NewPostingsList()
		// for each term corresponding to the current field
		for terms.Next() {
			_, pl := terms.Current()
			// write the postings list
			if err := writePL(pl); err != nil {
				return err
			}
			// track current offset as the offset for the current field/term
			w.termPostingsOffsets = append(w.termPostingsOffsets, currentOffset)

			// update field level postings list
			if writeFieldsPostingList {
				if err := fieldsPL.Union(pl); err != nil {
					return err
				}
			}
		}

		// write the field level postings list
		if writeFieldsPostingList {
			if err := writePL(fieldsPL); err != nil {
				return err
			}
			// track current offset as the offset for the current field
			w.fieldPostingsOffsets = append(w.fieldPostingsOffsets, currentOffset)
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

	var (
		writeFieldsPostingList = w.version.supportsFieldPostingsList()
		currentOffset          = uint64(0) // track offset of writes into `iow`.
	)

	// retrieve all known fields
	fields, err := w.builder.Fields()
	if err != nil {
		return err
	}

	// iterate term|field postings offsets
	var (
		termOffsets  = w.termPostingsOffsets
		fieldOffsets = w.fieldPostingsOffsets
	)

	// build a fst for each field's terms
	for fields.Next() {
		f := fields.Current()

		// write fields level postings list if required
		if writeFieldsPostingList {
			po := fieldOffsets[0]
			fieldOffsets = fieldOffsets[1:]
			md, err := fieldsMetadata(po)
			if err != nil {
				return err
			}
			if _, err := iow.Write(md); err != nil {
				return err
			}
			numBytesMD := uint64(len(md))
			numBytesMDSize, err := w.writeUint64(iow, numBytesMD)
			if err != nil {
				return err
			}
			currentOffset += numBytesMD + numBytesMDSize
		}

		// reset writer for this field's fst
		if err := w.fstWriter.Reset(iow); err != nil {
			return err
		}

		// retrieve all terms for this field
		terms, err := w.builder.Terms(f)
		if err != nil {
			return err
		}

		// for each term corresponding to this field
		for terms.Next() {
			t, _ := terms.Current()

			// retieve postsings offset for the current field,term
			if len(termOffsets) == 0 {
				return fmt.Errorf("postings offset not found for: field=%s, term=%s", f, t)
			}

			po := termOffsets[0]
			termOffsets = termOffsets[1:]

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
		w.fstTermsOffsets = append(w.fstTermsOffsets, currentOffset)
	}

	if err := fields.Err(); err != nil {
		return err
	}

	if err := fields.Close(); err != nil {
		return err
	}

	// make sure we consumed all the postings offsets
	if len(termOffsets) != 0 {
		return fmt.Errorf("term postings offsets remain at end of terms: remaining=%d",
			len(termOffsets))
	}

	// make sure we consumed all the postings offsets
	if len(fieldOffsets) != 0 {
		return fmt.Errorf("field postings offsets remain at end of terms: remaining=%d",
			len(fieldOffsets))
	}

	// all good!
	w.fstTermsFileWritten = true
	return nil
}

func fieldsMetadata(fieldPostingsOffset uint64) ([]byte, error) {
	d := fswriter.FieldData{FieldPostingsListOffset: fieldPostingsOffset}
	return d.Marshal()
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
	offsets := w.fstTermsOffsets

	// retrieve all known fields
	fields, err := w.builder.Fields()
	if err != nil {
		return err
	}

	// insert each field into fst
	for fields.Next() {
		f := fields.Current()

		// get offset for this field's term fst
		if len(offsets) == 0 {
			return fmt.Errorf("fst field offset not found for: field=%s", f)
		}

		offset := offsets[0]
		offsets = offsets[1:]

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
	if len(offsets) != 0 {
		return fmt.Errorf("field offsets remain at end of fields: remaining=%d",
			len(offsets))
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

func (w *writer) writeUint64(iow io.Writer, x uint64) (uint64, error) {
	// serialize the size, magicNumber
	w.intEncoder.Reset()
	w.intEncoder.PutUint64(x)
	xBytes := w.intEncoder.Bytes()

	// write out the size
	n, err := iow.Write(xBytes)
	if err != nil {
		return 0, err
	}
	return uint64(n), nil
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

func defaultV1Metadata() fswriter.Metadata {
	return fswriter.Metadata{
		PostingsFormat: fswriter.PostingsFormat_PILOSAV1_POSTINGS_FORMAT,
	}
}

type docOffset struct {
	postings.ID
	offset uint64
}
