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
	"bytes"
	"fmt"
	"io"
	"math"
	"runtime"
	"sync"

	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/pilosa"
	"github.com/m3db/m3/src/m3ninx/x"

	"github.com/golang/protobuf/proto"
)

var (
	writerConcurrency                    = runtime.NumCPU()
	defaultInitialPostingsOffsetsSize    = 1024
	defaultInitialFSTTermsOffsetsSize    = 1024
	defaultInitialDocOffsetsSize         = 1024
	defaultInitialPostingsNeedsUnionSize = 1024
	defaultInitialIntEncoderSize         = 128
	defaultPilosaRoaringMaxContainerSize = 128
)

type writer struct {
	version Version
	builder sgmt.Builder
	size    int64

	intEncoder      *encoding.Encoder
	postingsEncoder *pilosa.Encoder
	fstWriter       *fstWriter
	docsWriter      *DocumentsWriter

	metadata            []byte
	docsDataFileWritten bool
	postingsFileWritten bool
	fstTermsFileWritten bool
	fstTermsOffsets     []uint64
	termPostingsOffsets []uint64
	fieldTermsLengths   []uint64

	// only used by versions >= 1.1
	fieldPostingsOffsets []uint64
	fieldData            *fswriter.FieldData
	fieldBuffer          proto.Buffer

	writeFSTTermsWorkers []*writeFSTTermsWorker
}

// WriterOptions is a set of options used when writing an FST.
type WriterOptions struct {
	// DisableRegistry disables the FST builder node registry cache which can
	// de-duplicate transitions that are an exact match of each other during
	// a final compilation phase, this helps compress the FST by a significant
	// amount (e.g. 2x). You can disable this to speed up high fixed cost
	// lookups to during building of the FST however.
	DisableRegistry bool
}

// NewWriter returns a new writer.
func NewWriter(opts WriterOptions) (Writer, error) {
	return newWriterWithVersion(opts, nil)
}

// newWriterWithVersion is a constructor used by tests to override version.
func newWriterWithVersion(opts WriterOptions, vers *Version) (Writer, error) {
	v := CurrentVersion
	if vers != nil {
		v = *vers
	}
	if err := v.Supported(); err != nil {
		return nil, err
	}

	docsWriter, err := NewDocumentsWriter()
	if err != nil {
		return nil, err
	}

	workers := make([]*writeFSTTermsWorker, 0, writerConcurrency)
	for i := 0; i < int(writerConcurrency); i++ {
		worker := newWriteFSTTermsWorker(i, opts)
		workers = append(workers, worker)
	}

	return &writer{
		postingsEncoder:      pilosa.NewEncoder(),
		version:              v,
		intEncoder:           encoding.NewEncoder(defaultInitialIntEncoderSize),
		fstWriter:            newFSTWriter(opts),
		docsWriter:           docsWriter,
		fstTermsOffsets:      make([]uint64, 0, defaultInitialFSTTermsOffsetsSize),
		termPostingsOffsets:  make([]uint64, 0, defaultInitialPostingsOffsetsSize),
		fieldPostingsOffsets: make([]uint64, 0, defaultInitialPostingsOffsetsSize),
		fieldTermsLengths:    make([]uint64, 0, defaultInitialPostingsOffsetsSize),
		fieldData:            &fswriter.FieldData{},

		writeFSTTermsWorkers: workers,
	}, nil
}

func (w *writer) clear() {
	w.builder = nil

	w.fstWriter.Reset(nil)
	w.intEncoder.Reset()
	w.postingsEncoder.Reset()
	w.docsWriter.Reset(DocumentsWriterOptions{})

	w.metadata = nil
	w.docsDataFileWritten = false
	w.postingsFileWritten = false
	w.fstTermsFileWritten = false
	w.fstTermsOffsets = w.fstTermsOffsets[:0]
	w.termPostingsOffsets = w.termPostingsOffsets[:0]
	w.fieldTermsLengths = w.fieldTermsLengths[:0]
	w.fieldPostingsOffsets = w.fieldPostingsOffsets[:0]
	w.fieldData.Reset()
	w.fieldBuffer.Reset()

	for _, worker := range w.writeFSTTermsWorkers {
		worker.reset(nil)
	}
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
	iter, err := w.builder.AllDocs()
	closer := x.NewSafeCloser(iter)
	defer closer.Close()
	if err != nil {
		return err
	}

	w.docsWriter.Reset(DocumentsWriterOptions{
		Iter:     iter,
		SizeHint: int(w.size),
	})
	if err := w.docsWriter.WriteDocumentsData(iow); err != nil {
		return err
	}

	w.docsDataFileWritten = true
	return closer.Close()
}

func (w *writer) WriteDocumentsIndex(iow io.Writer) error {
	if !w.docsDataFileWritten {
		return fmt.Errorf("documents data file has to be written before documents index file")
	}

	return w.docsWriter.WriteDocumentsIndex(iow)
}

func (w *writer) WritePostingsOffsets(iow io.Writer) error {
	var (
		writeFieldsPostingList = w.version.supportsFieldPostingsList()
		currentOffset          = uint64(0)
	)
	writePL := func(pl postings.List) (uint64, error) { // helper method
		// serialize the postings list
		w.postingsEncoder.Reset()
		postingsBytes, err := w.postingsEncoder.Encode(pl)
		if err != nil {
			return 0, err
		}
		return w.writePayloadAndSizeAndMagicNumber(iow, postingsBytes)
	}

	// retrieve known fields
	fields, err := w.builder.FieldsPostingsList()
	if err != nil {
		return err
	}

	termsIter, err := w.builder.TermsIterator()
	if err != nil {
		return err
	}

	// for each known field
	for fields.Next() {
		f, fieldPostingsList := fields.Current()
		// retrieve known terms for current field
		if err := termsIter.ResetField(f); err != nil {
			return err
		}

		// for each term corresponding to the current field
		terms := 0
		for termsIter.Next() {
			_, pl := termsIter.Current()
			terms++
			if pl.IsEmpty() {
				// empty postings list, no-op and write math.MaxUint64 here
				w.termPostingsOffsets = append(w.termPostingsOffsets, math.MaxUint64)
			} else {
				// write the postings list
				n, err := writePL(pl)
				if err != nil {
					return err
				}
				// update offset with the number of bytes we've written
				currentOffset += n
				// track current offset as the offset for the current field/term
				w.termPostingsOffsets = append(w.termPostingsOffsets, currentOffset)
			}
		}
		w.fieldTermsLengths = append(w.fieldTermsLengths, uint64(terms))

		// write the field level postings list
		if writeFieldsPostingList {
			if fieldPostingsList.IsEmpty() {
				// empty postings list, no-op and write math.MaxUint64 here
				w.fieldPostingsOffsets = append(w.fieldPostingsOffsets, math.MaxUint64)
			} else {
				// Write the unioned postings list out.
				n, err := writePL(fieldPostingsList)
				if err != nil {
					return err
				}
				// update offset with the number of bytes we've written
				currentOffset += n
				// track current offset as the offset for the current field
				w.fieldPostingsOffsets = append(w.fieldPostingsOffsets, currentOffset)
			}
		}

		if err := termsIter.Err(); err != nil {
			return err
		}

		if err := termsIter.Close(); err != nil {
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

type writeFSTTermsWorker struct {
	numWorker int
	fstWriter *fstWriter
	fstBuff   *bytes.Buffer

	termsIter sgmt.ReuseableTermsIterator

	closed bool

	workCh   chan writeFSTTermsArgs
	resultCh chan writeFSTTermsResult
}

type workSignal struct {
	abort bool
}

type writeFSTTermsArgs struct {
	workSignal
	field        []byte
	metadataBuff []byte
	termsOffsets []uint64
}

type writeFSTTermsResult struct {
	workSignal
	err          error
	metadataBuff []byte
	fstBuff      []byte
	numBytesFST  uint64
}

func newWriteFSTTermsWorker(
	numWorker int,
	opts WriterOptions,
) *writeFSTTermsWorker {
	w := &writeFSTTermsWorker{
		numWorker: numWorker,
		fstWriter: newFSTWriter(opts),
		fstBuff:   bytes.NewBuffer(nil),
	}
	return w
}

func (w *writeFSTTermsWorker) reset(
	termsIter sgmt.ReuseableTermsIterator,
) {
	numWorker := w.numWorker
	fstWriter := w.fstWriter
	fstBuff := w.fstBuff

	*w = writeFSTTermsWorker{}

	w.numWorker = numWorker
	w.termsIter = termsIter
	w.workCh = make(chan writeFSTTermsArgs, 32)
	w.resultCh = make(chan writeFSTTermsResult, 32)
	w.fstWriter = fstWriter
	w.fstBuff = fstBuff
}

func (w *writeFSTTermsWorker) run() {
	defer func() {
		// Cleanup.
		if err := w.termsIter.Close(); err != nil {
			w.resultCh <- writeFSTTermsResult{err: err}
		}
		w.termsIter = nil

		// Make sure consumer of result is signaled end of result publishing.
		w.resultCh <- writeFSTTermsResult{workSignal: workSignal{abort: true}}
	}()

	for args := range w.workCh {
		if args.abort {
			// Publisher side was closed cleanly.
			return
		}

		// Run and publish result.
		w.resultCh <- w.work(args)
	}
}

func (w *writeFSTTermsWorker) closePublisher() {
	if !w.closed {
		w.workCh <- writeFSTTermsArgs{workSignal: workSignal{abort: true}}
		w.closed = true
	}
}

func (w *writeFSTTermsWorker) work(args writeFSTTermsArgs) writeFSTTermsResult {
	// Reset buffers.
	w.fstBuff.Reset()
	w.fstWriter.Reset(w.fstBuff)

	f := args.field
	termsOffsets := args.termsOffsets
	numTerms := len(termsOffsets)

	if err := w.termsIter.ResetField(f); err != nil {
		return writeFSTTermsResult{err: err}
	}

	// for each term corresponding to this field
	for w.termsIter.Next() {
		t, _ := w.termsIter.Current()

		// retieve postsings offset for the current field,term
		if len(termsOffsets) == 0 {
			return writeFSTTermsResult{
				err: fmt.Errorf("postings offset not found for: field=%s, term=%s", f, t),
			}
		}

		po := termsOffsets[0]
		termsOffsets = termsOffsets[1:]

		// add the term -> posting offset into the term's fst
		if err := w.fstWriter.Add(t, po); err != nil {
			return writeFSTTermsResult{err: err}
		}
	}
	if err := w.termsIter.Err(); err != nil {
		return writeFSTTermsResult{err: err}
	}

	if len(termsOffsets) != 0 {
		return writeFSTTermsResult{
			err: fmt.Errorf("iterated terms but expected more: actual=%d, expected=%d",
				numTerms-len(termsOffsets), numTerms),
		}
	}

	// retrieve a serialized representation of the field's fst
	numBytesFST, err := w.fstWriter.Close()
	if err != nil {
		return writeFSTTermsResult{err: err}
	}

	return writeFSTTermsResult{
		metadataBuff: args.metadataBuff,
		fstBuff:      append(make([]byte, 0, len(w.fstBuff.Bytes())), w.fstBuff.Bytes()...),
		numBytesFST:  numBytesFST,
	}
}

func (w *writer) WriteFSTTerms(iow io.Writer) error {
	if !w.postingsFileWritten {
		return fmt.Errorf("postings offsets have to be written before fst terms can be written")
	}

	var (
		writeFieldsPostingList = w.version.supportsFieldPostingsList()
		currentOffset          = uint64(0) // track offset of writes into `iow`.
	)
	consume := func() error {
		closed := make([]int, 0, len(w.writeFSTTermsWorkers))
		for {
		WorkerConsumeLoop:
			for i, worker := range w.writeFSTTermsWorkers {
				for _, j := range closed {
					if i == j {
						continue WorkerConsumeLoop
					}
				}

				result := <-worker.resultCh
				if result.abort {
					closed = append(closed, i)
					continue
				}

				if err := result.err; err != nil {
					return err
				}

				if len(result.metadataBuff) > 0 {
					if _, err := iow.Write(result.metadataBuff); err != nil {
						return err
					}
					numBytesMD := uint64(len(result.metadataBuff))
					numBytesMDSize, err := w.writeUint64(iow, numBytesMD)
					if err != nil {
						return err
					}

					// update offset with the number of bytes we've written
					currentOffset += numBytesMD + numBytesMDSize
				}

				// write the fst
				if _, err := iow.Write(result.fstBuff); err != nil {
					return err
				}

				// serialize the size of the fst
				n, err := w.writeSizeAndMagicNumber(iow, result.numBytesFST)
				if err != nil {
					return err
				}

				// update offset with the number of bytes we've written
				currentOffset += result.numBytesFST + n

				// track current offset as the offset for the current field's fst
				w.fstTermsOffsets = append(w.fstTermsOffsets, currentOffset)
			}
			if len(closed) == len(w.writeFSTTermsWorkers) {
				// All workers have closed their result channels, we're done.
				return nil
			}
		}
	}

	// Run workers.
	var workersDone sync.WaitGroup
	for i := range w.writeFSTTermsWorkers {
		termsIter, err := w.builder.TermsIterator()
		if err != nil {
			return err
		}

		worker := w.writeFSTTermsWorkers[i]
		worker.reset(termsIter)
		workersDone.Add(1)
		go func(i int) {
			worker.run()
			workersDone.Done()
		}(i)
	}

	// Ensure we cleanup all workers.
	defer func() {
		// Abort workers if they haven't already closed due to successful
		for _, worker := range w.writeFSTTermsWorkers {
			worker.closePublisher()
		}
		// Wait for all workers to finish.
		workersDone.Wait()
	}()

	// Start consume worker.
	consumeCh := make(chan error)
	go func() { consumeCh <- consume() }()

	// retrieve all known fields
	fields, err := w.builder.FieldsPostingsList()
	if err != nil {
		return err
	}

	// iterate term|field postings offsets and build a fst for each field's terms
	var (
		termOffsets       = w.termPostingsOffsets
		fieldOffsets      = w.fieldPostingsOffsets
		fieldTermsLengths = w.fieldTermsLengths
		fieldIndex        = -1
	)
	for fields.Next() {
		f, _ := fields.Current()
		fieldIndex++

		var fieldsMetadata []byte

		// write fields level postings list if required
		if writeFieldsPostingList {
			po := fieldOffsets[0]
			fieldOffsets = fieldOffsets[1:]
			md, err := w.fieldsMetadata(po)
			if err != nil {
				return err
			}
			fieldsMetadata = md
		}

		numTerms := fieldTermsLengths[0]
		fieldTermsLengths = fieldTermsLengths[1:]
		offsets := termOffsets[:numTerms]
		termOffsets = termOffsets[numTerms:]

		numWorker := fieldIndex % len(w.writeFSTTermsWorkers)
		worker := w.writeFSTTermsWorkers[numWorker]

		// TODO: ensure this won't block forever if worker/consumer shutdown
		// due to an error.
		// This caused an infinite pause during testing since consume returns
		// an error and then the consumer is no longer reading results
		// and the worker channel becomes blocked.
		worker.workCh <- writeFSTTermsArgs{
			field:        append(make([]byte, 0, len(f)), f...),
			metadataBuff: append(make([]byte, 0, len(fieldsMetadata)), fieldsMetadata...),
			termsOffsets: offsets,
		}
	}

	if err := fields.Err(); err != nil {
		return err
	}

	if err := fields.Close(); err != nil {
		return err
	}

	// Signal workers no more to publish.
	for _, worker := range w.writeFSTTermsWorkers {
		worker.closePublisher()
	}
	if err := <-consumeCh; err != nil {
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

func (w *writer) fieldsMetadata(fieldPostingsOffset uint64) ([]byte, error) {
	w.fieldBuffer.Reset()
	w.fieldData.FieldPostingsListOffset = fieldPostingsOffset
	if err := w.fieldBuffer.Marshal(w.fieldData); err != nil {
		return nil, err
	}
	return w.fieldBuffer.Bytes(), nil
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
	fields, err := w.builder.FieldsPostingsList()
	if err != nil {
		return err
	}

	// insert each field into fst
	for fields.Next() {
		f, _ := fields.Current()

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
