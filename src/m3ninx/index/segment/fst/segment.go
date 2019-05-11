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
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	"github.com/m3db/m3/src/m3ninx/index"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/pilosa"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/x"
	xerrors "github.com/m3db/m3/src/x/errors"
	pilosaroaring "github.com/m3db/pilosa/roaring"
	"github.com/m3db/vellum"
)

var (
	errReaderClosed            = errors.New("segment is closed")
	errReaderNilRegexp         = errors.New("nil regexp provided")
	errUnsupportedMajorVersion = errors.New("unsupported major version")
	errDocumentsDataUnset      = errors.New("documents data bytes are not set")
	errDocumentsIdxUnset       = errors.New("documents index bytes are not set")
	errPostingsDataUnset       = errors.New("postings data bytes are not set")
	errFSTTermsDataUnset       = errors.New("fst terms data bytes are not set")
	errFSTFieldsDataUnset      = errors.New("fst fields data bytes are not set")
)

// SegmentData represent the collection of required parameters to construct a Segment.
type SegmentData struct {
	Version  Version
	Metadata []byte

	DocsData      []byte
	DocsIdxData   []byte
	PostingsData  []byte
	FSTTermsData  []byte
	FSTFieldsData []byte

	// DocsReader is an alternative to specifying
	// the docs data and docs idx data if the documents
	// already reside in memory and we want to use the
	// in memory references instead.
	DocsReader *docs.SliceReader

	Closer io.Closer
}

// Validate validates the provided segment data, returning an error if it's not.
func (sd SegmentData) Validate() error {
	if err := sd.Version.Supported(); err != nil {
		return err
	}

	if sd.PostingsData == nil {
		return errPostingsDataUnset
	}

	if sd.FSTTermsData == nil {
		return errFSTTermsDataUnset
	}

	if sd.FSTFieldsData == nil {
		return errFSTFieldsDataUnset
	}

	if sd.DocsReader == nil {
		if sd.DocsData == nil {
			return errDocumentsDataUnset
		}

		if sd.DocsIdxData == nil {
			return errDocumentsIdxUnset
		}
	}

	return nil
}

// NewSegment returns a new Segment backed by the provided options.
// NB(prateek): this method only assumes ownership of the data if it returns a nil error,
// otherwise, the user is expected to handle the lifecycle of the input.
func NewSegment(data SegmentData, opts Options) (Segment, error) {
	if err := data.Validate(); err != nil {
		return nil, err
	}

	metadata := fswriter.Metadata{}
	if err := metadata.Unmarshal(data.Metadata); err != nil {
		return nil, err
	}

	if metadata.PostingsFormat != fswriter.PostingsFormat_PILOSAV1_POSTINGS_FORMAT {
		return nil, fmt.Errorf("unsupported postings format: %v", metadata.PostingsFormat.String())
	}

	fieldsFST, err := vellum.Load(data.FSTFieldsData)
	if err != nil {
		return nil, fmt.Errorf("unable to load fields fst: %v", err)
	}

	var (
		docsSliceReader = data.DocsReader
		docsDataReader  *docs.DataReader
		docsIndexReader *docs.IndexReader
		startInclusive  postings.ID
		endExclusive    postings.ID
	)
	if docsSliceReader != nil {
		startInclusive = docsSliceReader.Base()
		endExclusive = startInclusive + postings.ID(docsSliceReader.Len())
	} else {
		docsDataReader = docs.NewDataReader(data.DocsData)
		docsIndexReader, err = docs.NewIndexReader(data.DocsIdxData)
		if err != nil {
			return nil, fmt.Errorf("unable to load documents index: %v", err)
		}

		// NB(jeromefroe): Currently we assume the postings IDs are contiguous.
		startInclusive = docsIndexReader.Base()
		endExclusive = startInclusive + postings.ID(docsIndexReader.Len())
	}

	return &fsSegment{
		fieldsFST:       fieldsFST,
		docsDataReader:  docsDataReader,
		docsIndexReader: docsIndexReader,
		docsSliceReader: docsSliceReader,

		data:           data,
		opts:           opts,
		numDocs:        metadata.NumDocs,
		startInclusive: startInclusive,
		endExclusive:   endExclusive,
	}, nil
}

type fsSegment struct {
	sync.RWMutex
	closed          bool
	fieldsFST       *vellum.FST
	docsDataReader  *docs.DataReader
	docsIndexReader *docs.IndexReader
	docsSliceReader *docs.SliceReader
	data            SegmentData
	opts            Options

	numDocs        int64
	startInclusive postings.ID
	endExclusive   postings.ID
}

func (r *fsSegment) Size() int64 {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return 0
	}
	return r.numDocs
}

func (r *fsSegment) ContainsID(docID []byte) (bool, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return false, errReaderClosed
	}

	termsFST, exists, err := r.retrieveTermsFSTWithRLock(doc.IDReservedFieldName)
	if err != nil {
		return false, err
	}

	if !exists {
		return false, fmt.Errorf("internal error while retrieving id FST: %v", err)
	}

	_, exists, err = termsFST.Get(docID)
	closeErr := termsFST.Close()
	if err != nil {
		return false, err
	}

	return exists, closeErr
}

func (r *fsSegment) ContainsField(field []byte) (bool, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return false, errReaderClosed
	}
	return r.fieldsFST.Contains(field)
}

func (r *fsSegment) Reader() (index.Reader, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}
	return &fsSegmentReader{
		fsSegment: r,
	}, nil
}

func (r *fsSegment) Close() error {
	r.Lock()
	defer r.Unlock()
	if r.closed {
		return errReaderClosed
	}
	r.closed = true
	var multiErr xerrors.MultiError
	multiErr = multiErr.Add(r.fieldsFST.Close())
	if r.data.Closer != nil {
		multiErr = multiErr.Add(r.data.Closer.Close())
	}
	return multiErr.FinalError()
}

func (r *fsSegment) FieldsIterable() sgmt.FieldsIterable {
	return r
}

func (r *fsSegment) Fields() (sgmt.FieldsIterator, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	iter := newFSTTermsIter()
	iter.reset(fstTermsIterOpts{
		fst:         r.fieldsFST,
		finalizeFST: false,
	})
	return iter, nil
}

func (r *fsSegment) TermsIterable() sgmt.TermsIterable {
	return &termsIterable{
		r:            r,
		fieldsIter:   newFSTTermsIter(),
		postingsIter: newFSTTermsPostingsIter(),
	}
}

// termsIterable allows multiple term lookups to share the same roaring
// bitmap being unpacked for use when iterating over an entire segment
type termsIterable struct {
	r            *fsSegment
	fieldsIter   *fstTermsIter
	postingsIter *fstTermsPostingsIter
}

func (i *termsIterable) Terms(field []byte) (sgmt.TermsIterator, error) {
	i.r.RLock()
	defer i.r.RUnlock()
	if i.r.closed {
		return nil, errReaderClosed
	}

	termsFST, exists, err := i.r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}

	if !exists {
		return sgmt.EmptyTermsIterator, nil
	}

	i.fieldsIter.reset(fstTermsIterOpts{
		fst:         termsFST,
		finalizeFST: true,
	})
	i.postingsIter.reset(i.r, i.fieldsIter)
	return i.postingsIter, nil
}

func (r *fsSegment) UnmarshalPostingsListBitmap(b *pilosaroaring.Bitmap, offset uint64) error {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return errReaderClosed
	}

	postingsBytes, err := r.retrieveBytesWithRLock(r.data.PostingsData, offset)
	if err != nil {
		return fmt.Errorf("unable to retrieve postings data: %v", err)
	}

	b.Reset()
	return b.UnmarshalBinary(postingsBytes)
}

func (r *fsSegment) MatchField(field []byte) (postings.List, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}
	if !r.data.Version.supportsFieldPostingsList() {
		// i.e. don't have the field level postings list, so fall back to regexp
		return r.matchRegexpWithRLock(field, index.DotStarCompiledRegex())
	}

	termsFSTOffset, exists, err := r.fieldsFST.Get(field)
	if err != nil {
		return nil, err
	}
	if !exists {
		// i.e. we don't know anything about the term, so can early return an empty postings list
		return r.opts.PostingsListPool().Get(), nil
	}

	protoBytes, _, err := r.retrieveTermsBytesWithRLock(r.data.FSTTermsData, termsFSTOffset)
	if err != nil {
		return nil, err
	}

	var fieldData fswriter.FieldData
	if err := fieldData.Unmarshal(protoBytes); err != nil {
		return nil, err
	}

	postingsOffset := fieldData.FieldPostingsListOffset
	return r.retrievePostingsListWithRLock(postingsOffset)
}

func (r *fsSegment) MatchTerm(field []byte, term []byte) (postings.List, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	termsFST, exists, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}

	if !exists {
		// i.e. we don't know anything about the field, so can early return an empty postings list
		return r.opts.PostingsListPool().Get(), nil
	}

	fstCloser := x.NewSafeCloser(termsFST)
	defer fstCloser.Close()

	postingsOffset, exists, err := termsFST.Get(term)
	if err != nil {
		return nil, err
	}

	if !exists {
		// i.e. we don't know anything about the term, so can early return an empty postings list
		return r.opts.PostingsListPool().Get(), nil
	}

	pl, err := r.retrievePostingsListWithRLock(postingsOffset)
	if err != nil {
		return nil, err
	}

	if err := fstCloser.Close(); err != nil {
		return nil, err
	}

	return pl, nil
}

func (r *fsSegment) MatchRegexp(field []byte, compiled index.CompiledRegex) (postings.List, error) {
	r.RLock()
	pl, err := r.matchRegexpWithRLock(field, compiled)
	r.RUnlock()
	return pl, err
}

func (r *fsSegment) matchRegexpWithRLock(field []byte, compiled index.CompiledRegex) (postings.List, error) {
	if r.closed {
		return nil, errReaderClosed
	}

	re := compiled.FST
	if re == nil {
		return nil, errReaderNilRegexp
	}

	termsFST, exists, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}

	if !exists {
		// i.e. we don't know anything about the field, so can early return an empty postings list
		return r.opts.PostingsListPool().Get(), nil
	}

	var (
		fstCloser     = x.NewSafeCloser(termsFST)
		iter, iterErr = termsFST.Search(re, compiled.PrefixBegin, compiled.PrefixEnd)
		iterCloser    = x.NewSafeCloser(iter)
		// NB(prateek): way quicker to union the PLs together at the end, rathen than one at a time.
		pls []postings.List // TODO: pool this slice allocation
	)
	defer func() {
		iterCloser.Close()
		fstCloser.Close()
	}()

	for {
		if iterErr == vellum.ErrIteratorDone {
			break
		}

		if iterErr != nil {
			return nil, iterErr
		}

		_, postingsOffset := iter.Current()
		nextPl, err := r.retrievePostingsListWithRLock(postingsOffset)
		if err != nil {
			return nil, err
		}
		pls = append(pls, nextPl)
		iterErr = iter.Next()
	}

	pl, err := roaring.Union(pls)
	if err != nil {
		return nil, err
	}

	if err := iterCloser.Close(); err != nil {
		return nil, err
	}

	if err := fstCloser.Close(); err != nil {
		return nil, err
	}

	return pl, nil
}

func (r *fsSegment) MatchAll() (postings.MutableList, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	pl := r.opts.PostingsListPool().Get()
	err := pl.AddRange(r.startInclusive, r.endExclusive)
	if err != nil {
		return nil, err
	}

	return pl, nil
}

func (r *fsSegment) Doc(id postings.ID) (doc.Document, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return doc.Document{}, errReaderClosed
	}

	// If using docs slice reader, return from the in memory slice reader
	if r.docsSliceReader != nil {
		return r.docsSliceReader.Read(id)
	}

	offset, err := r.docsIndexReader.Read(id)
	if err != nil {
		return doc.Document{}, err
	}

	return r.docsDataReader.Read(offset)
}

func (r *fsSegment) Docs(pl postings.List) (doc.Iterator, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	return index.NewIDDocIterator(r, pl.Iterator()), nil
}

func (r *fsSegment) AllDocs() (index.IDDocIterator, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}
	pi := postings.NewRangeIterator(r.startInclusive, r.endExclusive)
	return index.NewIDDocIterator(r, pi), nil
}

func (r *fsSegment) retrievePostingsListWithRLock(postingsOffset uint64) (postings.List, error) {
	postingsBytes, err := r.retrieveBytesWithRLock(r.data.PostingsData, postingsOffset)
	if err != nil {
		return nil, fmt.Errorf("unable to retrieve postings data: %v", err)
	}

	return pilosa.Unmarshal(postingsBytes)
}

func (r *fsSegment) retrieveTermsFSTWithRLock(field []byte) (*vellum.FST, bool, error) {
	termsFSTOffset, exists, err := r.fieldsFST.Get(field)
	if err != nil {
		return nil, false, err
	}

	if !exists {
		return nil, false, nil
	}

	termsFSTBytes, err := r.retrieveBytesWithRLock(r.data.FSTTermsData, termsFSTOffset)
	if err != nil {
		return nil, false, fmt.Errorf("error while decoding terms fst: %v", err)
	}

	termsFST, err := vellum.Load(termsFSTBytes)
	if err != nil {
		return nil, false, fmt.Errorf("error while loading terms fst: %v", err)
	}

	return termsFST, true, nil
}

// retrieveTermsBytesWithRLock assumes the base []byte slice is a collection of
// (protobuf payload, proto payload size, fst payload, fst payload size, magicNumber) tuples;
// where all sizes/magicNumber are strictly uint64 (i.e. 8 bytes). It assumes the 8 bytes
// preceding the offset are the magicNumber, the 8 bytes before that are the fst payload size,
// and the `size` bytes before that are the payload, 8 bytes preceeding that are
// `proto payload size`, and the `proto payload size` bytes before that are the proto payload.
// It retrieves the payload while doing bounds checks to ensure no segfaults.
func (r *fsSegment) retrieveTermsBytesWithRLock(base []byte, offset uint64) (proto []byte, fst []byte, err error) {
	const sizeofUint64 = 8
	var (
		magicNumberEnd   = int64(offset) // to prevent underflows
		magicNumberStart = offset - sizeofUint64
	)
	if magicNumberEnd > int64(len(base)) || magicNumberStart < 0 {
		return nil, nil, fmt.Errorf("base bytes too small, length: %d, base-offset: %d", len(base), magicNumberEnd)
	}
	magicNumberBytes := base[magicNumberStart:magicNumberEnd]
	d := encoding.NewDecoder(magicNumberBytes)
	n, err := d.Uint64()
	if err != nil {
		return nil, nil, fmt.Errorf("error while decoding magicNumber: %v", err)
	}
	if n != uint64(magicNumber) {
		return nil, nil, fmt.Errorf("mismatch while decoding magicNumber: %d", n)
	}

	var (
		sizeEnd   = magicNumberStart
		sizeStart = sizeEnd - sizeofUint64
	)
	if sizeStart < 0 {
		return nil, nil, fmt.Errorf("base bytes too small, length: %d, size-offset: %d", len(base), sizeStart)
	}
	sizeBytes := base[sizeStart:sizeEnd]
	d.Reset(sizeBytes)
	size, err := d.Uint64()
	if err != nil {
		return nil, nil, fmt.Errorf("error while decoding size: %v", err)
	}

	var (
		payloadEnd   = sizeStart
		payloadStart = payloadEnd - size
	)
	if payloadStart < 0 {
		return nil, nil, fmt.Errorf("base bytes too small, length: %d, payload-start: %d, payload-size: %d",
			len(base), payloadStart, size)
	}

	var (
		fstBytes       = base[payloadStart:payloadEnd]
		protoSizeEnd   = payloadStart
		protoSizeStart = protoSizeEnd - sizeofUint64
	)
	if protoSizeStart < 0 {
		return nil, nil, fmt.Errorf("base bytes too small, length: %d, proto-size-offset: %d", len(base), protoSizeStart)
	}

	protoSizeBytes := base[protoSizeStart:protoSizeEnd]
	d.Reset(protoSizeBytes)
	protoSize, err := d.Uint64()
	if err != nil {
		return nil, nil, fmt.Errorf("error while decoding size: proto %v", err)
	}

	var (
		protoEnd   = protoSizeStart
		protoStart = protoEnd - protoSize
	)
	if protoStart < 0 {
		return nil, nil, fmt.Errorf("base bytes too small, length: %d, proto-start: %d", len(base), protoStart)
	}
	protoBytes := base[protoStart:protoEnd]

	return protoBytes, fstBytes, nil
}

// retrieveBytesWithRLock assumes the base []byte slice is a collection of (payload, size, magicNumber) triples,
// where size/magicNumber are strictly uint64 (i.e. 8 bytes). It assumes the 8 bytes preceding the offset
// are the magicNumber, the 8 bytes before that are the size, and the `size` bytes before that are the
// payload. It retrieves the payload while doing bounds checks to ensure no segfaults.
func (r *fsSegment) retrieveBytesWithRLock(base []byte, offset uint64) ([]byte, error) {
	const sizeofUint64 = 8
	var (
		magicNumberEnd   = int64(offset) // to prevent underflows
		magicNumberStart = offset - sizeofUint64
	)
	if magicNumberEnd > int64(len(base)) || magicNumberStart < 0 {
		return nil, fmt.Errorf("base bytes too small, length: %d, base-offset: %d", len(base), magicNumberEnd)
	}
	magicNumberBytes := base[magicNumberStart:magicNumberEnd]
	d := encoding.NewDecoder(magicNumberBytes)
	n, err := d.Uint64()
	if err != nil {
		return nil, fmt.Errorf("error while decoding magicNumber: %v", err)
	}
	if n != uint64(magicNumber) {
		return nil, fmt.Errorf("mismatch while decoding magicNumber: %d", n)
	}

	var (
		sizeEnd   = magicNumberStart
		sizeStart = sizeEnd - sizeofUint64
	)
	if sizeStart < 0 {
		return nil, fmt.Errorf("base bytes too small, length: %d, size-offset: %d", len(base), sizeStart)
	}
	sizeBytes := base[sizeStart:sizeEnd]
	d.Reset(sizeBytes)
	size, err := d.Uint64()
	if err != nil {
		return nil, fmt.Errorf("error while decoding size: %v", err)
	}

	var (
		payloadEnd   = sizeStart
		payloadStart = payloadEnd - size
	)
	if payloadStart < 0 {
		return nil, fmt.Errorf("base bytes too small, length: %d, payload-start: %d, payload-size: %d",
			len(base), payloadStart, size)
	}

	return base[payloadStart:payloadEnd], nil
}

type fsSegmentReader struct {
	sync.RWMutex
	closed bool

	fsSegment *fsSegment
}

var _ index.Reader = &fsSegmentReader{}

func (sr *fsSegmentReader) MatchField(field []byte) (postings.List, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	pl, err := sr.fsSegment.MatchField(field)
	sr.RUnlock()
	return pl, err
}

func (sr *fsSegmentReader) MatchTerm(field []byte, term []byte) (postings.List, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	pl, err := sr.fsSegment.MatchTerm(field, term)
	sr.RUnlock()
	return pl, err
}

func (sr *fsSegmentReader) MatchRegexp(field []byte, compiled index.CompiledRegex) (postings.List, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	pl, err := sr.fsSegment.MatchRegexp(field, compiled)
	sr.RUnlock()
	return pl, err
}

func (sr *fsSegmentReader) MatchAll() (postings.MutableList, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	pl, err := sr.fsSegment.MatchAll()
	sr.RUnlock()
	return pl, err
}

func (sr *fsSegmentReader) Doc(id postings.ID) (doc.Document, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return doc.Document{}, errReaderClosed
	}
	pl, err := sr.fsSegment.Doc(id)
	sr.RUnlock()
	return pl, err
}

func (sr *fsSegmentReader) Docs(pl postings.List) (doc.Iterator, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	iter, err := sr.fsSegment.Docs(pl)
	sr.RUnlock()
	return iter, err
}

func (sr *fsSegmentReader) AllDocs() (index.IDDocIterator, error) {
	sr.RLock()
	if sr.closed {
		sr.RUnlock()
		return nil, errReaderClosed
	}
	iter, err := sr.fsSegment.AllDocs()
	sr.RUnlock()
	return iter, err
}

func (sr *fsSegmentReader) Close() error {
	sr.Lock()
	if sr.closed {
		sr.Unlock()
		return errReaderClosed
	}
	sr.closed = true
	sr.Unlock()
	return nil
}
