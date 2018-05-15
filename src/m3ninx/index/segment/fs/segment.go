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
	"errors"
	"fmt"
	"regexp"
	"sync"
	"unicode/utf8"

	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/generated/proto/fswriter"
	"github.com/m3db/m3ninx/index"
	"github.com/m3db/m3ninx/index/segment/fs/encoding"
	"github.com/m3db/m3ninx/index/segment/fs/encoding/docs"
	"github.com/m3db/m3ninx/postings"
	"github.com/m3db/m3ninx/postings/pilosa"
	"github.com/m3db/m3ninx/postings/roaring"
	"github.com/m3db/m3ninx/x"

	"github.com/couchbase/vellum"
	vregex "github.com/couchbaselabs/vellum/regexp"
)

var (
	errReaderClosed            = errors.New("segment is closed")
	errUnsupportedMajorVersion = errors.New("unsupported major version")
	errDocumentsDataUnset      = errors.New("documents data bytes are not set")
	errDocumentsIdxUnset       = errors.New("documents index bytes are not set")
	errPostingsDataUnset       = errors.New("postings data bytes are not set")
	errFSTTermsDataUnset       = errors.New("fst terms data bytes are not set")
	errFSTFieldsDataUnset      = errors.New("fst fields data bytes are not set")

	minByteKey = []byte{}
	maxByteKey = []byte(string(utf8.MaxRune))
)

// SegmentData represent the collection of required parameters to construct a Segment.
type SegmentData struct {
	MajorVersion  int
	MinorVersion  int
	Metadata      []byte
	DocsData      []byte
	DocsIdxData   []byte
	PostingsData  []byte
	FSTTermsData  []byte
	FSTFieldsData []byte
}

// Validate validates the provided segment data, returning an error if it's not.
func (sd SegmentData) Validate() error {
	if sd.MajorVersion != MajorVersion {
		return errUnsupportedMajorVersion
	}

	if sd.DocsData == nil {
		return errDocumentsDataUnset
	}

	if sd.DocsIdxData == nil {
		return errDocumentsIdxUnset
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

	return nil
}

// NewSegmentOpts represent the collection of knobs used by the Segment.
type NewSegmentOpts struct {
	PostingsListPool postings.Pool
}

// NewSegment returns a new Segment backed by the provided options.
func NewSegment(data SegmentData, opts NewSegmentOpts) (Segment, error) {
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

	docsIndexReader, err := docs.NewIndexReader(data.DocsIdxData)
	if err != nil {
		return nil, fmt.Errorf("unable to load documents index: %v", err)
	}

	// NB(jeromefroe): Currently we assume the postings IDs are contiguous.
	startInclusive := docsIndexReader.Base()
	endExclusive := startInclusive + postings.ID(docsIndexReader.Len())

	docsDataReader := docs.NewDataReader(data.DocsData)

	return &fsSegment{
		fieldsFST:       fieldsFST,
		docsDataReader:  docsDataReader,
		docsIndexReader: docsIndexReader,

		data:           data,
		opts:           opts,
		numDocs:        metadata.NumDocs,
		startInclusive: startInclusive,
		endExclusive:   endExclusive,
	}, nil
}

type fsSegment struct {
	sync.RWMutex
	closed bool

	fieldsFST       *vellum.FST
	docsDataReader  *docs.DataReader
	docsIndexReader *docs.IndexReader

	data SegmentData
	opts NewSegmentOpts

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

	termsFST, err := r.retrieveTermsFSTWithRLock(doc.IDReservedFieldName)
	if err != nil {
		return false, err
	}
	fstCloser := x.NewSafeCloser(termsFST)
	defer fstCloser.Close()

	_, exists, err := termsFST.Get(docID)
	if err != nil {
		return false, err
	}

	return exists, fstCloser.Close()
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
	return r.fieldsFST.Close()
}

// FOLLOWUP(prateek): really need to change the types returned in Fields() and Terms()
// to be iterators to allow us to pool the bytes being returned to the user. Otherwise
// we're forced to copy these massive slices every time. Tracking this under
// https://github.com/m3db/m3ninx/issues/66
func (r *fsSegment) Fields() ([][]byte, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	return r.allKeys(r.fieldsFST)
}

func (r *fsSegment) Terms(field []byte) ([][]byte, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	termsFST, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}

	fstCloser := x.NewSafeCloser(termsFST)
	defer fstCloser.Close()

	terms, err := r.allKeys(termsFST)
	if err != nil {
		return nil, err
	}

	if err := fstCloser.Close(); err != nil {
		return nil, err
	}

	return terms, nil
}

func (r *fsSegment) MatchTerm(field []byte, term []byte) (postings.List, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	termsFST, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}
	fstCloser := x.NewSafeCloser(termsFST)
	defer fstCloser.Close()

	postingsOffset, exists, err := termsFST.Get(term)
	if err != nil {
		return nil, err
	}

	if !exists {
		// i.e. we don't know anything about the term, so can early return an empty postings list
		return r.opts.PostingsListPool.Get(), nil
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

func (r *fsSegment) MatchRegexp(field []byte, regexp []byte, compiled *regexp.Regexp) (postings.List, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	re, err := vregex.New(string(regexp))
	if err != nil {
		return nil, err
	}

	termsFST, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}

	var (
		fstCloser     = x.NewSafeCloser(termsFST)
		pl            = r.opts.PostingsListPool.Get()
		iter, iterErr = termsFST.Search(re, minByteKey, maxByteKey)
		iterCloser    = x.NewSafeCloser(iter)
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
		if err := pl.Union(nextPl); err != nil {
			return nil, err
		}

		iterErr = iter.Next()
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

	pl := r.opts.PostingsListPool.Get()
	pl.AddRange(r.startInclusive, r.endExclusive)

	return pl, nil
}

func (r *fsSegment) Doc(id postings.ID) (doc.Document, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return doc.Document{}, errReaderClosed
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

	return pilosa.Unmarshal(postingsBytes, roaring.NewPostingsList)
}

func (r *fsSegment) allKeys(fst *vellum.FST) ([][]byte, error) {
	num := fst.Len()
	keys := make([][]byte, 0, num)

	iter, iterErr := fst.Iterator(minByteKey, maxByteKey)
	safeCloser := x.NewSafeCloser(iter)
	defer safeCloser.Close()

	for {
		if iterErr == vellum.ErrIteratorDone {
			break
		}
		if iterErr != nil {
			return nil, iterErr
		}
		key, _ := iter.Current()
		keys = append(keys, r.copyBytes(key))
		iterErr = iter.Next()
	}

	if err := safeCloser.Close(); err != nil {
		return nil, err
	}
	return keys, nil
}

func (r *fsSegment) retrieveTermsFSTWithRLock(field []byte) (*vellum.FST, error) {
	termsFSTOffset, exists, err := r.fieldsFST.Get(field)
	if err != nil {
		return nil, err
	}

	if !exists {
		return nil, fmt.Errorf("no terms known for field: %v", string(field))
	}

	termsFSTBytes, err := r.retrieveBytesWithRLock(r.data.FSTTermsData, termsFSTOffset)
	if err != nil {
		return nil, fmt.Errorf("error while decoding terms fst: %v", err)
	}

	return vellum.Load(termsFSTBytes)
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

func (sr *fsSegmentReader) MatchTerm(field []byte, term []byte) (postings.List, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return nil, errReaderClosed
	}
	return sr.fsSegment.MatchTerm(field, term)
}

func (sr *fsSegmentReader) MatchRegexp(field []byte, regexp []byte, compiled *regexp.Regexp) (postings.List, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return nil, errReaderClosed
	}
	return sr.fsSegment.MatchRegexp(field, regexp, compiled)
}

func (sr *fsSegmentReader) MatchAll() (postings.MutableList, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return nil, errReaderClosed
	}
	return sr.fsSegment.MatchAll()
}

func (sr *fsSegmentReader) Doc(id postings.ID) (doc.Document, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return doc.Document{}, errReaderClosed
	}
	return sr.fsSegment.Doc(id)
}

func (sr *fsSegmentReader) Docs(pl postings.List) (doc.Iterator, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return nil, errReaderClosed
	}
	return sr.fsSegment.Docs(pl)
}

func (sr *fsSegmentReader) AllDocs() (index.IDDocIterator, error) {
	sr.RLock()
	defer sr.RUnlock()
	if sr.closed {
		return nil, errReaderClosed
	}
	return sr.fsSegment.AllDocs()
}

func (sr *fsSegmentReader) Close() error {
	sr.Lock()
	defer sr.Unlock()
	if sr.closed {
		return errReaderClosed
	}
	sr.closed = true
	return nil
}

// copyBytes returns a copy of the provided bytes. We need to do this as the bytes
// backing the fsSegment are mmap-ed, and maintain their lifecycle exclusive from those
// owned by users.
// FOLLOWUP(prateek): return iterator types at all exit points of Reader, and
// then we can pool the bytes below.
func (r *fsSegment) copyBytes(b []byte) []byte {
	copied := make([]byte, len(b))
	copy(copied, b)
	return copied
}
