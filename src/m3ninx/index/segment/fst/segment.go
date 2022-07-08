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
	"errors"
	"fmt"
	"io"
	"math"
	"sync"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/generated/proto/fswriter"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	sgmt "github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst/encoding/docs"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"
	"github.com/m3db/m3/src/m3ninx/x"
	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/mmap"
	pilosaroaring "github.com/m3dbx/pilosa/roaring"

	"github.com/m3dbx/vellum"
)

var (
	errReaderClosed            = errors.New("segment is closed")
	errReaderFinalized         = errors.New("segment is finalized")
	errReaderNilRegexp         = errors.New("nil regexp provided")
	errUnsupportedMajorVersion = errors.New("unsupported major version")
	errDocumentsDataUnset      = errors.New("documents data bytes are not set")
	errDocumentsIdxUnset       = errors.New("documents index bytes are not set")
	errPostingsDataUnset       = errors.New("postings data bytes are not set")
	errFSTTermsDataUnset       = errors.New("fst terms data bytes are not set")
	errFSTFieldsDataUnset      = errors.New("fst fields data bytes are not set")

	graphitePathPrefix = []byte(doc.GraphitePathPrefix)
)

// SegmentData represent the collection of required parameters to construct a Segment.
type SegmentData struct {
	Version  Version
	Metadata []byte

	DocsData      mmap.Descriptor
	DocsIdxData   mmap.Descriptor
	PostingsData  mmap.Descriptor
	FSTTermsData  mmap.Descriptor
	FSTFieldsData mmap.Descriptor

	// DocsReader is an alternative to specifying
	// the docs data and docs idx data if the documents
	// already reside in memory and we want to use the
	// in memory references instead.
	DocsReader docs.Reader

	Closer io.Closer
}

// Validate validates the provided segment data, returning an error if it's not.
func (sd SegmentData) Validate() error {
	if err := sd.Version.Supported(); err != nil {
		return err
	}

	if sd.PostingsData.Bytes == nil {
		return errPostingsDataUnset
	}

	if sd.FSTTermsData.Bytes == nil {
		return errFSTTermsDataUnset
	}

	if sd.FSTFieldsData.Bytes == nil {
		return errFSTFieldsDataUnset
	}

	if sd.DocsReader == nil {
		if sd.DocsData.Bytes == nil {
			return errDocumentsDataUnset
		}

		if sd.DocsIdxData.Bytes == nil {
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

	fieldsFST, err := vellum.Load(data.FSTFieldsData.Bytes)
	if err != nil {
		return nil, fmt.Errorf("unable to load fields fst: %v", err)
	}

	var (
		docsThirdPartyReader  = data.DocsReader
		docsDataReader        *docs.DataReader
		docsEncodedDataReader *docs.EncodedDataReader
		docsIndexReader       *docs.IndexReader
	)
	if docsThirdPartyReader == nil {
		docsDataReader = docs.NewDataReader(data.DocsData.Bytes)
		docsIndexReader, err = docs.NewIndexReader(data.DocsIdxData.Bytes)
		if err != nil {
			return nil, fmt.Errorf("unable to load documents index: %v", err)
		}
	}
	docsEncodedDataReader = docs.NewEncodedDataReader(data.DocsData.Bytes)

	s := &fsSegment{
		fieldsFST:             fieldsFST,
		docsDataReader:        docsDataReader,
		docsEncodedDataReader: docsEncodedDataReader,
		docsIndexReader:       docsIndexReader,
		docsThirdPartyReader:  docsThirdPartyReader,

		data: data,
		opts: opts,

		termFSTs: vellumFSTs{fstMap: newFSTMap(fstMapOptions{})},

		numDocs: metadata.NumDocs,
	}

	// Preload all the term FSTs so that there's no locking
	// required (which was causing lock contention with queries requiring
	// access to the terms FST for a field that hasn't been accessed before
	// and loading on demand).
	iter := newFSTTermsIter(newFSTTermsIterOptions{})
	iter.reset(fstTermsIterOpts{
		seg:         s,
		fst:         fieldsFST,
		finalizeFST: false,
	})

	iterCloser := x.NewSafeCloser(iter)
	defer func() { _ = iterCloser.Close() }()

	for iter.Next() {
		field := iter.Current()
		termsFSTOffset := iter.CurrentOffset()
		if bytes.HasPrefix(field, graphitePathPrefix) {
			// NB(rob): Too expensive to pre-load all the graphite path indexed
			// path terms, load these on demand for find queries which are
			// small in volume comparative to queries.
			continue
		}

		termsFSTBytes, err := s.retrieveBytesWithRLock(s.data.FSTTermsData.Bytes, termsFSTOffset)
		if err != nil {
			return nil, fmt.Errorf(
				"error while decoding terms fst: field=%s, err=%v", field, err)
		}

		termsFST, err := vellum.Load(termsFSTBytes)
		if err != nil {
			return nil, fmt.Errorf(
				"error while loading terms fst: field=%s, err=%v", field, err)
		}

		// Save FST to FST map.
		vellumFST := newVellumFST(termsFST)
		s.termFSTs.fstMap.Set(field, vellumFST)
	}
	if err := iterCloser.Close(); err != nil {
		return nil, err
	}

	// NB(r): The segment uses the context finalization to finalize
	// resources. Finalize is called after Close is called and all
	// the segment readers have also been closed.
	s.ctx = opts.ContextPool().Get()
	s.ctx.RegisterFinalizer(s)

	return s, nil
}

// Ensure FST segment implements ImmutableSegment so can be casted upwards
// and mmap's can be freed.
var _ segment.ImmutableSegment = (*fsSegment)(nil)

type fsSegment struct {
	sync.RWMutex
	ctx                   context.Context
	closed                bool
	finalized             bool
	fieldsFST             *vellum.FST
	docsDataReader        *docs.DataReader
	docsEncodedDataReader *docs.EncodedDataReader
	docsIndexReader       *docs.IndexReader
	docsThirdPartyReader  docs.Reader
	data                  SegmentData
	opts                  Options

	termFSTs vellumFSTs
	numDocs  int64
}

type vellumFSTs struct {
	fstMap     *fstMap
	readerPool *fstReaderPool
}

type vellumFST struct {
	fst        *vellum.FST
	readerPool *fstReaderPool
}

func newVellumFST(fst *vellum.FST) vellumFST {
	return vellumFST{
		fst:        fst,
		readerPool: newFSTReaderPool(fst),
	}
}

func (f vellumFST) Get(key []byte) (uint64, bool, error) {
	reader, err := f.readerPool.Get()
	if err != nil {
		return 0, false, err
	}
	result, exists, err := reader.Get(key)
	// Always return reader to pool.
	f.readerPool.Put(reader)
	return result, exists, err
}

type fstReaderPool struct {
	pool sync.Pool
}

func newFSTReaderPool(fst *vellum.FST) *fstReaderPool {
	return &fstReaderPool{
		pool: sync.Pool{
			New: func() interface{} {
				r, _ := fst.Reader()
				return r
			},
		},
	}
}

func (p *fstReaderPool) Get() (*vellum.Reader, error) {
	v := p.pool.Get().(*vellum.Reader)
	if v == nil {
		return nil, fmt.Errorf("vellum reader failed to initialize")
	}
	return v, nil
}

func (p *fstReaderPool) Put(v *vellum.Reader) {
	p.pool.Put(v)
}

func (r *fsSegment) SegmentData(ctx context.Context) (SegmentData, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return SegmentData{}, errReaderClosed
	}

	// NB(r): Ensure that we do not release, mmaps, etc
	// until all readers have been closed.
	r.ctx.DependsOn(ctx)
	return r.data, nil
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
		return false, fmt.Errorf(
			"internal error while retrieving id FST: %s",
			doc.IDReservedFieldName)
	}

	_, exists, err = termsFST.Get(docID)
	return exists, err
}

func (r *fsSegment) ContainsField(field []byte) (bool, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return false, errReaderClosed
	}
	return r.fieldsFST.Contains(field)
}

func (r *fsSegment) Reader() (sgmt.Reader, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errReaderClosed
	}

	reader := newReader(r, r.opts)

	// NB(rob): Ensure that we do not release, mmaps, etc
	// until all readers have been closed.
	r.ctx.DependsOn(reader.ctx)

	return reader, nil
}

func (r *fsSegment) Close() error {
	r.Lock()
	if r.closed {
		r.Unlock()
		return errReaderClosed
	}
	r.closed = true
	r.Unlock()
	// NB(r): Inform context we are done, once all segment readers are
	// closed the segment Finalize will be called async.
	r.ctx.Close()
	return nil
}

func (r *fsSegment) Finalize() {
	r.Lock()
	defer r.Unlock()

	if r.finalized {
		return
	}

	r.finalized = true

	r.fieldsFST.Close()
	if r.data.Closer != nil {
		r.data.Closer.Close()
	}

	for _, elem := range r.termFSTs.fstMap.Iter() {
		vellumFST := elem.Value()
		vellumFST.fst.Close()
	}

	r.fieldsFST.Close()

	if r.data.Closer != nil {
		r.data.Closer.Close()
	}
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

	// Make sure we do not finalize until this iterator is completed.
	ctx := r.opts.ContextPool().Get()
	r.ctx.DependsOn(ctx)

	iter := newFSTTermsIter(newFSTTermsIterOptions{
		closeContextOnClose: ctx,
	})
	iter.reset(fstTermsIterOpts{
		seg:         r,
		fst:         r.fieldsFST,
		finalizeFST: false,
	})
	return iter, nil
}

func (r *fsSegment) TermsIterable() sgmt.TermsIterable {
	return newTermsIterable(r)
}

func (r *fsSegment) FreeMmap() error {
	multiErr := xerrors.NewMultiError()

	// NB(bodu): PostingsData, FSTTermsData and FSTFieldsData always present.
	if err := mmap.MadviseDontNeed(r.data.PostingsData); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := mmap.MadviseDontNeed(r.data.FSTTermsData); err != nil {
		multiErr = multiErr.Add(err)
	}
	if err := mmap.MadviseDontNeed(r.data.FSTFieldsData); err != nil {
		multiErr = multiErr.Add(err)
	}

	// DocsData and DocsIdxData are not always present.
	if r.data.DocsData.Bytes != nil {
		if err := mmap.MadviseDontNeed(r.data.DocsData); err != nil {
			multiErr = multiErr.Add(err)
		}
	}
	if r.data.DocsIdxData.Bytes != nil {
		if err := mmap.MadviseDontNeed(r.data.DocsIdxData); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}

// termsIterable allows multiple term lookups to share the same roaring
// bitmap being unpacked for use when iterating over an entire segment
// for the terms postings iterator.
type termsIterable struct {
	seg          *fsSegment
	fieldsIter   *fstTermsIter
	postingsIter *fstTermsPostingsIter
}

func newTermsIterable(seg *fsSegment) *termsIterable {
	return &termsIterable{
		seg:          seg,
		fieldsIter:   newFSTTermsIter(newFSTTermsIterOptions{}),
		postingsIter: newFSTTermsPostingsIter(),
	}
}

func (i *termsIterable) acquireRLockCheckValidAndExtendLifetimeWithContext() (*fstTermsIter, error) {
	i.seg.RLock()
	defer i.seg.RUnlock()
	if i.seg.closed {
		return nil, errReaderClosed
	}

	// This code path is used by compaction to iterate through all terms in the segment.
	// We ensure it creates a reader that is not closed until compaction is completed
	// and hence already extends the lifetime, so to avoid overhead of all the
	// locking of creating new dependent contexts each time during background compaction
	// we disable extending the lifetime of the segment for this code path.
	return i.fieldsIter, nil
}

func (i *termsIterable) Terms(field []byte) (sgmt.TermsIterator, error) {
	fieldsIter, err := i.acquireRLockCheckValidAndExtendLifetimeWithContext()
	if err != nil {
		return nil, err
	}

	return i.termsWithTermsIterAndContextLifetime(fieldsIter, field)
}

func (i *termsIterable) TermsWithRegex(
	field []byte,
	compiled *index.CompiledRegex,
) (sgmt.TermsIterator, error) {
	fieldsIter, err := i.acquireRLockCheckValidAndExtendLifetimeWithContext()
	if err != nil {
		return nil, err
	}

	return i.termsWithRegexWithTermsIterAndContextLifetime(fieldsIter, field, compiled)
}

func (i *termsIterable) termsWithTermsIterAndContextLifetime(
	fieldsIter *fstTermsIter,
	field []byte,
) (sgmt.TermsIterator, error) {
	var iterWithContextLifetimeUsed bool
	defer func() {
		if iterWithContextLifetimeUsed {
			return
		}
		// NB(rob): Important to close since closing will release the context lifetime
		// that is acquired.
		fieldsIter.Close()
	}()

	termsFST, exists, err := i.seg.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}
	if !exists {
		return sgmt.EmptyTermsIterator, nil
	}

	iterWithContextLifetimeUsed = true
	fieldsIter.reset(fstTermsIterOpts{
		seg:         i.seg,
		fst:         termsFST.fst,
		finalizeFST: false,
	})
	i.postingsIter.reset(i.seg, fieldsIter, false)
	return i.postingsIter, nil
}

func (i *termsIterable) termsWithRegexWithTermsIterAndContextLifetime(
	fieldsIter *fstTermsIter,
	field []byte,
	compiled *index.CompiledRegex,
) (sgmt.TermsIterator, error) {
	var iterWithContextLifetimeUsed bool
	defer func() {
		if iterWithContextLifetimeUsed {
			return
		}
		// NB(rob): Important to close since closing will release the context lifetime
		// that is acquired.
		fieldsIter.Close()
	}()

	termsFST, exists, err := i.seg.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}
	if !exists {
		return sgmt.EmptyTermsIterator, nil
	}

	iterWithContextLifetimeUsed = true
	fieldsIter.reset(fstTermsIterOpts{
		seg:         i.seg,
		fst:         termsFST.fst,
		fstSearch:   compiled,
		finalizeFST: false,
	})
	i.postingsIter.reset(i.seg, fieldsIter, false)
	return i.postingsIter, nil
}

func (i *termsIterable) FieldsPostingsList() (sgmt.FieldsPostingsListIterator, error) {
	fieldsIter, err := i.acquireRLockCheckValidAndExtendLifetimeWithContext()
	if err != nil {
		return nil, err
	}

	return i.fieldsWithTermsIterAndContextLifetime(fieldsIter), nil
}

func (i *termsIterable) FieldsPostingsListWithRegex(
	compiled *index.CompiledRegex,
) (sgmt.FieldsPostingsListIterator, error) {
	fieldsIter, err := i.acquireRLockCheckValidAndExtendLifetimeWithContext()
	if err != nil {
		return nil, err
	}

	return i.fieldsWithRegexWithTermsIterAndContextLifetime(fieldsIter, compiled)
}

func (i *termsIterable) fieldsWithTermsIterAndContextLifetime(
	fieldsIter *fstTermsIter,
) sgmt.FieldsPostingsListIterator {
	fieldsIter.reset(fstTermsIterOpts{
		seg:         i.seg,
		fst:         i.seg.fieldsFST,
		finalizeFST: false,
		fieldsFST:   true,
	})
	i.postingsIter.reset(i.seg, fieldsIter, true)
	return i.postingsIter
}

func (i *termsIterable) fieldsWithRegexWithTermsIterAndContextLifetime(
	fieldsIter *fstTermsIter,
	compiled *index.CompiledRegex,
) (sgmt.TermsIterator, error) {
	var iterWithContextLifetimeUsed bool
	defer func() {
		if iterWithContextLifetimeUsed {
			return
		}
		// NB(rob): Important to close since closing will release the context lifetime
		// that is acquired.
		fieldsIter.Close()
	}()

	if compiled == nil {
		return nil, errReaderNilRegexp
	}

	iterWithContextLifetimeUsed = true
	fieldsIter.reset(fstTermsIterOpts{
		seg:         i.seg,
		fst:         i.seg.fieldsFST,
		fstSearch:   compiled,
		finalizeFST: false,
		fieldsFST:   true,
	})
	i.postingsIter.reset(i.seg, fieldsIter, true)
	return i.postingsIter, nil
}

func (r *fsSegment) unmarshalReadOnlyBitmapNotClosedMaybeFinalizedWithLock(
	b *roaring.ReadOnlyBitmap,
	offset uint64,
	fieldsOffset bool,
) error {
	if r.finalized {
		return errReaderFinalized
	}

	var postingsBytes []byte
	if fieldsOffset {
		protoBytes, _, err := r.retrieveTermsBytesWithRLock(r.data.FSTTermsData.Bytes, offset)
		if err != nil {
			return err
		}

		var fieldData fswriter.FieldData
		if err := fieldData.Unmarshal(protoBytes); err != nil {
			return err
		}

		postingsOffset := fieldData.FieldPostingsListOffset
		if postingsOffset == math.MaxUint64 {
			// Reserve for empty terms.
			return b.Reset(nil)
		}

		postingsBytes, err = r.retrieveBytesWithRLock(r.data.PostingsData.Bytes, postingsOffset)
		if err != nil {
			return fmt.Errorf("unable to retrieve postings data: %v", err)
		}
	} else {
		if offset == math.MaxUint64 {
			// Reserve for empty terms.
			return b.Reset(nil)
		}

		var err error
		postingsBytes, err = r.retrieveBytesWithRLock(r.data.PostingsData.Bytes, offset)
		if err != nil {
			return fmt.Errorf("unable to retrieve postings data: %v", err)
		}
	}

	return b.Reset(postingsBytes)
}

func (r *fsSegment) unmarshalBitmapNotClosedMaybeFinalizedWithLock(
	b *pilosaroaring.Bitmap,
	offset uint64,
	fieldsOffset bool,
) error {
	if r.finalized {
		return errReaderFinalized
	}

	var postingsBytes []byte
	if fieldsOffset {
		protoBytes, _, err := r.retrieveTermsBytesWithRLock(r.data.FSTTermsData.Bytes, offset)
		if err != nil {
			return err
		}

		var fieldData fswriter.FieldData
		if err := fieldData.Unmarshal(protoBytes); err != nil {
			return err
		}

		postingsOffset := fieldData.FieldPostingsListOffset
		if postingsOffset == math.MaxUint64 {
			// Reserve for empty terms.
			b.Reset()
			return nil
		}

		postingsBytes, err = r.retrieveBytesWithRLock(r.data.PostingsData.Bytes, postingsOffset)
		if err != nil {
			return fmt.Errorf("unable to retrieve postings data: %v", err)
		}
	} else {
		if offset == math.MaxUint64 {
			// Reserve for empty terms.
			b.Reset()
			return nil
		}

		var err error
		postingsBytes, err = r.retrieveBytesWithRLock(r.data.PostingsData.Bytes, offset)
		if err != nil {
			return fmt.Errorf("unable to retrieve postings data: %v", err)
		}
	}

	return b.UnmarshalBinary(postingsBytes)
}

func (r *fsSegment) matchFieldNotClosedMaybeFinalizedWithRLock(
	field []byte,
) (postings.List, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	if !r.data.Version.supportsFieldPostingsList() {
		// i.e. don't have the field level postings list, so fall back to regexp
		return r.matchRegexpNotClosedMaybeFinalizedWithRLock(field, index.DotStarCompiledRegex())
	}

	termsFSTOffset, exists, err := r.fieldsFST.Get(field)
	if err != nil {
		return nil, err
	}
	if !exists {
		// i.e. we don't know anything about the term, so can early return an empty postings list
		if index.MigrationReadOnlyPostings() {
			// NB(r): Important this is a read only bitmap since we perform
			// operations on postings lists and expect them all to be read only
			// postings lists.
			return roaring.NewReadOnlyBitmap(nil)
		}
		return r.opts.PostingsListPool().Get(), nil
	}

	protoBytes, _, err := r.retrieveTermsBytesWithRLock(r.data.FSTTermsData.Bytes, termsFSTOffset)
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

func (r *fsSegment) matchTermNotClosedMaybeFinalizedWithRLock(
	field, term []byte,
) (postings.List, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	termsFST, exists, err := r.retrieveTermsFSTWithRLock(field)
	if err != nil {
		return nil, err
	}
	if !exists {
		// i.e. we don't know anything about the field, so can early return an empty postings list
		if index.MigrationReadOnlyPostings() {
			// NB(r): Important this is a read only bitmap since we perform
			// operations on postings lists and expect them all to be read only
			// postings lists.
			return roaring.NewReadOnlyBitmap(nil)
		}
		return r.opts.PostingsListPool().Get(), nil
	}

	postingsOffset, exists, err := termsFST.Get(term)
	if err != nil {
		return nil, err
	}

	if !exists {
		// i.e. we don't know anything about the term, so can early return an empty postings list
		if index.MigrationReadOnlyPostings() {
			// NB(r): Important this is a read only bitmap since we perform
			// operations on postings lists and expect them all to be read only
			// postings lists.
			return roaring.NewReadOnlyBitmap(nil)
		}
		return r.opts.PostingsListPool().Get(), nil
	}

	return r.retrievePostingsListWithRLock(postingsOffset)
}

type regexpSearcher struct {
	iterCloser x.SafeCloser
	iterAlloc  vellum.FSTIterator
	iter       *vellum.FSTIterator
	pls        []postings.List
}

func newRegexpSearcher() *regexpSearcher {
	r := &regexpSearcher{
		iterCloser: x.NewSafeCloser(nil),
		pls:        make([]postings.List, 0, 16),
	}
	r.iter = &r.iterAlloc
	return r
}

func (s *regexpSearcher) Reset() {
	for i := range s.pls {
		s.pls[i] = nil
	}
	s.pls = s.pls[:0]
}

var regexpSearcherPool = sync.Pool{
	New: func() interface{} {
		return newRegexpSearcher()
	},
}

func getRegexpSearcher() *regexpSearcher {
	return regexpSearcherPool.Get().(*regexpSearcher)
}

func putRegexpSearcher(v *regexpSearcher) {
	v.Reset()
	regexpSearcherPool.Put(v)
}

func (r *fsSegment) matchRegexpNotClosedMaybeFinalizedWithRLock(
	field []byte,
	compiled index.CompiledRegex,
) (postings.List, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
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
		if index.MigrationReadOnlyPostings() {
			// NB(r): Important this is a read only bitmap since we perform
			// operations on postings lists and expect them all to be read only
			// postings lists.
			return roaring.NewReadOnlyBitmap(nil)
		}
		return r.opts.PostingsListPool().Get(), nil
	}

	searcher := getRegexpSearcher()
	iterErr := searcher.iter.Reset(termsFST.fst, compiled.PrefixBegin, compiled.PrefixEnd, re)
	searcher.iterCloser.Reset(searcher.iter)
	defer func() {
		searcher.iterCloser.Close()
		putRegexpSearcher(searcher)
	}()

	for {
		if iterErr == vellum.ErrIteratorDone {
			break
		}

		if iterErr != nil {
			return nil, iterErr
		}

		_, postingsOffset := searcher.iter.Current()
		nextPl, err := r.retrievePostingsListWithRLock(postingsOffset)
		if err != nil {
			return nil, err
		}
		searcher.pls = append(searcher.pls, nextPl)
		iterErr = searcher.iter.Next()
	}

	var pl postings.List
	if index.MigrationReadOnlyPostings() {
		// Perform a lazy fast union.
		pl, err = roaring.UnionReadOnly(searcher.pls)
	} else {
		pl, err = roaring.Union(searcher.pls)
	}
	if err != nil {
		return nil, err
	}

	if err := searcher.iterCloser.Close(); err != nil {
		return nil, err
	}

	return pl, nil
}

func (r *fsSegment) matchAllNotClosedMaybeFinalizedWithRLock() (postings.List, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	if index.MigrationReadOnlyPostings() {
		// NB(r): Important this is a read only postings since we perform
		// operations on postings lists and expect them all to be read only
		// postings lists.
		return roaring.NewReadOnlyRangePostingsList(0, uint64(r.numDocs))
	}

	pl := r.opts.PostingsListPool().Get()
	err := pl.AddRange(0, postings.ID(r.numDocs))
	if err != nil {
		return nil, err
	}

	return pl, nil
}

func (r *fsSegment) metadataNotClosedMaybeFinalizedWithRLock(id postings.ID) (doc.Metadata, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return doc.Metadata{}, errReaderFinalized
	}

	// If using docs slice reader, return from the in memory slice reader
	if r.docsThirdPartyReader != nil {
		return r.docsThirdPartyReader.Read(id)
	}

	offset, err := r.docsIndexReader.Read(id)
	if err != nil {
		return doc.Metadata{}, err
	}

	return r.docsDataReader.Read(offset)
}

func (r *fsSegment) metadataIteratorNotClosedMaybeFinalizedWithRLock(
	retriever index.MetadataRetriever,
	pl postings.List,
) (doc.MetadataIterator, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	return index.NewIDDocIterator(retriever, pl.Iterator()), nil
}

func (r *fsSegment) docNotClosedMaybeFinalizedWithRLock(id postings.ID) (doc.Document, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return doc.Document{}, errReaderFinalized
	}

	// If using docs slice reader, return from the in memory slice reader
	if r.docsThirdPartyReader != nil {
		m, err := r.docsThirdPartyReader.Read(id)
		if err != nil {
			return doc.Document{}, err
		}

		return doc.NewDocumentFromMetadata(m), nil
	}

	offset, err := r.docsIndexReader.Read(id)
	if err != nil {
		return doc.Document{}, err
	}

	e, err := r.docsEncodedDataReader.Read(offset)
	if err != nil {
		return doc.Document{}, err
	}

	return doc.NewDocumentFromEncoded(e), nil
}

func (r *fsSegment) docsNotClosedMaybeFinalizedWithRLock(
	retriever index.DocRetriever,
	pl postings.List,
) (doc.Iterator, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	return index.NewIterator(retriever, pl.Iterator()), nil
}

func (r *fsSegment) allDocsNotClosedMaybeFinalizedWithRLock(
	retriever index.MetadataRetriever,
) (index.IDDocIterator, error) {
	// NB(r): Not closed, but could be finalized (i.e. closed segment reader)
	// calling match field after this segment is finalized.
	if r.finalized {
		return nil, errReaderFinalized
	}

	pi := postings.NewRangeIterator(0, postings.ID(r.numDocs))
	return index.NewIDDocIterator(retriever, pi), nil
}

func (r *fsSegment) retrievePostingsListWithRLock(postingsOffset uint64) (postings.List, error) {
	if index.MigrationReadOnlyPostings() {
		// Read only bitmap is a very low allocation postings list.
		bitmap, err := roaring.NewReadOnlyBitmap(nil)
		if err != nil {
			return nil, err
		}
		if err := r.unmarshalReadOnlyBitmapNotClosedMaybeFinalizedWithLock(bitmap, postingsOffset, false); err != nil {
			return nil, err
		}
		return bitmap, nil
	}

	bitmap := pilosaroaring.NewBitmap()
	if err := r.unmarshalBitmapNotClosedMaybeFinalizedWithLock(bitmap, postingsOffset, false); err != nil {
		return nil, err
	}

	return roaring.NewPostingsListFromBitmap(bitmap), nil
}

func (r *fsSegment) retrieveTermsFSTWithRLock(field []byte) (vellumFST, bool, error) {
	if bytes.HasPrefix(field, graphitePathPrefix) {
		// NB(rob): Too expensive to pre-load all the graphite path indexed
		// path terms, load these on demand for find queries which are
		// small in volume comparative to queries.
		termsFSTOffset, exists, err := r.fieldsFST.Get(field)
		if err != nil {
			return vellumFST{}, false, err
		}
		if !exists {
			return vellumFST{}, false, nil
		}

		termsFSTBytes, err := r.retrieveBytesWithRLock(r.data.FSTTermsData.Bytes, termsFSTOffset)
		if err != nil {
			return vellumFST{}, false, err
		}

		termsFST, err := vellum.Load(termsFSTBytes)
		if err != nil {
			return vellumFST{}, false, err
		}

		return newVellumFST(termsFST), true, nil
	}

	// Otherwise FST map should contain the field (since pre-load all others).
	result, exists := r.termFSTs.fstMap.Get(field)
	return result, exists, nil
}

func (r *fsSegment) retrieveFieldsFSTWithRLock() (*vellum.FST, error) {
	return vellum.Load(r.data.FSTFieldsData.Bytes)
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

var _ sgmt.Reader = (*fsSegmentReader)(nil)

// fsSegmentReader is not thread safe for use and relies on the underlying
// segment for synchronization.
type fsSegmentReader struct {
	closed    bool
	ctx       context.Context
	fsSegment *fsSegment
}

func newReader(
	fsSegment *fsSegment,
	opts Options,
) *fsSegmentReader {
	return &fsSegmentReader{
		ctx:       opts.ContextPool().Get(),
		fsSegment: fsSegment,
	}
}

func (sr *fsSegmentReader) Fields() (sgmt.FieldsIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}

	// No need to specify to unlock a lock when closed since reader
	// extends the lifetime of the segment.
	iter := newFSTTermsIter(newFSTTermsIterOptions{})
	iter.reset(fstTermsIterOpts{
		seg:         sr.fsSegment,
		fst:         sr.fsSegment.fieldsFST,
		finalizeFST: false,
	})
	return iter, nil
}

func (sr *fsSegmentReader) ContainsField(field []byte) (bool, error) {
	if sr.closed {
		return false, errReaderClosed
	}
	return sr.fsSegment.fieldsFST.Contains(field)
}

func (sr *fsSegmentReader) FieldsPostingsList() (sgmt.FieldsPostingsListIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	iterable := newTermsIterable(sr.fsSegment)
	// NB(rob): The segment reader holds a finalized state reader lock the whole
	// time before closed.
	fieldsIter := newFSTTermsIter(newFSTTermsIterOptions{})
	return iterable.fieldsWithTermsIterAndContextLifetime(fieldsIter), nil
}

func (sr *fsSegmentReader) FieldsPostingsListWithRegex(
	compiled *index.CompiledRegex,
) (sgmt.FieldsPostingsListIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	iterable := newTermsIterable(sr.fsSegment)
	// NB(rob): The segment reader holds a finalized state reader lock the whole
	// time before closed.
	fieldsIter := newFSTTermsIter(newFSTTermsIterOptions{})
	return iterable.fieldsWithRegexWithTermsIterAndContextLifetime(fieldsIter, compiled)
}

func (sr *fsSegmentReader) Terms(field []byte) (sgmt.TermsIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	iterable := newTermsIterable(sr.fsSegment)
	// NB(rob): The segment reader holds a finalized state reader lock the whole
	// time before closed.
	fieldsIter := newFSTTermsIter(newFSTTermsIterOptions{})
	return iterable.termsWithTermsIterAndContextLifetime(fieldsIter, field)
}

func (sr *fsSegmentReader) TermsWithRegex(
	field []byte,
	compiled *index.CompiledRegex,
) (sgmt.TermsIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	iterable := newTermsIterable(sr.fsSegment)
	// NB(rob): The segment reader holds a finalized state reader lock the whole
	// time before closed.
	fieldsIter := newFSTTermsIter(newFSTTermsIterOptions{})
	return iterable.termsWithRegexWithTermsIterAndContextLifetime(fieldsIter, field, compiled)
}

func (sr *fsSegmentReader) MatchField(field []byte) (postings.List, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.matchFieldNotClosedMaybeFinalizedWithRLock(field)
}

func (sr *fsSegmentReader) MatchTerm(field []byte, term []byte) (postings.List, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.matchTermNotClosedMaybeFinalizedWithRLock(field, term)
}

func (sr *fsSegmentReader) MatchRegexp(
	field []byte,
	compiled index.CompiledRegex,
) (postings.List, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.matchRegexpNotClosedMaybeFinalizedWithRLock(field, compiled)
}

func (sr *fsSegmentReader) MatchAll() (postings.List, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.matchAllNotClosedMaybeFinalizedWithRLock()
}

func (sr *fsSegmentReader) Metadata(id postings.ID) (doc.Metadata, error) {
	if sr.closed {
		return doc.Metadata{}, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.metadataNotClosedMaybeFinalizedWithRLock(id)
}

func (sr *fsSegmentReader) MetadataIterator(pl postings.List) (doc.MetadataIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	// Also make sure the doc retriever is the reader not the segment so that
	// is closed check is not performed and only the is finalized check.
	return sr.fsSegment.metadataIteratorNotClosedMaybeFinalizedWithRLock(sr, pl)
}

func (sr *fsSegmentReader) Doc(id postings.ID) (doc.Document, error) {
	if sr.closed {
		return doc.Document{}, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	return sr.fsSegment.docNotClosedMaybeFinalizedWithRLock(id)
}

func (sr *fsSegmentReader) NumDocs() (int, error) {
	if sr.closed {
		return 0, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	n := sr.fsSegment.numDocs
	return int(n), nil
}

func (sr *fsSegmentReader) Docs(pl postings.List) (doc.Iterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	// Also make sure the doc retriever is the reader not the segment so that
	// is closed check is not performed and only the is finalized check.
	return sr.fsSegment.docsNotClosedMaybeFinalizedWithRLock(sr, pl)
}

func (sr *fsSegmentReader) AllDocs() (index.IDDocIterator, error) {
	if sr.closed {
		return nil, errReaderClosed
	}
	// NB(r): We are allowed to call match field after Close called on
	// the segment but not after it is finalized.
	// Also make sure the doc retriever is the reader not the segment so that
	// is closed check is not performed and only the is finalized check.
	return sr.fsSegment.allDocsNotClosedMaybeFinalizedWithRLock(sr)
}

func (sr *fsSegmentReader) Close() error {
	if sr.closed {
		return errReaderClosed
	}
	sr.closed = true
	// Close the context so that segment doesn't need to track this any longer.
	sr.ctx.Close()
	return nil
}
