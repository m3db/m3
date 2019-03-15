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

package index

import (
	"errors"
	"sync"

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/pborman/uuid"
)

var (
	errCantGetReaderFromClosedSegment = errors.New("cant get reader from closed segment")
	errCantCloseClosedSegment         = errors.New("cant close closed segment")
)

// ReadThroughSegment wraps a segment with a postings list cache so that
// queries can be transparently cached in a read through manner. In addition,
// the postings lists returned by the segments may not be safe to use once the
// underlying segments are closed due to the postings lists pointing into the
// segments mmap'd region. As a result, the close method of the ReadThroughSegment
// will make sure that the cache is purged of all the segments postings lists before
// the segment itself is closed.
type ReadThroughSegment struct {
	segment.Segment
	sync.RWMutex

	uuid              uuid.UUID
	postingsListCache *PostingsListCache

	opts ReadThroughSegmentOptions

	closed bool
}

// ReadThroughSegmentOptions is the options struct for the
// ReadThroughSegment.
type ReadThroughSegmentOptions struct {
	// Whether the postings list for regexp queries should be cached.
	CacheRegexp bool
	// Whether the postings list for term queries should be cached.
	CacheTerms bool
}

// NewReadThroughSegment creates a new read through segment.
func NewReadThroughSegment(
	seg segment.Segment,
	cache *PostingsListCache,
	opts ReadThroughSegmentOptions,
) segment.Segment {
	return &ReadThroughSegment{
		Segment: seg,

		opts:              opts,
		uuid:              uuid.NewUUID(),
		postingsListCache: cache,
	}
}

// Reader returns a read through reader for the read through segment.
func (r *ReadThroughSegment) Reader() (index.Reader, error) {
	r.RLock()
	defer r.RUnlock()
	if r.closed {
		return nil, errCantGetReaderFromClosedSegment
	}

	reader, err := r.Segment.Reader()
	if err != nil {
		return nil, err
	}
	return newReadThroughSegmentReader(
		reader, r.uuid, r.postingsListCache, r.opts), nil
}

// Close purges all entries in the cache associated with this segment,
// and then closes the underlying segment.
func (r *ReadThroughSegment) Close() error {
	r.Lock()
	defer r.Unlock()
	if r.closed {
		return errCantCloseClosedSegment
	}

	r.closed = true

	if r.postingsListCache != nil {
		// Purge segments from the cache before closing the segment to avoid
		// temporarily having postings lists in the cache whose underlying
		// bytes are no longer mmap'd.
		r.postingsListCache.PurgeSegment(r.uuid)
	}
	return r.Segment.Close()
}

type readThroughSegmentReader struct {
	index.Reader

	opts              ReadThroughSegmentOptions
	uuid              uuid.UUID
	postingsListCache *PostingsListCache
}

func newReadThroughSegmentReader(
	reader index.Reader,
	uuid uuid.UUID,
	cache *PostingsListCache,
	opts ReadThroughSegmentOptions,
) index.Reader {
	return &readThroughSegmentReader{
		Reader: reader,

		opts:              opts,
		uuid:              uuid,
		postingsListCache: cache,
	}
}

// MatchRegexp returns a cached posting list or queries the underlying
// segment if their is a cache miss.
func (s *readThroughSegmentReader) MatchRegexp(
	field []byte,
	c index.CompiledRegex,
) (postings.List, error) {
	if s.postingsListCache == nil || !s.opts.CacheRegexp {
		return s.Reader.MatchRegexp(field, c)
	}

	// TODO(rartoul): Would be nice to not allocate strings here.
	fieldStr := string(field)
	patternStr := c.FSTSyntax.String()
	pl, ok := s.postingsListCache.GetRegexp(s.uuid, fieldStr, patternStr)
	if ok {
		return pl, nil
	}

	pl, err := s.Reader.MatchRegexp(field, c)
	if err == nil {
		s.postingsListCache.PutRegexp(s.uuid, fieldStr, patternStr, pl)
	}
	return pl, err
}

// MatchTerm returns a cached posting list or queries the underlying
// segment if their is a cache miss.
func (s *readThroughSegmentReader) MatchTerm(
	field []byte, term []byte,
) (postings.List, error) {
	if s.postingsListCache == nil || !s.opts.CacheTerms {
		return s.Reader.MatchTerm(field, term)
	}

	// TODO(rartoul): Would be nice to not allocate strings here.
	fieldStr := string(field)
	patternStr := string(term)
	pl, ok := s.postingsListCache.GetTerm(s.uuid, fieldStr, patternStr)
	if ok {
		return pl, nil
	}

	pl, err := s.Reader.MatchTerm(field, term)
	if err == nil {
		s.postingsListCache.PutTerm(s.uuid, fieldStr, patternStr, pl)
	}
	return pl, err
}
