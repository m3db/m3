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
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/pborman/uuid"
)

var (
	errCantQueryClosedSegment = errors.New("cant query closed segment")
	errCantCloseClosedSegment = errors.New("cant close closed segment")
)

// ReadThroughSegment wraps a segment with a postings list cache so that
// queries can be transparently cached in a read through manner. In addition,
// the postings lists returned by the segments may not be safe to use once the
// underlying segments are closed due to the postings lists pointing into the
// segments mmap'd region. As a result, the close method of the ReadThroughSegment
// will make sure that the cache is purged of all the segments postings lists before
// the segment itself is closed.
type ReadThroughSegment struct {
	fst.Segment
	sync.Mutex

	opts              ReadThroughSegmentOptions
	uuid              uuid.UUID
	postingsListCache *PostingsListCache

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

// NewReadThroughSegment accepts a segment and a postings list cache
// and returns a read-through segment such that the postings list
// cache will be checked and updated with every query.
func NewReadThroughSegment(
	seg fst.Segment,
	cache *PostingsListCache,
	opts ReadThroughSegmentOptions,
) fst.Segment {
	return &ReadThroughSegment{
		Segment: seg,

		opts:              opts,
		uuid:              uuid.NewUUID(),
		postingsListCache: cache,
	}
}

// MatchRegexp returns a cached posting list or queries the underlying
// segment if their is a cache miss.
func (s *ReadThroughSegment) MatchRegexp(
	field []byte,
	c index.CompiledRegex,
) (postings.List, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil, errCantQueryClosedSegment
	}

	if s.postingsListCache == nil || !s.opts.CacheRegexp {
		return s.Segment.MatchRegexp(field, c)
	}

	pattern := c.FSTSyntax.String()
	pl, ok := s.postingsListCache.GetRegexp(s.uuid, pattern)
	if ok {
		return pl, nil
	}

	pl, err := s.Segment.MatchRegexp(field, c)
	if err == nil {
		s.postingsListCache.PutRegexp(s.uuid, pattern, pl)
	}
	return pl, err
}

// MatchTerm returns a cached posting list or queries the underlying
// segment if their is a cache miss.
func (s *ReadThroughSegment) MatchTerm(
	field []byte, term []byte,
) (postings.List, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil, errCantQueryClosedSegment
	}

	if s.postingsListCache == nil || !s.opts.CacheTerms {
		return s.Segment.MatchTerm(field, term)
	}

	termString := string(term)
	pl, ok := s.postingsListCache.GetTerm(s.uuid, termString)
	if ok {
		return pl, nil
	}

	pl, err := s.Segment.MatchTerm(field, term)
	if err == nil {
		s.postingsListCache.PutTerm(s.uuid, termString, pl)
	}
	return pl, err
}

// Close purges all entries in the cache associated with this segment,
// and then closes the underlying segment.
func (s *ReadThroughSegment) Close() error {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return errCantCloseClosedSegment
	}

	s.closed = true

	// Purge segments from the cache before closing the segment to avoid
	// temporarily having postings lists in the cache whose underlying
	// bytes are no longer mmap'd.
	s.postingsListCache.PurgeSegment(s.uuid)
	return s.Segment.Close()
}
