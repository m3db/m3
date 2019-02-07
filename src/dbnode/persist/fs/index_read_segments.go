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
	"io"
	"sync"

	"github.com/m3db/m3/src/dbnode/storage/index"
	m3ninxindex "github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	m3ninxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/pborman/uuid"
)

var (
	errFilesystemOptionsNotSpecified = errors.New("filesystem options not specified")
)

// ReadIndexSegmentsOptions is a set of options used when reading
// index segments.
type ReadIndexSegmentsOptions struct {
	// ReaderOptions is the index file set reader options.
	ReaderOptions IndexReaderOpenOptions

	// FilesystemOptions is the filesystem options which is
	// required for reading index segments.
	FilesystemOptions Options

	// QueryCache

	// Unexported fields that are hooks used for testing.
	newReaderFn            newIndexReaderFn
	newPersistentSegmentFn newPersistentSegmentFn
}

// ReadIndexSegments will read a set of segments.
func ReadIndexSegments(
	opts ReadIndexSegmentsOptions,
) ([]segment.Segment, error) {
	readerOpts := opts.ReaderOptions
	fsOpts := opts.FilesystemOptions
	if fsOpts == nil {
		return nil, errFilesystemOptionsNotSpecified
	}

	newReader := opts.newReaderFn
	if newReader == nil {
		newReader = NewIndexReader
	}

	newPersistentSegment := opts.newPersistentSegmentFn
	if newPersistentSegment == nil {
		newPersistentSegment = m3ninxpersist.NewSegment
	}

	reader, err := newReader(fsOpts)
	if err != nil {
		return nil, err
	}

	var (
		segments []segment.Segment
		success  = false
	)

	// Need to do this to guarantee we release all resources in case of failure.
	defer func() {
		if !success {
			for _, seg := range segments {
				seg.Close()
			}
			reader.Close()
		}
	}()

	if _, err := reader.Open(readerOpts); err != nil {
		return nil, err
	}
	segments = make([]segment.Segment, 0, reader.SegmentFileSets())

	for {
		fileset, err := reader.ReadSegmentFileSet()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, err
		}

		fstOpts := fsOpts.FSTOptions()
		seg, err := newPersistentSegment(fileset, fstOpts)
		if err != nil {
			return nil, err
		}

		segWithCache := newSegmentWithCache(seg, fsOpts.PostingsListCache())
		segments = append(segments, segWithCache)
	}

	// Indicate we don't need the defer() above to release any resources, as we are
	// transferring ownership to the caller.
	success = true
	return segments, nil
}

type segmentWithCache struct {
	fst.Segment
	sync.Mutex

	uuid              uuid.UUID
	postingsListCache *index.PostingsListCache

	closed bool
}

func newSegmentWithCache(
	seg fst.Segment,
	cache *index.PostingsListCache,
) *segmentWithCache {
	return &segmentWithCache{
		Segment: seg,

		uuid:              uuid.NewUUID(),
		postingsListCache: cache,
	}
}

func (s *segmentWithCache) MatchRegexp(
	field []byte,
	c m3ninxindex.CompiledRegex,
) (postings.List, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil, errors.New("cant query closed segment")
	}

	if s.postingsListCache == nil {
		return s.Segment.MatchRegexp(field, c)
	}

	pattern := c.FSTSyntax.String()
	pl, ok := s.postingsListCache.GetRegexp(s.uuid, pattern)
	if ok {
		return pl, nil
	}

	pl, err := s.Segment.MatchRegexp(field, c)
	if err != nil {
		s.postingsListCache.PutRegexp(s.uuid, pattern, pl)
	}
	return pl, err
}

func (s *segmentWithCache) MatchTerm(
	field []byte, term []byte,
) (postings.List, error) {
	s.Lock()
	defer s.Unlock()
	if s.closed {
		return nil, errors.New("cant query closed segment")
	}

	if s.postingsListCache == nil {
		return s.Segment.MatchTerm(field, term)
	}

	termString := string(term)
	pl, ok := s.postingsListCache.GetTerm(s.uuid, termString)
	if ok {
		return pl, nil
	}

	pl, err := s.Segment.MatchTerm(field, term)
	if err != nil {
		s.postingsListCache.PutTerm(s.uuid, termString, pl)
	}
	return pl, err
}

func (s *segmentWithCache) Close() error {
	s.Lock()
	defer s.Unlock()
	s.closed = true

	s.postingsListCache.PurgeSegment(s.uuid)
	return s.Segment.Close()
}
