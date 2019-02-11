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
	"regexp/syntax"
	"testing"

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/postings/roaring"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	defaultReadThroughSegmentOptions = ReadThroughSegmentOptions{
		CacheRegexp: true,
		CacheTerms:  true,
	}
)

func TestReadThroughSegmentMatchRegexp(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)
	reader := index.NewMockReader(ctrl)
	segment.EXPECT().Reader().Return(reader, nil)

	cache, err := NewPostingsListCache(1, testPostingListCacheOptions)
	require.NoError(t, err)

	field := []byte("some-field")
	parsedRegex, err := syntax.Parse(".*this-will-be-slow.*", syntax.Simple)
	require.NoError(t, err)
	compiledRegex := index.CompiledRegex{
		FSTSyntax: parsedRegex,
	}

	readThrough, err := NewReadThroughSegment(
		segment, cache, defaultReadThroughSegmentOptions).Reader()
	require.NoError(t, err)

	originalPL := roaring.NewPostingsList()
	require.NoError(t, originalPL.Insert(1))
	reader.EXPECT().MatchRegexp(field, gomock.Any()).Return(originalPL, nil)

	// Make sure it goes to the segment when the cache misses.
	pl, err := readThrough.MatchRegexp(field, compiledRegex)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))

	// Make sure it relies on the cache if its present (mock only expects
	// one call.)
	pl, err = readThrough.MatchRegexp(field, compiledRegex)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestReadThroughSegmentMatchRegexpCacheDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)
	reader := index.NewMockReader(ctrl)
	segment.EXPECT().Reader().Return(reader, nil)

	cache, err := NewPostingsListCache(1, testPostingListCacheOptions)
	require.NoError(t, err)

	field := []byte("some-field")
	parsedRegex, err := syntax.Parse(".*this-will-be-slow.*", syntax.Simple)
	require.NoError(t, err)
	compiledRegex := index.CompiledRegex{
		FSTSyntax: parsedRegex,
	}

	readThrough, err := NewReadThroughSegment(segment, cache, ReadThroughSegmentOptions{
		CacheRegexp: false,
	}).Reader()
	require.NoError(t, err)

	originalPL := roaring.NewPostingsList()
	require.NoError(t, originalPL.Insert(1))
	reader.EXPECT().
		MatchRegexp(field, gomock.Any()).
		Return(originalPL, nil).
		Times(2)

	// Make sure it goes to the segment.
	pl, err := readThrough.MatchRegexp(field, compiledRegex)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))

	// Make sure it goes to the segment the second time - meaning the cache was
	// disabled.
	pl, err = readThrough.MatchRegexp(field, compiledRegex)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestReadThroughSegmentMatchRegexpNoCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		segment          = fst.NewMockSegment(ctrl)
		reader           = index.NewMockReader(ctrl)
		field            = []byte("some-field")
		parsedRegex, err = syntax.Parse(".*this-will-be-slow.*", syntax.Simple)
	)
	require.NoError(t, err)

	segment.EXPECT().Reader().Return(reader, nil)
	compiledRegex := index.CompiledRegex{
		FSTSyntax: parsedRegex,
	}

	readThrough, err := NewReadThroughSegment(
		segment, nil, defaultReadThroughSegmentOptions).Reader()
	require.NoError(t, err)

	originalPL := roaring.NewPostingsList()
	require.NoError(t, originalPL.Insert(1))
	reader.EXPECT().MatchRegexp(field, gomock.Any()).Return(originalPL, nil)

	// Make sure it it works with no cache.
	pl, err := readThrough.MatchRegexp(field, compiledRegex)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestReadThroughSegmentMatchTerm(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)
	reader := index.NewMockReader(ctrl)
	segment.EXPECT().Reader().Return(reader, nil)

	cache, err := NewPostingsListCache(1, testPostingListCacheOptions)
	require.NoError(t, err)

	var (
		field = []byte("some-field")
		term  = []byte("some-term")

		originalPL = roaring.NewPostingsList()
	)
	require.NoError(t, originalPL.Insert(1))

	readThrough, err := NewReadThroughSegment(
		segment, cache, defaultReadThroughSegmentOptions).Reader()
	require.NoError(t, err)

	reader.EXPECT().MatchTerm(field, term).Return(originalPL, nil)

	// Make sure it goes to the segment when the cache misses.
	pl, err := readThrough.MatchTerm(field, term)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))

	// Make sure it relies on the cache if its present (mock only expects
	// one call.)
	pl, err = readThrough.MatchTerm(field, term)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestReadThroughSegmentMatchTermCacheDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)
	reader := index.NewMockReader(ctrl)
	segment.EXPECT().Reader().Return(reader, nil)

	cache, err := NewPostingsListCache(1, testPostingListCacheOptions)
	require.NoError(t, err)

	var (
		field = []byte("some-field")
		term  = []byte("some-term")

		originalPL = roaring.NewPostingsList()
	)
	require.NoError(t, originalPL.Insert(1))

	readThrough, err := NewReadThroughSegment(segment, cache, ReadThroughSegmentOptions{
		CacheTerms: false,
	}).Reader()
	require.NoError(t, err)

	reader.EXPECT().
		MatchTerm(field, term).
		Return(originalPL, nil).
		Times(2)

	// Make sure it goes to the segment when the cache misses.
	pl, err := readThrough.MatchTerm(field, term)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))

	// Make sure it goes to the segment the second time - meaning the cache was
	// disabled.
	pl, err = readThrough.MatchTerm(field, term)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestReadThroughSegmentMatchTermNoCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		segment = fst.NewMockSegment(ctrl)
		reader  = index.NewMockReader(ctrl)

		field = []byte("some-field")
		term  = []byte("some-term")

		originalPL = roaring.NewPostingsList()
	)
	require.NoError(t, originalPL.Insert(1))

	segment.EXPECT().Reader().Return(reader, nil)

	readThrough, err := NewReadThroughSegment(
		segment, nil, defaultReadThroughSegmentOptions).Reader()
	require.NoError(t, err)

	reader.EXPECT().MatchTerm(field, term).Return(originalPL, nil)

	// Make sure it it works with no cache.
	pl, err := readThrough.MatchTerm(field, term)
	require.NoError(t, err)
	require.True(t, pl.Equal(originalPL))
}

func TestClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)
	cache, err := NewPostingsListCache(1, testPostingListCacheOptions)
	require.NoError(t, err)

	readThroughSeg := NewReadThroughSegment(
		segment, cache, defaultReadThroughSegmentOptions)

	segmentUUID := readThroughSeg.(*ReadThroughSegment).uuid

	// Store an entry for the segment in the cache so we can check if it
	// gets purged after.
	cache.PutRegexp(segmentUUID, "some-regexp", roaring.NewPostingsList())

	segment.EXPECT().Close().Return(nil)
	err = readThroughSeg.Close()
	require.NoError(t, err)
	require.True(t, readThroughSeg.(*ReadThroughSegment).closed)

	// Make sure it does not allow double closes.
	err = readThroughSeg.Close()
	require.Equal(t, errCantCloseClosedSegment, err)

	// Make sure it does not allow readers to be created after closing.
	_, err = readThroughSeg.Reader()
	require.Equal(t, errCantGetReaderFromClosedSegment, err)
}

func TestCloseNoCache(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	segment := fst.NewMockSegment(ctrl)

	readThrough := NewReadThroughSegment(
		segment, nil, defaultReadThroughSegmentOptions)

	segment.EXPECT().Close().Return(nil)
	err := readThrough.Close()
	require.NoError(t, err)
	require.True(t, readThrough.(*ReadThroughSegment).closed)
}
