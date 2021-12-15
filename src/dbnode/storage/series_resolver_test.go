// Copyright (c) 2021 Uber Technologies, Inc.
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

package storage

import (
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
)

func TestResolveError(t *testing.T) {
	wg := sync.WaitGroup{}
	id := ident.StringID("foo")
	sut := NewSeriesResolver(&wg, &Entry{ID: id}, func(id ident.ID) (*Entry, error) {
		return nil, fmt.Errorf("unable to resolve series")
	})
	_, err := sut.SeriesRef()
	require.Error(t, err)
}

func TestResolveNilEntry(t *testing.T) {
	wg := sync.WaitGroup{}
	id := ident.StringID("foo")
	sut := NewSeriesResolver(&wg, &Entry{ID: id}, func(id ident.ID) (*Entry, error) {
		return nil, nil
	})
	_, err := sut.SeriesRef()
	require.Error(t, err)
}

func TestResolve(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	id := ident.StringID("foo")
	sut := NewSeriesResolver(&wg, &Entry{ID: id}, func(id ident.ID) (*Entry, error) {
		return NewEntry(NewEntryOptions{
			Series: newMockSeries(ctrl),
			Index:  11,
		}), nil
	})
	seriesRef, err := sut.SeriesRef()
	require.NoError(t, err)
	require.IsType(t, &Entry{}, seriesRef)
	entry := seriesRef.(*Entry)
	require.Equal(t, uint64(11), entry.Index)
}

func TestSecondResolveWontWait(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	id := ident.StringID("foo")
	sut := NewSeriesResolver(&wg, &Entry{ID: id}, func(id ident.ID) (*Entry, error) {
		return NewEntry(NewEntryOptions{
			Series: newMockSeries(ctrl),
			Index:  11,
		}), nil
	})
	seriesRef, err := sut.SeriesRef()
	require.NoError(t, err)
	require.IsType(t, &Entry{}, seriesRef)
	entry := seriesRef.(*Entry)
	require.Equal(t, uint64(11), entry.Index)

	wg.Add(1)
	seriesRef2, err := sut.SeriesRef()
	require.NoError(t, err)
	require.IsType(t, &Entry{}, seriesRef2)
	entry2 := seriesRef2.(*Entry)
	require.Equal(t, entry, entry2)
}

func TestReleaseRef(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	entry := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
	entry.IncrementReaderWriterCount()
	sut := NewSeriesResolver(&wg, entry, func(id ident.ID) (*Entry, error) {
		entry.IncrementReaderWriterCount()
		return entry, nil
	})

	// Initial creation increments the entry.
	require.Equal(t, int32(1), entry.ReaderWriterCount())

	seriesRef, err := sut.SeriesRef()
	require.NoError(t, err)
	require.IsType(t, &Entry{}, seriesRef)

	// Subsequent reference increments the entry (this is to be defensive around
	// ref counting based on the one that happened to be created vs one persisted in the shard).
	require.Equal(t, int32(2), entry.ReaderWriterCount())

	sut.ReleaseRef()

	// All references released.
	require.Zero(t, entry.ReaderWriterCount())
}

func TestReleaseRefWithoutSeriesRef(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	wg := sync.WaitGroup{}
	id := ident.StringID("foo")
	sut := NewSeriesResolver(&wg, &Entry{ID: id}, func(id ident.ID) (*Entry, error) {
		entry := NewEntry(NewEntryOptions{Series: newMockSeries(ctrl)})
		entry.IncrementReaderWriterCount()
		return entry, nil
	})
	sut.ReleaseRef()
}
