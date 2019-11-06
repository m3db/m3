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

package storage

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

var (
	id        = ident.StringID("foo")
	idErr     = ident.StringID("bar")
	tagIter   ident.TagIterator
	uniqueIdx = uint64(10)
)

type releaser struct {
	calls int
}

func (r *releaser) OnReleaseReadWriteRef() {
	r.calls++
}

func TestCheckoutSeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ns     = NewMockdatabaseNamespace(ctrl)
		series = series.NewMockDatabaseSeries(ctrl)
		acc    = NewDatabaseNamespaceDataAccumulator(ns)

		release = &releaser{}
		ref     = SeriesReadWriteRef{
			UniqueIndex:         uniqueIdx,
			Series:              series,
			ReleaseReadWriteRef: release,
		}
	)

	ns.EXPECT().SeriesReadWriteRef(id, tagIter).Return(ref, nil)
	ns.EXPECT().SeriesReadWriteRef(idErr, tagIter).
		Return(SeriesReadWriteRef{}, errors.New("err"))

	_, err := acc.CheckoutSeries(idErr, tagIter)
	require.Error(t, err)

	seriesResult, err := acc.CheckoutSeries(id, tagIter)
	require.NoError(t, err)
	require.Equal(t, series, seriesResult.Series)
	require.Equal(t, uniqueIdx, seriesResult.UniqueIndex)

	cast, ok := acc.(*namespaceDataAccumulator)
	require.True(t, ok)
	require.Equal(t, 1, len(cast.needsRelease))
	require.Equal(t, release, cast.needsRelease[0])
	// Ensure it hasn't been released.
	require.Equal(t, 0, release.calls)
}

func TestAccumulatorRelease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ns  = NewMockdatabaseNamespace(ctrl)
		acc = NewDatabaseNamespaceDataAccumulator(ns)

		release = &releaser{}
		ref     = SeriesReadWriteRef{
			UniqueIndex:         uniqueIdx,
			Series:              series.NewMockDatabaseSeries(ctrl),
			ReleaseReadWriteRef: release,
		}
	)

	ns.EXPECT().SeriesReadWriteRef(id, tagIter).Return(ref, nil)
	_, err := acc.CheckoutSeries(id, tagIter)
	require.NoError(t, err)

	cast, ok := acc.(*namespaceDataAccumulator)
	require.True(t, ok)
	require.Equal(t, 1, len(cast.needsRelease))
	require.Equal(t, release, cast.needsRelease[0])

	acc.Release()
	require.Equal(t, 0, len(cast.needsRelease))
	// ensure release has been called.
	require.Equal(t, 1, release.calls)

	// ensure double release does not panic.
	acc.Release()
	require.NoError(t, acc.Close())
}

func TestAccumulatorErrorsOnCloseWithoutRelease(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		ns  = NewMockdatabaseNamespace(ctrl)
		acc = NewDatabaseNamespaceDataAccumulator(ns)

		release = &releaser{}
		ref     = SeriesReadWriteRef{
			UniqueIndex:         uniqueIdx,
			Series:              series.NewMockDatabaseSeries(ctrl),
			ReleaseReadWriteRef: release,
		}
	)

	ns.EXPECT().SeriesReadWriteRef(id, tagIter).Return(ref, nil)
	_, err := acc.CheckoutSeries(id, tagIter)
	require.NoError(t, err)

	cast, ok := acc.(*namespaceDataAccumulator)
	require.True(t, ok)
	require.Equal(t, 1, len(cast.needsRelease))
	require.Equal(t, release, cast.needsRelease[0])

	err = acc.Close()
	// ensure release has been called regardless of error status.
	require.Equal(t, 0, len(cast.needsRelease))
	require.Equal(t, 1, release.calls)

	require.Error(t, err)
}
