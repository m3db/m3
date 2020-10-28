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

	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

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

type checkoutFn func(bootstrap.NamespaceDataAccumulator, uint32,
	ident.ID, ident.TagIterator) (bootstrap.CheckoutSeriesResult, error)

func checkoutWithLock(
	acc bootstrap.NamespaceDataAccumulator,
	shardID uint32,
	id ident.ID,
	tags ident.TagIterator,
) (bootstrap.CheckoutSeriesResult, error) {
	res, _, err := acc.CheckoutSeriesWithLock(shardID, id, tags)
	return res, err
}

func checkoutWithoutLock(
	acc bootstrap.NamespaceDataAccumulator,
	shardID uint32,
	id ident.ID,
	tags ident.TagIterator,
) (bootstrap.CheckoutSeriesResult, error) {
	res, _, err := acc.CheckoutSeriesWithoutLock(shardID, id, tags)
	return res, err
}

func TestCheckoutSeries(t *testing.T) {
	testCheckoutSeries(t, checkoutWithoutLock)
}

func TestCheckoutSeriesWithLock(t *testing.T) {
	testCheckoutSeries(t, checkoutWithLock)
}

func testCheckoutSeries(t *testing.T, checkoutFn checkoutFn) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()
	var (
		ns      = NewMockdatabaseNamespace(ctrl)
		series  = bootstrap.NewMockSeriesRef(ctrl)
		acc     = NewDatabaseNamespaceDataAccumulator(ns)
		shardID = uint32(7)

		release = &releaser{}
		ref     = SeriesReadWriteRef{
			UniqueIndex:         uniqueIdx,
			Series:              series,
			ReleaseReadWriteRef: release,
			Shard:               shardID,
		}
	)

	ns.EXPECT().SeriesReadWriteRef(shardID, id, tagIter).Return(ref, true, nil)
	ns.EXPECT().SeriesReadWriteRef(shardID, idErr, tagIter).
		Return(SeriesReadWriteRef{}, false, errors.New("err"))

	_, err := checkoutFn(acc, shardID, idErr, tagIter)
	require.Error(t, err)

	seriesResult, err := checkoutFn(acc, shardID, id, tagIter)
	require.NoError(t, err)
	require.Equal(t, series, seriesResult.Series)
	require.Equal(t, uniqueIdx, seriesResult.UniqueIndex)
	require.Equal(t, shardID, seriesResult.Shard)

	cast, ok := acc.(*namespaceDataAccumulator)
	require.True(t, ok)
	require.Equal(t, 1, len(cast.needsRelease))
	require.Equal(t, release, cast.needsRelease[0])
	// Ensure it hasn't been released.
	require.Equal(t, 0, release.calls)
}

func TestAccumulatorRelease(t *testing.T) {
	testAccumulatorRelease(t, checkoutWithoutLock)
}

func TestAccumulatorReleaseWithLock(t *testing.T) {
	testAccumulatorRelease(t, checkoutWithLock)
}

func testAccumulatorRelease(t *testing.T, checkoutFn checkoutFn) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	var (
		err     error
		ns      = NewMockdatabaseNamespace(ctrl)
		acc     = NewDatabaseNamespaceDataAccumulator(ns)
		shardID = uint32(1337)

		release = &releaser{}
		ref     = SeriesReadWriteRef{
			UniqueIndex:         uniqueIdx,
			Series:              bootstrap.NewMockSeriesRef(ctrl),
			ReleaseReadWriteRef: release,
		}
	)

	ns.EXPECT().SeriesReadWriteRef(shardID, id, tagIter).Return(ref, true, nil)
	_, err = checkoutFn(acc, shardID, id, tagIter)
	require.NoError(t, err)

	cast, ok := acc.(*namespaceDataAccumulator)
	require.True(t, ok)
	require.Equal(t, 1, len(cast.needsRelease))
	require.Equal(t, release, cast.needsRelease[0])

	require.NoError(t, acc.Close())
	require.Equal(t, 0, len(cast.needsRelease))
	// ensure release has been called.
	require.Equal(t, 1, release.calls)
	// ensure double-close errors.
	require.Error(t, acc.Close())
}
