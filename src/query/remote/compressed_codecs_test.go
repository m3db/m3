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

package remote

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/block"
	rpc "github.com/m3db/m3/src/query/generated/proto/rpcpb"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	seriesID        = "series"
	seriesNamespace = test.SeriesNamespace

	testTags = test.TestTags

	blockSize = test.BlockSize

	start       = test.Start
	seriesStart = test.SeriesStart
	middle      = test.Middle
	end         = test.End

	tagMu sync.RWMutex
)

func buildTestSeriesIterator(t *testing.T) encoding.SeriesIterator {
	it, err := test.BuildTestSeriesIterator(seriesID)
	assert.NoError(t, err)
	return it
}

func validateSeriesInternals(t *testing.T, it encoding.SeriesIterator) {
	defer it.Close()

	replicas, err := it.Replicas()
	require.NoError(t, err)
	expectedReaders := []int{1, 2}
	expectedStarts := []time.Time{start.Truncate(blockSize), middle.Truncate(blockSize)}
	for _, replica := range replicas {
		readers := replica.Readers()
		i := 0
		for hasNext := true; hasNext; hasNext = readers.Next() {
			l, s, size := readers.CurrentReaders()
			require.Equal(t, expectedReaders[i], l)
			assert.Equal(t, expectedStarts[i], s)
			assert.Equal(t, blockSize, size)
			for j := 0; j < l; j++ {
				block := readers.CurrentReaderAt(j)
				assert.Equal(t, expectedStarts[i], block.Start)
				assert.Equal(t, blockSize, block.BlockSize)
			}
			i++
		}
	}
}

func expectedValues() []float64 {
	expectedValues := make([]float64, 60)
	for i := 2; i < 30; i++ {
		expectedValues[i] = float64(i) + 1
	}
	for i := 0; i < 30; i++ {
		expectedValues[i+30] = float64(i) + 101
	}
	return expectedValues[2:]

}

func validateSeries(t *testing.T, it encoding.SeriesIterator) {
	defer it.Close()

	for i, expectedValue := range expectedValues() {
		require.True(t, it.Next())
		dp, unit, annotation := it.Current()
		require.Equal(t, expectedValue, dp.Value)
		require.Equal(t, seriesStart.Add(time.Duration(i)*time.Minute), dp.Timestamp)
		uv, err := unit.Value()
		assert.NoError(t, err)
		assert.Equal(t, time.Second, uv)
		assert.Empty(t, annotation)
	}

	assert.False(t, it.Next())
	assert.Equal(t, seriesID, it.ID().String())
	if it.Namespace() != nil {
		assert.Equal(t, seriesNamespace, it.Namespace().String())
	}
	assert.Equal(t, seriesStart, it.Start())
	assert.Equal(t, end, it.End())

	tagMu.RLock()
	tagIter := it.Tags()
	tagCount := 0
	expectedCount := 2
	for tagIter.Next() {
		tag := tagIter.Current()
		name := tag.Name.String()
		expectedVal, contains := testTags[name]
		require.True(t, contains)
		assert.Equal(t, expectedVal, tag.Value.String())
		tagCount++
	}

	assert.Equal(t, expectedCount, tagCount)
	tagMu.RUnlock()
}

func TestGeneratedSeries(t *testing.T) {
	validateSeries(t, buildTestSeriesIterator(t))
	validateSeriesInternals(t, buildTestSeriesIterator(t))
}

func verifyCompressedSeries(t *testing.T, s *rpc.Series) {
	meta := s.GetMeta()
	series := s.GetCompressed()
	require.NotNil(t, series)
	assert.Equal(t, []byte(seriesID), meta.GetId())
	assert.Equal(t, seriesStart.UnixNano(), meta.GetStartTime())
	assert.Equal(t, end.UnixNano(), meta.GetEndTime())

	replicas := series.GetReplicas()
	require.Len(t, replicas, 2)

	for _, replica := range replicas {
		segments := replica.GetSegments()
		require.Len(t, segments, 2)

		seg := segments[0]
		assert.Empty(t, seg.GetUnmerged())
		merged := seg.GetMerged()
		assert.NotNil(t, merged)
		assert.NotEmpty(t, merged.GetHead())
		assert.NotEmpty(t, merged.GetTail())
		assert.Equal(t, start.UnixNano(), merged.GetStartTime())
		assert.Equal(t, int64(blockSize), merged.GetBlockSize())

		seg = segments[1]
		assert.Nil(t, seg.GetMerged())
		unmergedSegments := seg.GetUnmerged()
		assert.Len(t, unmergedSegments, 2)
		for _, unmerged := range unmergedSegments {
			assert.NotEmpty(t, unmerged.GetHead())
			assert.NotEmpty(t, unmerged.GetTail())
			assert.Equal(t, middle.Truncate(blockSize).UnixNano(), unmerged.GetStartTime())
			assert.Equal(t, int64(blockSize), unmerged.GetBlockSize())
		}
	}
}

func TestConversionToCompressedData(t *testing.T) {
	it := buildTestSeriesIterator(t)
	series, err := compressedSeriesFromSeriesIterator(it, nil)
	require.Error(t, err)
	require.Nil(t, series)
}

func TestSeriesConversionFromCompressedData(t *testing.T) {
	it := buildTestSeriesIterator(t)
	series, err := compressedSeriesFromSeriesIterator(it, nil)
	require.Error(t, err)
	require.Nil(t, series)
}

func TestSeriesConversionFromCompressedDataWithIteratorPool(t *testing.T) {
	it := buildTestSeriesIterator(t)
	ip := test.MakeMockIteratorPool()
	series, err := compressedSeriesFromSeriesIterator(it, ip)

	require.NoError(t, err)
	verifyCompressedSeries(t, series)
	rpcSeries := series.GetCompressed()
	require.NotNil(t, rpcSeries)
	assert.NotEmpty(t, rpcSeries.GetCompressedTags())

	seriesIterator, err := seriesIteratorFromCompressedSeries(rpcSeries, series.GetMeta(), ip)
	require.NoError(t, err)
	validateSeries(t, seriesIterator)

	seriesIterator, err = seriesIteratorFromCompressedSeries(rpcSeries, series.GetMeta(), ip)
	require.NoError(t, err)
	validateSeriesInternals(t, seriesIterator)

	assert.True(t, ip.MriPoolUsed)
	assert.True(t, ip.SiPoolUsed)
	assert.True(t, ip.MriaPoolUsed)
	assert.True(t, ip.CbwPoolUsed)
	assert.True(t, ip.IdentPoolUsed)
	assert.True(t, ip.EncodePoolUsed)
	assert.True(t, ip.DecodePoolUsed)
	// Should not be using mutable series iterator pool
	assert.False(t, ip.MsiPoolUsed)
}

func TestEncodeToCompressedFetchResult(t *testing.T) {
	iters := encoding.NewSeriesIterators(
		[]encoding.SeriesIterator{buildTestSeriesIterator(t),
			buildTestSeriesIterator(t)}, nil)
	ip := test.MakeMockIteratorPool()
	result, err := consolidators.NewSeriesFetchResult(
		iters,
		nil,
		block.NewResultMetadata(),
	)

	require.NoError(t, err)
	fetchResult, err := encodeToCompressedSeries(result, ip)
	require.NoError(t, err)

	require.Len(t, fetchResult, 2)
	for _, series := range fetchResult {
		verifyCompressedSeries(t, series)
	}

	assert.True(t, ip.EncodePoolUsed)
	assert.False(t, ip.MriPoolUsed)
	assert.False(t, ip.SiPoolUsed)
	assert.False(t, ip.MriaPoolUsed)
	assert.False(t, ip.CbwPoolUsed)
	assert.False(t, ip.IdentPoolUsed)
	assert.False(t, ip.DecodePoolUsed)
	assert.False(t, ip.MsiPoolUsed)
}

func TestDecodeCompressedFetchResult(t *testing.T) {
	iters := encoding.NewSeriesIterators(
		[]encoding.SeriesIterator{buildTestSeriesIterator(t),
			buildTestSeriesIterator(t)}, nil)
	result, err := consolidators.NewSeriesFetchResult(
		iters,
		nil,
		block.NewResultMetadata(),
	)

	require.NoError(t, err)
	compressed, err := encodeToCompressedSeries(result, nil)
	require.Error(t, err)
	require.Nil(t, compressed)
}

func TestDecodeCompressedFetchResultWithIteratorPool(t *testing.T) {
	ip := test.MakeMockIteratorPool()
	iters := encoding.NewSeriesIterators(
		[]encoding.SeriesIterator{buildTestSeriesIterator(t),
			buildTestSeriesIterator(t)}, nil)

	result, err := consolidators.NewSeriesFetchResult(
		iters,
		nil,
		block.NewResultMetadata(),
	)

	require.NoError(t, err)
	compressed, err := encodeToCompressedSeries(result, ip)
	require.NoError(t, err)
	require.Len(t, compressed, 2)
	for _, series := range compressed {
		verifyCompressedSeries(t, series)
	}

	fetchResult := &rpc.FetchResponse{
		Series: compressed,
	}

	revertedIters, err := decodeCompressedFetchResponse(fetchResult, ip)
	require.NoError(t, err)
	revertedIterList := revertedIters.Iters()
	require.Len(t, revertedIterList, 2)
	for _, seriesIterator := range revertedIterList {
		validateSeries(t, seriesIterator)
	}

	revertedIters, err = decodeCompressedFetchResponse(fetchResult, ip)
	require.NoError(t, err)
	revertedIterList = revertedIters.Iters()
	require.Len(t, revertedIterList, 2)
	for _, seriesIterator := range revertedIterList {
		validateSeriesInternals(t, seriesIterator)
	}

	assert.True(t, ip.MriPoolUsed)
	assert.True(t, ip.SiPoolUsed)
	assert.True(t, ip.MriaPoolUsed)
	assert.True(t, ip.CbwPoolUsed)
	assert.True(t, ip.IdentPoolUsed)
	assert.True(t, ip.EncodePoolUsed)
	assert.True(t, ip.DecodePoolUsed)
	assert.True(t, ip.MsiPoolUsed)
}

// NB: make sure that SeriesIterator is not closed during conversion, or bytes will be empty
func TestConversionDoesNotCloseSeriesIterator(t *testing.T) {
	ctrl := gomock.NewController(t)
	mockIter := encoding.NewMockSeriesIterator(ctrl)
	mockIter.EXPECT().Close().Times(0)
	mockIter.EXPECT().Replicas().Return([]encoding.MultiReaderIterator{}, nil).Times(1)
	mockIter.EXPECT().Start().Return(time.Now()).Times(1)
	mockIter.EXPECT().End().Return(time.Now()).Times(1)
	mockIter.EXPECT().Tags().Return(ident.NewTagsIterator(ident.NewTags())).Times(1)
	mockIter.EXPECT().Namespace().Return(ident.StringID("")).Times(1)
	mockIter.EXPECT().ID().Return(ident.StringID("")).Times(1)

	compressedSeriesFromSeriesIterator(mockIter, nil)
}
