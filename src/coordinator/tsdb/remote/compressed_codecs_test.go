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
	"fmt"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/encoding"
	"github.com/m3db/m3db/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3db/src/dbnode/ts"
	"github.com/m3db/m3db/src/dbnode/x/xio"
	"github.com/m3db/m3db/src/dbnode/x/xpool"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	seriesID        = "foo"
	seriesNamespace = "namespace"
)

var (
	testTags = map[string]string{"foo": "bar", "baz": "qux"}

	blockSize = time.Hour / 2

	start  = time.Now().Truncate(time.Hour).Add(2 * time.Minute)
	middle = start.Add(blockSize)
	end    = middle.Add(blockSize)

	testIterAlloc = func(r io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
	}
)

// Builds a MultiReaderIterator representing a single replica
// with two segments, one merged with values from 1->30, and
// one which is unmerged with 2 segments from 101->130
// with one of the unmerged containing even points, other containing odd
func buildReplica(t *testing.T) encoding.MultiReaderIterator {
	// Build a merged BlockReader
	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	i := 0
	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: start.Add(at), Value: float64(i)}
		err := encoder.Encode(datapoint, xtime.Second, nil)
		assert.NoError(t, err)
	}
	segment := encoder.Discard()
	blockStart := start.Truncate(blockSize)
	mergedReader := xio.BlockReader{
		SegmentReader: xio.NewSegmentReader(segment),
		Start:         blockStart,
		BlockSize:     blockSize,
	}

	// Build two unmerged BlockReaders
	i = 100
	encoder = m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	encoderTwo := m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: middle.Add(at), Value: float64(i)}
		if int(at)%int(2*time.Minute) == 0 {
			err := encoder.Encode(datapoint, xtime.Second, nil)
			assert.NoError(t, err)
		} else {
			err := encoderTwo.Encode(datapoint, xtime.Second, nil)
			assert.NoError(t, err)
		}
	}
	segment = encoder.Discard()
	segmentTwo := encoderTwo.Discard()
	secondBlockStart := blockStart.Add(blockSize)
	unmergedReaders := []xio.BlockReader{
		{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         secondBlockStart.Add(time.Minute),
			BlockSize:     blockSize,
		},
		{
			SegmentReader: xio.NewSegmentReader(segmentTwo),
			Start:         secondBlockStart,
			BlockSize:     blockSize,
		},
	}

	multiReader := encoding.NewMultiReaderIterator(testIterAlloc, nil)
	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator([][]xio.BlockReader{
		{mergedReader},
		unmergedReaders,
	})

	multiReader.ResetSliceOfSlices(sliceOfSlicesIter)
	return multiReader
}

func buildTestSeriesIterator(t *testing.T) encoding.SeriesIterator {
	replicaOne := buildReplica(t)
	replicaTwo := buildReplica(t)

	tags := ident.Tags{}
	for name, value := range testTags {
		tags.Append(ident.StringTag(name, value))
	}

	return encoding.NewSeriesIterator(
		ident.StringID(seriesID),
		ident.StringID(seriesNamespace),
		ident.NewTagsIterator(tags),
		start,
		end,
		[]encoding.MultiReaderIterator{
			replicaOne,
			replicaTwo,
		},
		nil,
	)
}

func validateSeries(t *testing.T, it encoding.SeriesIterator) {
	expectedValues := [60]float64{}
	for i := 0; i < 30; i++ {
		expectedValues[i] = float64(i) + 1
	}
	for i := 0; i < 30; i++ {
		expectedValues[i+30] = float64(i) + 101
	}
	for i := 0; i < 60; i++ {
		require.True(t, it.Next())
		dp, unit, annotation := it.Current()
		assert.Equal(t, start.Add(time.Duration(i)*time.Minute), dp.Timestamp)
		require.Equal(t, expectedValues[i], dp.Value)
		uv, err := unit.Value()
		assert.NoError(t, err)
		assert.Equal(t, time.Second, uv)
		assert.Empty(t, annotation)
	}

	assert.False(t, it.Next())
	assert.Equal(t, seriesID, it.ID().String())
	assert.Equal(t, seriesNamespace, it.Namespace().String())
	assert.Equal(t, start, it.Start())
	assert.Equal(t, end, it.End())

	tagIter := it.Tags()
	tagCount := 0
	expectedCount := len(testTags)
	for tagIter.Next() {
		tag := tagIter.Current()
		name := tag.Name.String()
		expectedVal, contains := testTags[name]
		require.True(t, contains)
		assert.Equal(t, expectedVal, tag.Value.String())
		delete(testTags, expectedVal)
		tagCount++
	}
	assert.Equal(t, expectedCount, tagCount)

	it.Close()
}

func TestGeneratedSeries(t *testing.T) {
	validateSeries(t, buildTestSeriesIterator(t))
}

var (
	mergedHead   = []byte{21, 49, 84, 189, 36, 231, 80, 0, 48, 28, 243, 187, 187, 187, 187, 187, 187, 187, 187, 187, 187, 187, 187, 187, 187}
	mergedTail   = []byte{192, 0}
	unmergedHead = [][]byte{
		{21, 49, 86, 96, 61, 67, 160, 0, 49, 156, 184, 241, 130, 96, 152, 38, 8, 48, 89, 206, 115, 156, 231, 57},
		{21, 49, 86, 96, 61, 67, 160, 0, 158, 49, 156, 211, 204, 19, 4, 193, 48, 65, 130, 206, 115, 156, 231, 57},
	}
	unmergedTail = [][]byte{
		{208, 0},
		{206, 128, 0},
	}
)

type seriesIteratorWrapper struct {
	it     encoding.SeriesIterator
	closed bool
}

func TestConversionToCompressedData(t *testing.T) {
	it := buildTestSeriesIterator(t)
	series, err := RPCFromSeriesIterator(it)
	require.NoError(t, err)

	assert.Equal(t, seriesID, series.GetId())

	compressed := series.GetCompressed()
	assert.Equal(t, seriesNamespace, compressed.GetNamespace())
	assert.Equal(t, start.UnixNano(), compressed.GetStartTime())
	assert.Equal(t, end.UnixNano(), compressed.GetEndTime())

	replicas := compressed.GetReplicas()
	require.Len(t, replicas, 2)

	for _, replica := range replicas {
		segments := replica.GetSegments()
		require.Len(t, segments, 2)

		seg := segments[0]
		assert.Empty(t, seg.GetUnmerged())
		merged := seg.GetMerged()
		assert.NotNil(t, merged)
		assert.Equal(t, mergedHead, merged.GetHead())
		assert.Equal(t, mergedTail, merged.GetTail())
		fmt.Println(merged.GetHead(), merged.GetTail())

		seg = segments[1]
		assert.Nil(t, seg.GetMerged())
		unmergedSegments := seg.GetUnmerged()
		assert.Len(t, unmergedSegments, 2)
		for i, unmerged := range unmergedSegments {
			fmt.Println(unmerged.GetHead(), unmerged.GetTail())
			assert.Equal(t, unmergedHead[i], unmerged.GetHead())
			assert.Equal(t, unmergedTail[i], unmerged.GetTail())
		}
	}
}

func TestSeriesConversionFromCompressedData(t *testing.T) {
	it := buildTestSeriesIterator(t)
	rpcSeries, err := RPCFromSeriesIterator(it)
	require.NoError(t, err)
	seriesIterator := SeriesIteratorFromRPC(nil, rpcSeries)
	validateSeries(t, seriesIterator)
	seriesIterator.Close()
}

func TestSeriesConversionFromCompressedDataWithIteratorPool(t *testing.T) {
	it := buildTestSeriesIterator(t)
	rpcSeries, err := RPCFromSeriesIterator(it)
	require.NoError(t, err)

	ip := &mockIteratorPool{}
	seriesIterator := SeriesIteratorFromRPC(ip, rpcSeries)

	assert.True(t, ip.mriPoolUsed)
	assert.True(t, ip.siPoolUsed)
	assert.True(t, ip.mriaPoolUsed)
	assert.True(t, ip.cbwPoolUsed)
	assert.True(t, ip.identPoolUsed)

	validateSeries(t, seriesIterator)
	seriesIterator.Close()
}

type mockIteratorPool struct {
	mriPoolUsed, siPoolUsed, mriaPoolUsed, cbwPoolUsed, identPoolUsed bool
}

var _ encoding.IteratorPools = &mockIteratorPool{}

func (ip *mockIteratorPool) MultiReaderIterator() encoding.MultiReaderIteratorPool {
	ip.mriPoolUsed = true
	mriPool := encoding.NewMultiReaderIteratorPool(nil)
	mriPool.Init(iterAlloc)
	return mriPool
}
func (ip *mockIteratorPool) SeriesIterator() encoding.SeriesIteratorPool {
	ip.siPoolUsed = true
	siPool := encoding.NewSeriesIteratorPool(nil)
	siPool.Init()
	return siPool
}
func (ip *mockIteratorPool) MultiReaderIteratorArray() encoding.MultiReaderIteratorArrayPool {
	ip.mriaPoolUsed = true
	mriaPool := encoding.NewMultiReaderIteratorArrayPool(nil)
	mriaPool.Init()
	return mriaPool
}
func (ip *mockIteratorPool) CheckedBytesWrapper() xpool.CheckedBytesWrapperPool {
	ip.cbwPoolUsed = true
	cbwPool := xpool.NewCheckedBytesWrapperPool(nil)
	cbwPool.Init()
	return cbwPool
}
func (ip *mockIteratorPool) ID() ident.Pool {
	ip.identPoolUsed = true

	buckets := []pool.Bucket{{Capacity: 100, Count: 100}}
	bytesPool := pool.NewCheckedBytesPool(buckets, nil,
		func(sizes []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(sizes, nil)
		})
	bytesPool.Init()
	return ident.NewPool(bytesPool, ident.PoolOptions{})
}

func TestConversionClosesSeriesIterator(t *testing.T) {
	it := &seriesIteratorWrapper{
		it:     buildTestSeriesIterator(t),
		closed: false,
	}
	RPCFromSeriesIterator(it)
	assert.True(t, it.closed)
}

var _ encoding.SeriesIterator = &seriesIteratorWrapper{}

func (it *seriesIteratorWrapper) ID() ident.ID                             { return it.it.ID() }
func (it *seriesIteratorWrapper) Namespace() ident.ID                      { return it.it.Namespace() }
func (it *seriesIteratorWrapper) Tags() ident.TagIterator                  { return it.it.Tags() }
func (it *seriesIteratorWrapper) Start() time.Time                         { return it.it.Start() }
func (it *seriesIteratorWrapper) End() time.Time                           { return it.it.End() }
func (it *seriesIteratorWrapper) Replicas() []encoding.MultiReaderIterator { return it.it.Replicas() }
func (it *seriesIteratorWrapper) Next() bool                               { return it.it.Next() }
func (it *seriesIteratorWrapper) Err() error                               { return it.it.Err() }
func (it *seriesIteratorWrapper) Close() {
	it.closed = true
	it.it.Close()
}
func (it *seriesIteratorWrapper) Current() (ts.Datapoint, xtime.Unit, ts.Annotation) {
	return it.it.Current()
}
func (it *seriesIteratorWrapper) Reset(id ident.ID, ns ident.ID, t ident.TagIterator, startInclusive, endExclusive time.Time, replicas []encoding.MultiReaderIterator) {
	it.it.Reset(id, ns, t, startInclusive, endExclusive, replicas)
}
