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
	blockStart := start.Truncate(blockSize)

	encoder := m3tsz.NewEncoder(start, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	i := 0
	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: blockStart.Add(at), Value: float64(i)}
		err := encoder.Encode(datapoint, xtime.Second, nil)
		assert.NoError(t, err)
	}
	segment := encoder.Discard()
	mergedReader := xio.BlockReader{
		SegmentReader: xio.NewSegmentReader(segment),
		Start:         blockStart,
		BlockSize:     blockSize,
	}

	// Build two unmerged BlockReaders
	i = 100
	encoder = m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	encoderTwo := m3tsz.NewEncoder(middle, checked.NewBytes(nil, nil), true, encoding.NewOptions())
	useFirstEncoder := true
	secondBlockStart := blockStart.Add(blockSize)

	for at := time.Duration(0); at < blockSize; at += time.Minute {
		i++
		datapoint := ts.Datapoint{Timestamp: secondBlockStart.Add(at), Value: float64(i)}
		var err error
		if useFirstEncoder {
			err = encoder.Encode(datapoint, xtime.Second, nil)
		} else {
			err = encoderTwo.Encode(datapoint, xtime.Second, nil)
		}

		assert.NoError(t, err)
		useFirstEncoder = !useFirstEncoder
	}

	segment = encoder.Discard()
	segmentTwo := encoderTwo.Discard()
	unmergedReaders := []xio.BlockReader{
		{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         secondBlockStart,
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

// BuildTestSeriesIterator creates a sample SeriesIterator
// This series iterator has two identical replicas.
// Each replica has two blocks.
// The first block in each replica is merged and has values 1->30
// The values 1 and 2 appear before the SeriesIterator start time, and are not expected
// to appear when reading through the iterator
// The second block is unmerged; when it was merged, it has values 101 -> 130
// from two readers, one with even values and other with odd values
// Expected data points for reading through the iterator: [3..30,101..130], 58 in total
// SeriesIterator ID is 'foo', namespace is 'namespace'
// Tags are "foo": "bar" and "baz": "qux"
func BuildTestSeriesIterator(t *testing.T) encoding.SeriesIterator {
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

func validateSeriesInternals(t *testing.T, it encoding.SeriesIterator) {
	defer it.Close()

	replicas := it.Replicas()
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

func validateSeries(t *testing.T, it encoding.SeriesIterator) {
	defer it.Close()

	expectedValues := [60]float64{}
	for i := 0; i < 30; i++ {
		expectedValues[i] = float64(i) + 1
	}
	for i := 0; i < 30; i++ {
		expectedValues[i+30] = float64(i) + 101
	}
	for i := 2; i < 60; i++ {
		require.True(t, it.Next())
		dp, unit, annotation := it.Current()
		require.Equal(t, expectedValues[i], dp.Value)
		require.Equal(t, start.Add(time.Duration(i-2)*time.Minute), dp.Timestamp)
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
}

func TestGeneratedSeries(t *testing.T) {
	validateSeries(t, BuildTestSeriesIterator(t))
	validateSeriesInternals(t, BuildTestSeriesIterator(t))
}

type seriesIteratorWrapper struct {
	it     encoding.SeriesIterator
	closed bool
}

func TestConversionToCompressedData(t *testing.T) {
	it := BuildTestSeriesIterator(t)
	series, err := CompressedSeriesFromSeriesIterator(it)
	require.NoError(t, err)

	assert.Equal(t, []byte(seriesID), series.GetId())

	compressed := series.GetCompressed()
	assert.Equal(t, []byte(seriesNamespace), compressed.GetNamespace())
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
		assert.NotEmpty(t, merged.GetHead())
		assert.NotEmpty(t, merged.GetTail())
		assert.Equal(t, start.Truncate(blockSize).UnixNano(), merged.GetStartTime())
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

func TestSeriesConversionFromCompressedData(t *testing.T) {
	it := BuildTestSeriesIterator(t)
	rpcSeries, err := CompressedSeriesFromSeriesIterator(it)
	require.NoError(t, err)

	seriesIterator := SeriesIteratorFromCompressedSeries(rpcSeries, nil)
	validateSeries(t, seriesIterator)
	seriesIterator = SeriesIteratorFromCompressedSeries(rpcSeries, nil)
	validateSeriesInternals(t, seriesIterator)
}

func TestSeriesConversionFromCompressedDataWithIteratorPool(t *testing.T) {
	it := BuildTestSeriesIterator(t)
	rpcSeries, err := CompressedSeriesFromSeriesIterator(it)
	require.NoError(t, err)

	ip := &mockIteratorPool{}
	seriesIterator := SeriesIteratorFromCompressedSeries(rpcSeries, ip)
	validateSeries(t, seriesIterator)
	seriesIterator = SeriesIteratorFromCompressedSeries(rpcSeries, ip)
	validateSeriesInternals(t, seriesIterator)

	assert.True(t, ip.mriPoolUsed)
	assert.True(t, ip.siPoolUsed)
	assert.True(t, ip.mriaPoolUsed)
	assert.True(t, ip.cbwPoolUsed)
	assert.True(t, ip.identPoolUsed)
}

type mockIteratorPool struct {
	mriPoolUsed, siPoolUsed, mriaPoolUsed, cbwPoolUsed, identPoolUsed bool
}

var _ encoding.IteratorPools = &mockIteratorPool{}

func (ip *mockIteratorPool) MultiReaderIterator() encoding.MultiReaderIteratorPool {
	ip.mriPoolUsed = true
	mriPool := encoding.NewMultiReaderIteratorPool(nil)
	mriPool.Init(testIterAlloc)
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

// NB: make sure that SeriesIterator is not closed during conversion, or bytes will be empty
func TestConversionDoesNotCloseSeriesIterator(t *testing.T) {
	it := &seriesIteratorWrapper{
		it:     BuildTestSeriesIterator(t),
		closed: false,
	}
	CompressedSeriesFromSeriesIterator(it)
	assert.False(t, it.closed)
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
