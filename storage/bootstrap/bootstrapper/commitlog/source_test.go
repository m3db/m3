// Copyright (c) 2016 Uber Technologies, Inc.
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

package commitlog

import (
	"fmt"
	"io"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/encoding/m3tsz"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

const testNamespaceName = "testNamespace"

func testOptions() Options {
	opts := NewOptions()
	bopts := opts.BootstrapOptions()
	blopts := opts.BootstrapOptions().DatabaseBlockOptions()
	eopts := encoding.NewOptions()
	encoderPool := encoding.NewEncoderPool(nil)
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Time{}, nil, true, eopts)
	})
	readerIteratorPool := encoding.NewReaderIteratorPool(nil)
	readerIteratorPool.Init(func(reader io.Reader) encoding.ReaderIterator {
		return m3tsz.NewReaderIterator(reader, true, eopts)
	})
	multiReaderIteratorPool := encoding.NewMultiReaderIteratorPool(nil)
	multiReaderIteratorPool.Init(func(reader io.Reader) encoding.ReaderIterator {
		it := readerIteratorPool.Get()
		it.Reset(reader)
		return it
	})
	return opts.SetBootstrapOptions(bopts.SetDatabaseBlockOptions(blopts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool)))
}

func TestAvailableEmptyRangeError(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts)
	res := src.Available(testNamespaceName, bootstrap.ShardTimeRanges{})
	require.True(t, bootstrap.ShardTimeRanges{}.Equal(res))
}

func TestReadEmpty(t *testing.T) {
	opts := testOptions()

	src := newCommitLogSource(opts)

	res, err := src.Read(testNamespaceName, bootstrap.ShardTimeRanges{})
	require.Nil(t, res)
	require.Nil(t, err)
}

func TestReadErrorOnNewIteratorError(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts).(*commitLogSource)

	src.newIteratorFn = func(_ commitlog.Options) (commitlog.Iterator, error) {
		return nil, fmt.Errorf("an error")
	}

	ranges := xtime.NewRanges()
	ranges = ranges.AddRange(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Hour),
	})
	res, err := src.Read(testNamespaceName, bootstrap.ShardTimeRanges{0: ranges})
	require.Error(t, err)
	require.Nil(t, res)
}

func TestReadOrderedValues(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts).(*commitLogSource)

	blockSize := opts.BootstrapOptions().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= time.Hour)
	ranges := xtime.NewRanges()
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(time.Minute),
		End:   end,
	})

	foo := commitlog.Series{Shard: 0, ID: "foo"}
	bar := commitlog.Series{Shard: 1, ID: "bar"}
	baz := commitlog.Series{Shard: 2, ID: "baz"}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, nil},
		{bar, start.Add(2 * time.Minute), 1.0, xtime.Second, nil},
		{bar, start.Add(3 * time.Minute), 2.0, xtime.Second, nil},
		// "baz" is in shard 2 and should not be returned
		{baz, start.Add(4 * time.Minute), 1.0, xtime.Second, nil},
	}
	src.newIteratorFn = func(_ commitlog.Options) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	res, err := src.Read(testNamespaceName, bootstrap.ShardTimeRanges{0: ranges, 1: ranges})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 2, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	requireShardResults(t, values[:4], res.ShardResults(), opts)
}

func TestReadUnorderedValues(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts).(*commitLogSource)

	blockSize := opts.BootstrapOptions().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= time.Hour)
	ranges := xtime.NewRanges()
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(time.Minute),
		End:   end,
	})

	foo := commitlog.Series{Shard: 0, ID: "foo"}

	values := []testValue{
		{foo, start.Add(10 * time.Minute), 1.0, xtime.Second, nil},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, nil},
		{foo, start.Add(2 * time.Minute), 3.0, xtime.Second, nil},
		{foo, start.Add(3 * time.Minute), 4.0, xtime.Second, nil},
		{foo, start, 5.0, xtime.Second, nil},
	}
	src.newIteratorFn = func(_ commitlog.Options) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	res, err := src.Read(testNamespaceName, bootstrap.ShardTimeRanges{0: ranges, 1: ranges})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	requireShardResults(t, values, res.ShardResults(), opts)
}

func TestReadTrimsToRanges(t *testing.T) {
	opts := testOptions()
	src := newCommitLogSource(opts).(*commitLogSource)

	blockSize := opts.BootstrapOptions().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= time.Hour)
	ranges := xtime.NewRanges()
	ranges = ranges.AddRange(xtime.Range{
		Start: start.Add(time.Minute),
		End:   end,
	})

	foo := commitlog.Series{Shard: 0, ID: "foo"}

	values := []testValue{
		{foo, start.Add(-1 * time.Minute), 1.0, xtime.Nanosecond, nil},
		{foo, start, 2.0, xtime.Nanosecond, nil},
		{foo, start.Add(1 * time.Minute), 3.0, xtime.Nanosecond, nil},
		{foo, end.Truncate(blockSize).Add(blockSize).Add(time.Nanosecond), 4.0, xtime.Nanosecond, nil},
	}
	src.newIteratorFn = func(_ commitlog.Options) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	res, err := src.Read(testNamespaceName, bootstrap.ShardTimeRanges{0: ranges, 1: ranges})
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	requireShardResults(t, values[1:3], res.ShardResults(), opts)
}

type testValue struct {
	s commitlog.Series
	t time.Time
	v float64
	u xtime.Unit
	a ts.Annotation
}

func requireShardResults(
	t *testing.T,
	values []testValue,
	actual bootstrap.ShardResults,
	opts Options,
) {
	// First create what result should be constructed for test values
	bopts := opts.BootstrapOptions()
	ropts := bopts.RetentionOptions()
	blockSize := ropts.BlockSize()

	expected := bootstrap.ShardResults{}

	// Sort before iterating to ensure encoding to blocks is correct order
	sort.Stable(testValuesByTime(values))
	for _, v := range values {
		result, ok := expected[v.s.Shard]
		if !ok {
			result = bootstrap.NewShardResult(0, bopts)
			// Trigger blocks to be created for series
			result.AddSeries(v.s.ID, nil)
			expected[v.s.Shard] = result
		}

		blocks := result.AllSeries()[v.s.ID]
		blockStart := v.t.Truncate(blockSize)
		block := blocks.BlockOrAdd(blockStart)
		err := block.Write(v.t, v.v, v.u, v.a)
		require.NoError(t, err)
	}

	// Assert the values
	require.Equal(t, len(expected), len(actual))
	for shard, result := range expected {
		otherResult, ok := actual[shard]
		require.True(t, ok)

		series := result.AllSeries()
		otherSeries := otherResult.AllSeries()
		require.Equal(t, len(series), len(otherSeries))

		for id, blocks := range series {
			otherBlocks, ok := otherSeries[id]
			require.True(t, ok)

			allBlocks := blocks.AllBlocks()
			otherAllBlocks := otherBlocks.AllBlocks()
			require.Equal(t, len(allBlocks), len(otherAllBlocks))

			blopts := blocks.Options()
			readerIteratorPool := blopts.ReaderIteratorPool()
			for start, block := range allBlocks {
				otherBlock, ok := otherAllBlocks[start]
				require.True(t, ok)

				ctx := blopts.ContextPool().Get()
				defer ctx.Close()

				stream, streamErr := block.Stream(ctx)
				require.NoError(t, streamErr)

				otherStream, otherStreamErr := otherBlock.Stream(ctx)
				require.NoError(t, otherStreamErr)

				it := readerIteratorPool.Get()
				it.Reset(stream)
				defer it.Close()

				otherIt := readerIteratorPool.Get()
				otherIt.Reset(otherStream)
				defer otherIt.Close()

				for {
					next := it.Next()
					otherNext := otherIt.Next()
					if !next && !otherNext {
						break
					}

					require.True(t, next && otherNext)

					value, unit, annotation := it.Current()
					otherValue, otherUnit, otherAnnotation := otherIt.Current()
					require.True(t, value.Timestamp.Equal(otherValue.Timestamp))
					require.Equal(t, value.Value, otherValue.Value)
					require.Equal(t, unit, otherUnit)
					require.Equal(t, annotation, otherAnnotation)
				}
			}
		}
	}
}

type testCommitLogIterator struct {
	values []testValue
	idx    int
	err    error
	closed bool
}

type testValuesByTime []testValue

func (v testValuesByTime) Len() int      { return len(v) }
func (v testValuesByTime) Swap(i, j int) { v[i], v[j] = v[j], v[i] }
func (v testValuesByTime) Less(i, j int) bool {
	return v[i].t.Before(v[j].t)
}

func newTestCommitLogIterator(values []testValue, err error) *testCommitLogIterator {
	return &testCommitLogIterator{values: values, idx: -1, err: err}
}

func (i *testCommitLogIterator) Next() bool {
	i.idx++
	if i.idx >= len(i.values) {
		return false
	}
	return true
}

func (i *testCommitLogIterator) Current() (commitlog.Series, ts.Datapoint, xtime.Unit, ts.Annotation) {
	idx := i.idx
	if idx == -1 {
		idx = 0
	}
	v := i.values[idx]
	return v.s, ts.Datapoint{Timestamp: v.t, Value: v.v}, v.u, v.a
}

func (i *testCommitLogIterator) Err() error {
	return i.err
}

func (i *testCommitLogIterator) Close() {
	i.closed = true
}
