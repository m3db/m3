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

package commitlog

import (
	"fmt"
	"io"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID       = ident.StringID("testnamespace")
	testDefaultRunOpts    = bootstrap.NewRunOptions()
	minCommitLogRetention = 10 * time.Minute
)

func testNsMetadata(t *testing.T) namespace.Metadata {
	md, err := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	require.NoError(t, err)
	return md
}

func testOptions() Options {
	opts := NewOptions()
	ropts := opts.ResultOptions()
	rlopts := opts.ResultOptions().DatabaseBlockOptions()
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
	return opts.SetResultOptions(ropts.SetDatabaseBlockOptions(rlopts.
		SetEncoderPool(encoderPool).
		SetReaderIteratorPool(readerIteratorPool).
		SetMultiReaderIteratorPool(multiReaderIteratorPool)))
}

func TestAvailableEmptyRangeError(t *testing.T) {
	var (
		opts     = testDefaultOpts
		src      = newCommitLogSource(opts, fs.Inspection{})
		res, err = src.AvailableData(testNsMetadata(t), result.ShardTimeRanges{}, testDefaultRunOpts)
	)
	require.NoError(t, err)
	require.True(t, result.ShardTimeRanges{}.Equal(res))
}

func TestReadEmpty(t *testing.T) {
	opts := testDefaultOpts

	src := newCommitLogSource(opts, fs.Inspection{})

	res, err := src.ReadData(testNsMetadata(t), result.ShardTimeRanges{},
		testDefaultRunOpts)
	require.NoError(t, err)
	require.Equal(t, 0, len(res.ShardResults()))
	require.True(t, res.Unfulfilled().IsEmpty())
}

func TestReadErrorOnNewIteratorError(t *testing.T) {
	opts := testDefaultOpts
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return nil, fmt.Errorf("an error")
	}

	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: time.Now(),
		End:   time.Now().Add(time.Hour),
	})
	res, err := src.ReadData(testNsMetadata(t), result.ShardTimeRanges{0: ranges},
		testDefaultRunOpts)
	require.Error(t, err)
	require.Nil(t, res)
}

func TestReadOrderedValues(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= minCommitLogRetention)
	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo")}
	bar := commitlog.Series{Namespace: testNamespaceID, Shard: 1, ID: ident.StringID("bar")}
	baz := commitlog.Series{Namespace: testNamespaceID, Shard: 2, ID: ident.StringID("baz")}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, nil},
		{bar, start.Add(2 * time.Minute), 1.0, xtime.Second, nil},
		{bar, start.Add(3 * time.Minute), 2.0, xtime.Second, nil},
		// "baz" is in shard 2 and should not be returned
		{baz, start.Add(4 * time.Minute), 1.0, xtime.Second, nil},
	}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges}
	res, err := src.ReadData(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 2, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	require.NoError(t, verifyShardResultsAreCorrect(
		values[:4], blockSize, res.ShardResults(), opts))
}

func TestReadUnorderedValues(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= minCommitLogRetention)
	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo")}

	values := []testValue{
		{foo, start.Add(10 * time.Minute), 1.0, xtime.Second, nil},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, nil},
		{foo, start.Add(2 * time.Minute), 3.0, xtime.Second, nil},
		{foo, start.Add(3 * time.Minute), 4.0, xtime.Second, nil},
		{foo, start, 5.0, xtime.Second, nil},
	}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges}
	res, err := src.ReadData(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	require.NoError(t, verifyShardResultsAreCorrect(
		values, blockSize, res.ShardResults(), opts))
}

// TestReadHandlesDifferentSeriesWithIdenticalUniqueIndex was added as a regression test to make
// sure that the commit log bootstrapper does not make any assumptions about series having a unique
// unique index because that only holds for the duration that an M3DB node is on, but commit log
// files can span multiple M3DB processes which means that unique indexes could be re-used for multiple
// different series.
func TestReadHandlesDifferentSeriesWithIdenticalUniqueIndex(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	require.True(t, blockSize >= minCommitLogRetention)
	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	// All series need to be in the same shard to exercise the regression.
	foo := commitlog.Series{
		Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo"), UniqueIndex: 0}
	bar := commitlog.Series{
		Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("bar"), UniqueIndex: 0}

	values := []testValue{
		{foo, start, 1.0, xtime.Second, nil},
		{bar, start, 2.0, xtime.Second, nil},
	}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges}
	res, err := src.ReadData(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	require.NoError(t, verifyShardResultsAreCorrect(
		values, blockSize, res.ShardResults(), opts))
}

func TestReadTrimsToRanges(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	// Request a little after the start of data, because always reading full blocks
	// it should return the entire block beginning from "start"
	require.True(t, blockSize >= minCommitLogRetention)
	ranges := xtime.Ranges{}
	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	foo := commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo")}

	values := []testValue{
		{foo, start.Add(-1 * time.Minute), 1.0, xtime.Nanosecond, nil},
		{foo, start, 2.0, xtime.Nanosecond, nil},
		{foo, start.Add(1 * time.Minute), 3.0, xtime.Nanosecond, nil},
		{foo, end.Truncate(blockSize).Add(blockSize).Add(time.Nanosecond), 4.0, xtime.Nanosecond, nil},
	}
	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(values, nil), nil
	}

	targetRanges := result.ShardTimeRanges{0: ranges, 1: ranges}
	res, err := src.ReadData(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	require.NoError(t, verifyShardResultsAreCorrect(
		values[1:3], blockSize, res.ShardResults(), opts))
}

func TestItMergesSnapshotsAndCommitLogs(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		opts      = testDefaultOpts
		md        = testNsMetadata(t)
		src       = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		blockSize = md.Options().RetentionOptions().BlockSize()
		now       = time.Now()
		start     = now.Truncate(blockSize).Add(-blockSize)
		end       = now.Truncate(blockSize)
		ranges    = xtime.Ranges{}

		foo             = commitlog.Series{Namespace: testNamespaceID, Shard: 0, ID: ident.StringID("foo")}
		commitLogValues = []testValue{
			{foo, start.Add(2 * time.Minute), 1.0, xtime.Nanosecond, nil},
			{foo, start.Add(3 * time.Minute), 2.0, xtime.Nanosecond, nil},
			{foo, start.Add(4 * time.Minute), 3.0, xtime.Nanosecond, nil},

			// Should not be present
			{foo, end.Truncate(blockSize).Add(blockSize).Add(time.Nanosecond), 4.0, xtime.Nanosecond, nil},
		}
	)

	// Request a little after the start of data, because always reading full blocks it
	// should return the entire block beginning from "start".
	require.True(t, blockSize >= minCommitLogRetention)

	ranges = ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	src.newIteratorFn = func(_ commitlog.IteratorOpts) (commitlog.Iterator, error) {
		return newTestCommitLogIterator(commitLogValues, nil), nil
	}
	src.snapshotFilesFn = func(filePathPrefix string, namespace ident.ID, shard uint32) (fs.FileSetFilesSlice, error) {
		return fs.FileSetFilesSlice{
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					BlockStart:  start,
					Shard:       shard,
					VolumeIndex: 0,
				},
				AbsoluteFilepaths:  []string{"checkpoint"},
				CachedSnapshotTime: start.Add(time.Minute),
			},
		}, nil
	}

	mockReader := fs.NewMockDataFileSetReader(ctrl)
	mockReader.EXPECT().Open(fs.ReaderOpenOptionsMatcher{
		ID: fs.FileSetFileIdentifier{
			Namespace:   testNamespaceID,
			BlockStart:  start,
			Shard:       0,
			VolumeIndex: 0,
		},
		FileSetType: persist.FileSetSnapshotType,
	}).Return(nil).AnyTimes()
	mockReader.EXPECT().Entries().Return(1).AnyTimes()

	snapshotValues := []testValue{
		{foo, start.Add(1 * time.Minute), 1.0, xtime.Nanosecond, nil},
	}

	encoder := m3tsz.NewEncoder(snapshotValues[0].t, nil, true, nil)
	for _, value := range snapshotValues {
		dp := ts.Datapoint{
			Timestamp: value.t,
			Value:     value.v,
		}
		encoder.Encode(dp, value.u, value.a)
	}
	reader := encoder.Stream()
	seg, err := reader.Segment()
	require.NoError(t, err)
	bytes := make([]byte, seg.Len())
	_, err = reader.Read(bytes)
	require.NoError(t, err)
	mockReader.EXPECT().Read().Return(
		foo.ID,
		ident.EmptyTagIterator,
		checked.NewBytes(bytes, nil),
		digest.Checksum(bytes),
		nil,
	)
	mockReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), io.EOF)

	src.newReaderFn = func(bytesPool pool.CheckedBytesPool, opts fs.Options) (fs.DataFileSetReader, error) {
		return mockReader, nil
	}

	targetRanges := result.ShardTimeRanges{0: ranges}
	res, err := src.ReadData(md, targetRanges, testDefaultRunOpts)
	require.NoError(t, err)
	require.NotNil(t, res)
	require.Equal(t, 1, len(res.ShardResults()))
	require.Equal(t, 0, len(res.Unfulfilled()))
	expectedValues := append([]testValue{}, snapshotValues...)
	expectedValues = append(expectedValues, commitLogValues[0:3]...)

	require.NoError(t, verifyShardResultsAreCorrect(
		expectedValues, blockSize, res.ShardResults(), opts))
}

type testValue struct {
	s commitlog.Series
	t time.Time
	v float64
	u xtime.Unit
	a ts.Annotation
}

type seriesShardResultBlock struct {
	encoder encoding.Encoder
}

type seriesShardResult struct {
	blocks map[xtime.UnixNano]*seriesShardResultBlock
	result block.DatabaseSeriesBlocks
}

func verifyShardResultsAreCorrect(
	values []testValue,
	blockSize time.Duration,
	actual result.ShardResults,
	opts Options,
) error {
	if actual == nil {
		if len(values) == 0 {
			return nil
		}

		return fmt.Errorf(
			"shard result is nil, but expected: %d values", len(values))
	}
	// First create what result should be constructed for test values
	expected, err := createExpectedShardResult(values, blockSize, actual, opts)
	if err != nil {
		return err
	}

	// Assert the values
	if len(expected) != len(actual) {
		return fmt.Errorf(
			"number of shards do not match, expected: %d, but got: %d",
			len(expected), len(actual),
		)
	}

	for shard, expectedResult := range expected {
		actualResult, ok := actual[shard]
		if !ok {
			return fmt.Errorf("shard: %d present in expected, but not actual", shard)
		}

		err = verifyShardResultsAreEqual(opts, shard, actualResult, expectedResult)
		if err != nil {
			return err
		}
	}
	return nil
}

func createExpectedShardResult(
	values []testValue,
	blockSize time.Duration,
	actual result.ShardResults,
	opts Options,
) (result.ShardResults, error) {
	bopts := opts.ResultOptions()
	blopts := bopts.DatabaseBlockOptions()

	expected := result.ShardResults{}

	// Sort before iterating to ensure encoding to blocks is correct order
	sort.Stable(testValuesByTime(values))

	allResults := make(map[string]*seriesShardResult)
	for _, v := range values {
		shardResult, ok := expected[v.s.Shard]
		if !ok {
			shardResult = result.NewShardResult(0, bopts)
			expected[v.s.Shard] = shardResult
		}
		_, exists := shardResult.AllSeries().Get(v.s.ID)
		if !exists {
			// Trigger blocks to be created for series
			shardResult.AddSeries(v.s.ID, v.s.Tags, nil)
		}

		series, _ := shardResult.AllSeries().Get(v.s.ID)
		blocks := series.Blocks
		blockStart := v.t.Truncate(blockSize)

		r, ok := allResults[v.s.ID.String()]
		if !ok {
			r = &seriesShardResult{
				blocks: make(map[xtime.UnixNano]*seriesShardResultBlock),
				result: blocks,
			}
			allResults[v.s.ID.String()] = r
		}

		b, ok := r.blocks[xtime.ToUnixNano(blockStart)]
		if !ok {
			encoder := bopts.DatabaseBlockOptions().EncoderPool().Get()
			encoder.Reset(v.t, 0)
			b = &seriesShardResultBlock{
				encoder: encoder,
			}
			r.blocks[xtime.ToUnixNano(blockStart)] = b
		}

		err := b.encoder.Encode(ts.Datapoint{
			Timestamp: v.t,
			Value:     v.v,
		}, v.u, v.a)
		if err != nil {
			return expected, err
		}
	}

	for _, r := range allResults {
		for start, blockResult := range r.blocks {
			enc := blockResult.encoder
			bl := block.NewDatabaseBlock(start.ToTime(), blockSize, enc.Discard(), blopts)
			if r.result != nil {
				r.result.AddBlock(bl)
			}
		}

	}

	return expected, nil
}

func verifyShardResultsAreEqual(opts Options, shard uint32, actualResult, expectedResult result.ShardResult) error {
	expectedSeries := expectedResult.AllSeries()
	actualSeries := actualResult.AllSeries()
	if expectedSeries.Len() != actualSeries.Len() {
		return fmt.Errorf(
			"different number of series for shard: %v . expected: %d , actual: %d",
			shard,
			expectedSeries.Len(),
			actualSeries.Len(),
		)
	}

	for _, entry := range expectedSeries.Iter() {
		expectedID, expectedBlocks := entry.Key(), entry.Value()
		actualBlocks, ok := actualSeries.Get(expectedID)
		if !ok {
			return fmt.Errorf("series: %v present in expected but not actual", expectedID)
		}

		if !expectedBlocks.Tags.Equal(actualBlocks.Tags) {
			return fmt.Errorf(
				"series: %v present in expected and actual, but tags do not match", expectedID)
		}

		expectedAllBlocks := expectedBlocks.Blocks.AllBlocks()
		actualAllBlocks := actualBlocks.Blocks.AllBlocks()
		if len(expectedAllBlocks) != len(actualAllBlocks) {
			return fmt.Errorf(
				"number of expected blocks: %d does not match number of actual blocks: %d for series: %s",
				len(expectedAllBlocks),
				len(actualAllBlocks),
				expectedID,
			)
		}

		err := verifyBlocksAreEqual(opts, expectedAllBlocks, actualAllBlocks)
		if err != nil {
			return err
		}
	}

	return nil
}

func verifyBlocksAreEqual(opts Options, expectedAllBlocks, actualAllBlocks map[xtime.UnixNano]block.DatabaseBlock) error {
	blopts := opts.ResultOptions().DatabaseBlockOptions()
	for start, expectedBlock := range expectedAllBlocks {
		actualBlock, ok := actualAllBlocks[start]
		if !ok {
			return fmt.Errorf("Expected block for start time: %v", start)
		}

		ctx := blopts.ContextPool().Get()
		defer ctx.Close()

		expectedStream, expectedStreamErr := expectedBlock.Stream(ctx)
		if expectedStreamErr != nil {
			return fmt.Errorf("err creating expected stream: %s", expectedStreamErr.Error())
		}

		actualStream, actualStreamErr := actualBlock.Stream(ctx)
		if actualStreamErr != nil {
			return fmt.Errorf("err creating actual stream: %s", actualStreamErr.Error())
		}

		readerIteratorPool := blopts.ReaderIteratorPool()

		expectedIter := readerIteratorPool.Get()
		expectedIter.Reset(expectedStream)
		defer expectedIter.Close()

		actualIter := readerIteratorPool.Get()
		actualIter.Reset(actualStream)
		defer actualIter.Close()

		for {
			expectedNext := expectedIter.Next()
			actualNext := actualIter.Next()
			if !expectedNext && !actualNext {
				break
			}

			if !(expectedNext && actualNext) {
				return fmt.Errorf(
					"err: expectedNext was: %v, but actualNext was: %v",
					expectedNext,
					actualNext,
				)
			}

			expectedValue, expectedUnit, expectedAnnotation := expectedIter.Current()
			actualValue, actualUnit, actualAnnotation := actualIter.Current()

			if expectedValue.Timestamp != actualValue.Timestamp {
				return fmt.Errorf(
					"expectedValue.Timestamp was: %v, but actualValue.Timestamp was: %v",
					expectedValue.Timestamp,
					actualValue.Timestamp,
				)
			}

			if expectedValue.Value != actualValue.Value {
				return fmt.Errorf(
					"expectedValue.Value was: %v, but actualValue.Value was: %v",
					expectedValue.Value,
					actualValue.Value,
				)
			}

			if expectedUnit != actualUnit {
				return fmt.Errorf(
					"expectedUnit was: %v, but actualUnit was: %v",
					expectedUnit,
					actualUnit,
				)
			}

			if !reflect.DeepEqual(expectedAnnotation, actualAnnotation) {
				return fmt.Errorf(
					"expectedAnnotation was: %v, but actualAnnotation was: %v",
					expectedAnnotation,
					actualAnnotation,
				)
			}
		}
	}

	return nil
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
	return i.idx < len(i.values)
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
