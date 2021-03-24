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
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	testNamespaceID    = ident.StringID("commitlog_test_ns")
	testDefaultRunOpts = bootstrap.NewRunOptions().SetInitialTopologyState(&topology.StateSnapshot{})

	shortAnnotation = ts.Annotation("annot")
	longAnnotation  = ts.Annotation(strings.Repeat("x", ts.OptimizedAnnotationLen*3))
)

func testNsMetadata(t *testing.T) namespace.Metadata {
	md, err := namespace.NewMetadata(testNamespaceID, namespace.NewOptions())
	require.NoError(t, err)
	return md
}

func testCache(t *testing.T) bootstrap.Cache {
	cache, err := bootstrap.NewCache(bootstrap.NewCacheOptions().
		SetFilesystemOptions(fs.NewOptions()).
		SetInstrumentOptions(instrument.NewOptions()))
	require.NoError(t, err)

	return cache
}

func TestAvailableEmptyRangeError(t *testing.T) {
	var (
		opts     = testDefaultOpts
		src      = newCommitLogSource(opts, fs.Inspection{})
		res, err = src.AvailableData(testNsMetadata(t), result.NewShardTimeRanges(), testCache(t), testDefaultRunOpts)
	)
	require.NoError(t, err)
	require.True(t, result.NewShardTimeRanges().Equal(res))
}

func TestReadEmpty(t *testing.T) {
	opts := testDefaultOpts

	src := newCommitLogSource(opts, fs.Inspection{})
	md := testNsMetadata(t)
	target := result.NewShardTimeRanges()
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, target, md)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	values, err := tester.EnsureDumpAllForNamespace(md)
	require.NoError(t, err)
	require.Equal(t, 0, len(values))
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestReadOnlyOnce(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	nsCtx := namespace.NewContextFrom(md)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	ranges := xtime.NewRanges(xtime.Range{Start: start, End: end})

	foo := ts.Series{Namespace: nsCtx.ID, Shard: 0, ID: ident.StringID("foo")}
	bar := ts.Series{Namespace: nsCtx.ID, Shard: 1, ID: ident.StringID("bar")}
	baz := ts.Series{Namespace: nsCtx.ID, Shard: 2, ID: ident.StringID("baz")}

	values := testValues{
		{foo, start, 1.0, xtime.Second, shortAnnotation},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, longAnnotation},
		{bar, start.Add(2 * time.Minute), 1.0, xtime.Second, longAnnotation},
		{bar, start.Add(3 * time.Minute), 2.0, xtime.Second, shortAnnotation},
		// "baz" is in shard 2 and should not be returned
		{baz, start.Add(4 * time.Minute), 1.0, xtime.Second, shortAnnotation},
	}

	var commitLogReads int
	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		commitLogReads++
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	targetRanges := result.NewShardTimeRanges().Set(0, ranges).Set(1, ranges)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md)
	defer tester.Finish()

	// simulate 2 passes over the commit log
	for i := 0; i < 2; i++ {
		tester.TestReadWith(src)
		tester.TestUnfulfilledForNamespaceIsEmpty(md)

		read := tester.EnsureDumpWritesForNamespace(md)
		require.Equal(t, 2, len(read))
		enforceValuesAreCorrect(t, values[:4], read)
		tester.EnsureNoLoadedBlocks()
	}

	// commit log should only be iterated over once.
	require.Equal(t, 1, commitLogReads)
}

func TestReadErrorOnNewIteratorError(t *testing.T) {
	opts := testDefaultOpts
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return nil, nil, errors.New("an error")
	}

	ranges := xtime.NewRanges(xtime.Range{Start: time.Now(), End: time.Now().Add(time.Hour)})

	md := testNsMetadata(t)
	target := result.NewShardTimeRanges().Set(0, ranges)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, target, md)
	defer tester.Finish()

	ctx := context.NewBackground()
	defer ctx.Close()

	res, err := src.Read(ctx, tester.Namespaces, tester.Cache)
	require.Error(t, err)
	require.Nil(t, res.Results)
	tester.EnsureNoLoadedBlocks()
	tester.EnsureNoWrites()
}

func TestReadOrderedValues(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	testReadOrderedValues(t, opts, md, nil)
}

func testReadOrderedValues(t *testing.T, opts Options, md namespace.Metadata, setAnn setAnnotation) {
	nsCtx := namespace.NewContextFrom(md)

	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	ranges := xtime.NewRanges(xtime.Range{Start: start, End: end})

	foo := ts.Series{Namespace: nsCtx.ID, Shard: 0, ID: ident.StringID("foo")}
	bar := ts.Series{Namespace: nsCtx.ID, Shard: 1, ID: ident.StringID("bar")}
	baz := ts.Series{Namespace: nsCtx.ID, Shard: 2, ID: ident.StringID("baz")}

	values := testValues{
		{foo, start, 1.0, xtime.Second, longAnnotation},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, shortAnnotation},
		{bar, start.Add(2 * time.Minute), 1.0, xtime.Second, nil},
		{bar, start.Add(3 * time.Minute), 2.0, xtime.Second, nil},
		// "baz" is in shard 2 and should not be returned
		{baz, start.Add(4 * time.Minute), 1.0, xtime.Second, nil},
	}
	if setAnn != nil {
		values = setAnn(values)
	}

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	targetRanges := result.NewShardTimeRanges().Set(0, ranges).Set(1, ranges)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	read := tester.EnsureDumpWritesForNamespace(md)
	require.Equal(t, 2, len(read))
	enforceValuesAreCorrect(t, values[:4], read)
	tester.EnsureNoLoadedBlocks()
}

func TestReadUnorderedValues(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)
	testReadUnorderedValues(t, opts, md, nil)
}

func testReadUnorderedValues(t *testing.T, opts Options, md namespace.Metadata, setAnn setAnnotation) {
	nsCtx := namespace.NewContextFrom(md)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	ranges := xtime.NewRanges(xtime.Range{Start: start, End: end})

	foo := ts.Series{Namespace: nsCtx.ID, Shard: 0, ID: ident.StringID("foo")}

	values := testValues{
		{foo, start.Add(10 * time.Minute), 1.0, xtime.Second, shortAnnotation},
		{foo, start.Add(1 * time.Minute), 2.0, xtime.Second, nil},
		{foo, start.Add(2 * time.Minute), 3.0, xtime.Second, longAnnotation},
		{foo, start.Add(3 * time.Minute), 4.0, xtime.Second, nil},
		{foo, start, 5.0, xtime.Second, nil},
	}
	if setAnn != nil {
		values = setAnn(values)
	}

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	targetRanges := result.NewShardTimeRanges().Set(0, ranges).Set(1, ranges)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	read := tester.EnsureDumpWritesForNamespace(md)
	require.Equal(t, 1, len(read))
	enforceValuesAreCorrect(t, values, read)
	tester.EnsureNoLoadedBlocks()
}

// TestReadHandlesDifferentSeriesWithIdenticalUniqueIndex was added as a
// regression test to make sure that the commit log bootstrapper does not make
// any assumptions about series having a unique index because that only holds
// for the duration that an M3DB node is on, but commit log files can span
// multiple M3DB processes which means that unique indexes could be re-used
// for multiple different series.
func TestReadHandlesDifferentSeriesWithIdenticalUniqueIndex(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)

	nsCtx := namespace.NewContextFrom(md)
	src := newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)

	blockSize := md.Options().RetentionOptions().BlockSize()
	now := time.Now()
	start := now.Truncate(blockSize).Add(-blockSize)
	end := now.Truncate(blockSize)

	ranges := xtime.NewRanges(xtime.Range{Start: start, End: end})

	// All series need to be in the same shard to exercise the regression.
	foo := ts.Series{
		Namespace:   nsCtx.ID,
		Shard:       0,
		ID:          ident.StringID("foo"),
		UniqueIndex: 0,
	}
	bar := ts.Series{
		Namespace:   nsCtx.ID,
		Shard:       0,
		ID:          ident.StringID("bar"),
		UniqueIndex: 0,
	}

	values := testValues{
		{foo, start, 1.0, xtime.Second, nil},
		{bar, start, 2.0, xtime.Second, nil},
	}

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(values, nil), nil, nil
	}

	targetRanges := result.NewShardTimeRanges().Set(0, ranges).Set(1, ranges)
	tester := bootstrap.BuildNamespacesTester(t, testDefaultRunOpts, targetRanges, md)
	defer tester.Finish()

	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	read := tester.EnsureDumpWritesForNamespace(md)
	require.Equal(t, 2, len(read))
	enforceValuesAreCorrect(t, values, read)
	tester.EnsureNoLoadedBlocks()
}

func TestItMergesSnapshotsAndCommitLogs(t *testing.T) {
	opts := testDefaultOpts
	md := testNsMetadata(t)

	testItMergesSnapshotsAndCommitLogs(t, opts, md, nil)
}

func testItMergesSnapshotsAndCommitLogs(t *testing.T, opts Options,
	md namespace.Metadata, setAnn setAnnotation) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		nsCtx     = namespace.NewContextFrom(md)
		src       = newCommitLogSource(opts, fs.Inspection{}).(*commitLogSource)
		blockSize = md.Options().RetentionOptions().BlockSize()
		now       = time.Now()
		start     = now.Truncate(blockSize).Add(-blockSize)
		end       = now.Truncate(blockSize)
		ranges    = xtime.NewRanges()

		foo             = ts.Series{Namespace: nsCtx.ID, Shard: 0, ID: ident.StringID("foo")}
		commitLogValues = testValues{
			{foo, start.Add(2 * time.Minute), 1.0, xtime.Nanosecond, shortAnnotation},
			{foo, start.Add(3 * time.Minute), 2.0, xtime.Nanosecond, nil},
			{foo, start.Add(4 * time.Minute), 3.0, xtime.Nanosecond, longAnnotation},
		}
	)
	if setAnn != nil {
		commitLogValues = setAnn(commitLogValues)
	}

	ranges.AddRange(xtime.Range{
		Start: start,
		End:   end,
	})

	src.newIteratorFn = func(
		_ commitlog.IteratorOpts,
	) (commitlog.Iterator, []commitlog.ErrorWithPath, error) {
		return newTestCommitLogIterator(commitLogValues, nil), nil, nil
	}

	src.snapshotFilesFn = func(
		filePathPrefix string,
		namespace ident.ID,
		shard uint32,
	) (fs.FileSetFilesSlice, error) {
		return fs.FileSetFilesSlice{
			fs.FileSetFile{
				ID: fs.FileSetFileIdentifier{
					Namespace:   namespace,
					BlockStart:  start,
					Shard:       shard,
					VolumeIndex: 0,
				},
				// Make sure path passes the "is snapshot" check in SnapshotTimeAndID method.
				AbsoluteFilePaths:               []string{"snapshots/checkpoint"},
				CachedHasCompleteCheckpointFile: fs.EvalTrue,
				CachedSnapshotTime:              start.Add(time.Minute),
			},
		}, nil
	}

	mockReader := fs.NewMockDataFileSetReader(ctrl)
	mockReader.EXPECT().Open(fs.ReaderOpenOptionsMatcher{
		ID: fs.FileSetFileIdentifier{
			Namespace:   nsCtx.ID,
			BlockStart:  start,
			Shard:       0,
			VolumeIndex: 0,
		},
		FileSetType: persist.FileSetSnapshotType,
	}).Return(nil).AnyTimes()
	mockReader.EXPECT().Entries().Return(1).AnyTimes()
	mockReader.EXPECT().Close().Return(nil).AnyTimes()

	snapshotValues := testValues{
		{foo, start.Add(1 * time.Minute), 1.0, xtime.Nanosecond, nil},
	}
	if setAnn != nil {
		snapshotValues = setAnn(snapshotValues)
	}

	encoderPool := opts.ResultOptions().DatabaseBlockOptions().EncoderPool()
	encoder := encoderPool.Get()
	encoder.Reset(snapshotValues[0].t, 10, nsCtx.Schema)
	for _, value := range snapshotValues {
		dp := ts.Datapoint{
			Timestamp: value.t,
			Value:     value.v,
		}
		encoder.Encode(dp, value.u, value.a)
	}

	ctx := context.NewBackground()
	defer ctx.Close()

	reader, ok := encoder.Stream(ctx)
	require.True(t, ok)

	seg, err := reader.Segment()
	require.NoError(t, err)

	bytes, err := xio.ToBytes(reader)
	require.Equal(t, io.EOF, err)
	require.Equal(t, seg.Len(), len(bytes))

	mockReader.EXPECT().Read().Return(
		foo.ID,
		ident.EmptyTagIterator,
		checked.NewBytes(bytes, nil),
		digest.Checksum(bytes),
		nil,
	)
	mockReader.EXPECT().Read().Return(nil, nil, nil, uint32(0), io.EOF)

	src.newReaderFn = func(
		bytesPool pool.CheckedBytesPool,
		opts fs.Options,
	) (fs.DataFileSetReader, error) {
		return mockReader, nil
	}

	targetRanges := result.NewShardTimeRanges().Set(0, ranges)
	tester := bootstrap.BuildNamespacesTesterWithReaderIteratorPool(
		t,
		testDefaultRunOpts,
		targetRanges,
		opts.ResultOptions().DatabaseBlockOptions().MultiReaderIteratorPool(),
		fs.NewOptions(),
		md,
	)

	defer tester.Finish()
	tester.TestReadWith(src)
	tester.TestUnfulfilledForNamespaceIsEmpty(md)

	// NB: this case is a little tricky in that this test is combining writes
	// that come through both the `LoadBlock()` methods (for snapshotted data),
	// and the `Write()` method  (for data that is not snapshotted) into the
	// namespace data accumulator. Thus writes into the accumulated series should
	// be verified against both of these methods.
	read := tester.EnsureDumpWritesForNamespace(md)
	require.Equal(t, 1, len(read))
	enforceValuesAreCorrect(t, commitLogValues[0:3], read)

	read = tester.EnsureDumpLoadedBlocksForNamespace(md)
	enforceValuesAreCorrect(t, snapshotValues, read)
}

type setAnnotation func(testValues) testValues
type annotationEqual func([]byte, []byte) bool

type testValue struct {
	s ts.Series
	t time.Time
	v float64
	u xtime.Unit
	a ts.Annotation
}

type testValues []testValue

func (v testValues) toDecodedBlockMap() bootstrap.DecodedBlockMap {
	blockMap := make(bootstrap.DecodedBlockMap, len(v))
	for _, bl := range v {
		id := bl.s.ID.String()
		val := series.DecodedTestValue{
			Timestamp:  bl.t,
			Value:      bl.v,
			Unit:       bl.u,
			Annotation: bl.a,
		}

		if values, found := blockMap[id]; found {
			blockMap[id] = append(values, val)
		} else {
			blockMap[id] = bootstrap.DecodedValues{val}
		}
	}

	return blockMap
}

func enforceValuesAreCorrect(
	t *testing.T,
	values testValues,
	actual bootstrap.DecodedBlockMap,
) {
	require.NoError(t, verifyValuesAreCorrect(values, actual))
}

func verifyValuesAreCorrect(
	values testValues,
	actual bootstrap.DecodedBlockMap,
) error {
	expected := values.toDecodedBlockMap()
	return expected.VerifyEquals(actual)
}

type testCommitLogIterator struct {
	values testValues
	idx    int
	err    error
	closed bool
}

func newTestCommitLogIterator(values testValues, err error) *testCommitLogIterator {
	return &testCommitLogIterator{values: values, idx: -1, err: err}
}

func (i *testCommitLogIterator) Next() bool {
	i.idx++
	return i.idx < len(i.values)
}

func (i *testCommitLogIterator) Current() commitlog.LogEntry {
	idx := i.idx
	if idx == -1 {
		idx = 0
	}
	v := i.values[idx]
	return commitlog.LogEntry{
		Series:     v.s,
		Datapoint:  ts.Datapoint{Timestamp: v.t, Value: v.v},
		Unit:       v.u,
		Annotation: v.a,
		Metadata: commitlog.LogEntryMetadata{
			FileReadID:        uint64(idx) + 1,
			SeriesUniqueIndex: v.s.UniqueIndex,
		},
	}
}

func (i *testCommitLogIterator) Err() error {
	return i.err
}

func (i *testCommitLogIterator) Close() {
	i.closed = true
}
