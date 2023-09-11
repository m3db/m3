// Copyright (c) 2020 Uber Technologies, Inc.
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

package fs

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type testStreamingEntry struct {
	testEntry
	values []float64
}

func newTestStreamingWriter(t *testing.T, filePathPrefix string) StreamingWriter {
	writer, err := NewStreamingWriter(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize))
	require.NoError(t, err)
	return writer
}

func newOpenTestStreamingWriter(
	t *testing.T,
	filePathPrefix string,
	shard uint32,
	timestamp xtime.UnixNano,
	nextVersion int,
	plannedEntries uint,
) StreamingWriter {
	writer := newTestStreamingWriter(t, filePathPrefix)

	writerOpenOpts := StreamingWriterOpenOptions{
		NamespaceID: testNs1ID,
		ShardID:     shard,
		BlockStart:  timestamp,
		BlockSize:   testBlockSize,

		VolumeIndex:         nextVersion,
		PlannedRecordsCount: plannedEntries,
	}
	err := writer.Open(writerOpenOpts)
	require.NoError(t, err)

	return writer
}

func TestIdsMustBeSorted(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testStreamingEntry{
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
	}

	w := newOpenTestStreamingWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	defer w.Close()
	err := streamingWriteTestData(t, w, testWriterStart, entries)
	require.Error(t, err)
	require.Equal(t, "ids must be written in lexicographic order, no duplicates, but got baz followed by bar",
		err.Error())
}

func TestDoubleWritesAreNotAllowed(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testStreamingEntry{
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"baz", nil, nil}, []float64{4.8, 5.2, 6}},
	}

	w := newOpenTestStreamingWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	defer w.Close()
	err := streamingWriteTestData(t, w, testWriterStart, entries)
	require.Error(t, err)
	require.Equal(t, "ids must be written in lexicographic order, no duplicates, but got baz followed by baz",
		err.Error())
}

func TestSimpleReadStreamingWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testStreamingEntry{
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"cat", nil, nil}, []float64{100000}},
		{testEntry{"foo", nil, nil}, []float64{1, 2, 3}},
		{testEntry{"foo+bar=baz,qux=qaz", map[string]string{
			"bar": "baz",
			"qux": "qaz",
		}, nil}, []float64{7, 8, 9}},
	}

	w := newOpenTestStreamingWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	err := streamingWriteTestData(t, w, testWriterStart, entries)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, toTestEntries(entries))

	verifyInfoFile(t, filePathPrefix, testNs1ID, 0, len(entries))
}

func TestReuseStreamingWriter(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries1 := []testStreamingEntry{
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"cat", nil, nil}, []float64{100000}},
		{testEntry{"foo", nil, nil}, []float64{1, 2, 3}},
	}

	entries2 := []testStreamingEntry{
		{testEntry{"bar2", nil, nil}, []float64{24.8, 25.2, 26}},
		{testEntry{"baz2", nil, nil}, []float64{265536}},
		{testEntry{"cat2", nil, nil}, []float64{200000}},
	}

	w := newTestStreamingWriter(t, filePathPrefix)

	writerOpenOpts1 := StreamingWriterOpenOptions{
		NamespaceID:         testNs1ID,
		ShardID:             1,
		BlockStart:          testWriterStart,
		BlockSize:           testBlockSize,
		VolumeIndex:         0,
		PlannedRecordsCount: uint(len(entries1)),
	}
	err := w.Open(writerOpenOpts1)
	require.NoError(t, err)

	err = streamingWriteTestData(t, w, testWriterStart, entries1)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	verifyInfoFile(t, filePathPrefix, testNs1ID, 1, len(entries1))

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 1, testWriterStart, toTestEntries(entries1))

	writerOpenOpts2 := StreamingWriterOpenOptions{
		NamespaceID:         testNs1ID,
		ShardID:             2,
		BlockStart:          testWriterStart,
		BlockSize:           testBlockSize,
		VolumeIndex:         0,
		PlannedRecordsCount: uint(len(entries2)),
	}
	err = w.Open(writerOpenOpts2)
	require.NoError(t, err)

	err = streamingWriteTestData(t, w, testWriterStart, entries2)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	verifyInfoFile(t, filePathPrefix, testNs1ID, 2, len(entries2))

	readTestData(t, r, 2, testWriterStart, toTestEntries(entries2))
}

func TestReadStreamingWriteEmptyFileset(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newOpenTestStreamingWriter(t, filePathPrefix, 0, testWriterStart, 0, 1)
	err := streamingWriteTestData(t, w, testWriterStart, nil)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, nil)
}

func TestReadStreamingWriteReject0PlannedRecordsCount(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir) // nolint: errcheck

	writer, err := NewStreamingWriter(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize))
	require.NoError(t, err)

	writerOpenOpts := StreamingWriterOpenOptions{
		NamespaceID:         testNs1ID,
		BlockSize:           testBlockSize,
		PlannedRecordsCount: 0,
	}
	err = writer.Open(writerOpenOpts)
	require.EqualError(t, err, "PlannedRecordsCount must be positive, got 0")
}

func TestStreamingWriterAbort(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newOpenTestStreamingWriter(t, filePathPrefix, 0, testWriterStart, 0, 1)
	err := streamingWriteTestData(t, w, testWriterStart, nil)
	require.NoError(t, err)
	err = w.Abort()
	require.NoError(t, err)

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = r.Open(rOpenOpts)
	require.Equal(t, ErrCheckpointFileNotFound, err)
}

func streamingWriteTestData(
	t *testing.T,
	w StreamingWriter,
	blockStart xtime.UnixNano,
	entries []testStreamingEntry,
) error {
	return streamingWriteWithVolume(t, w, blockStart, entries)
}

func streamingWriteWithVolume(
	t *testing.T,
	w StreamingWriter,
	blockStart xtime.UnixNano,
	entries []testStreamingEntry,
) error {
	ctx := context.NewBackground()

	encoder := m3tsz.NewEncoder(blockStart, nil, true, encoding.NewOptions())
	defer encoder.Close()
	ctrl := gomock.NewController(t)
	schema := namespace.NewMockSchemaDescr(ctrl)
	encoder.SetSchema(schema)
	var dp ts.Datapoint

	tagEncodingPool := serialize.NewTagEncoderPool(serialize.NewTagEncoderOptions(), pool.NewObjectPoolOptions())
	tagEncodingPool.Init()

	for i := range entries {
		encoder.Reset(blockStart, 0, schema)
		dp.TimestampNanos = blockStart

		for _, v := range entries[i].values {
			dp.Value = v
			if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
				return err
			}

			dp.TimestampNanos = dp.TimestampNanos.Add(10 * time.Minute)
		}

		stream, ok := encoder.Stream(ctx)
		require.True(t, ok)
		segment, err := stream.Segment()
		if err != nil {
			return err
		}
		entries[i].data = append(segment.Head.Bytes(), segment.Tail.Bytes()...)
		dataChecksum := segment.CalculateChecksum()
		stream.Finalize()

		tagsIter := ident.NewTagsIterator(entries[i].Tags())
		tagEncoder := tagEncodingPool.Get()
		err = tagEncoder.Encode(tagsIter)
		require.NoError(t, err)
		encodedTags, _ := tagEncoder.Data()

		data := [][]byte{entries[i].data}

		if err := w.WriteAll(ident.BytesID(entries[i].id), encodedTags.Bytes(), data, dataChecksum); err != nil {
			return err
		}
	}
	return nil
}

func verifyInfoFile(t *testing.T, filePathPrefix string, namespace ident.ID, shard uint32, expectedEntries int) {
	readInfoFileResults := ReadInfoFiles(filePathPrefix, namespace, shard, 16, nil, persist.FileSetFlushType)
	require.Equal(t, 1, len(readInfoFileResults))
	require.NoError(t, readInfoFileResults[0].Err.Error())

	infoFile := readInfoFileResults[0].Info
	require.Equal(t, int64(testWriterStart), infoFile.BlockStart)
	require.Equal(t, testBlockSize, time.Duration(infoFile.BlockSize))
	require.Equal(t, int64(expectedEntries), infoFile.Entries)
}

func toTestEntries(streamingEntries []testStreamingEntry) []testEntry {
	testEntries := make([]testEntry, 0, len(streamingEntries))
	for _, e := range streamingEntries {
		testEntries = append(testEntries, e.testEntry)
	}
	return testEntries
}
