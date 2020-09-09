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

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/require"
)

type testLargeTileEntry struct {
	testEntry
	values []float64
}

func newTestLargeTilesWriter(
	t *testing.T,
	filePathPrefix string,
	shard uint32,
	timestamp time.Time,
	nextVersion int,
	plannedEntries uint,
) LargeTilesWriter {
	writer, err := NewLargeTilesWriter(
		LargeTilesWriterOptions{
			NamespaceID: testNs1ID,
			ShardID:     shard,
			BlockStart:  timestamp,
			BlockSize:   testBlockSize,

			VolumeIndex:         nextVersion,
			PlannedRecordsCount: plannedEntries,
			Options: testDefaultOpts.
				SetFilePathPrefix(filePathPrefix).
				SetWriterBufferSize(testWriterBufferSize),
		},
	)
	require.NoError(t, err)
	return writer
}

func TestIdsMustBeSorted(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testLargeTileEntry{
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
	}

	w := newTestLargeTilesWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	defer w.Close()
	err := writeTestLargeTilesData(t, w, testWriterStart, entries)
	require.Error(t, err)
	require.Equal(t, "ids must be written in lexicographic order, no duplicates, but got baz followed by bar",
		err.Error())
}

func TestDoubleWritesAreNotAllowed(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testLargeTileEntry{
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"baz", nil, nil}, []float64{4.8, 5.2, 6}},
	}

	w := newTestLargeTilesWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	defer w.Close()
	err := writeTestLargeTilesData(t, w, testWriterStart, entries)
	require.Error(t, err)
	require.Equal(t, "ids must be written in lexicographic order, no duplicates, but got baz followed by baz",
		err.Error())
}

func TestSimpleLargeTilesReadWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testLargeTileEntry{
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"cat", nil, nil}, []float64{100000}},
		{testEntry{"foo", nil, nil}, []float64{1, 2, 3}},
		{testEntry{"foo+bar=baz,qux=qaz", map[string]string{
			"bar": "baz",
			"qux": "qaz",
		}, nil}, []float64{7, 8, 9}},
	}

	w := newTestLargeTilesWriter(t, filePathPrefix, 0, testWriterStart, 0, 5)
	err := writeTestLargeTilesData(t, w, testWriterStart, entries)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	expectEntries := make([]testEntry, 0, len(entries))
	for _, e := range entries {
		expectEntries = append(expectEntries, e.testEntry)
	}

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, expectEntries)
}

func TestLargeTilesInfoReadWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testLargeTileEntry{
		{testEntry{"bar", nil, nil}, []float64{4.8, 5.2, 6}},
		{testEntry{"baz", nil, nil}, []float64{65536}},
		{testEntry{"cat", nil, nil}, []float64{100000}},
		{testEntry{"foo", nil, nil}, []float64{1, 2, 3}},
	}

	w := newTestLargeTilesWriter(t, filePathPrefix, 0, testWriterStart, 0, 12)
	err := writeTestLargeTilesData(t, w, testWriterStart, entries)
	require.NoError(t, err)
	err = w.Close()
	require.NoError(t, err)

	readInfoFileResults := ReadInfoFiles(filePathPrefix, testNs1ID, 0, 16, nil, persist.FileSetFlushType)
	require.Equal(t, 1, len(readInfoFileResults))
	for _, result := range readInfoFileResults {
		require.NoError(t, result.Err.Error())
	}

	infoFile := readInfoFileResults[0].Info
	require.True(t, testWriterStart.Equal(xtime.FromNanoseconds(infoFile.BlockStart)))
	require.Equal(t, testBlockSize, time.Duration(infoFile.BlockSize))
	require.Equal(t, int64(len(entries)), infoFile.Entries)
}

func writeTestLargeTilesData(
	t *testing.T,
	w LargeTilesWriter,
	blockStart time.Time,
	entries []testLargeTileEntry,
) error {
	return writeTestLargeTilesDataWithVolume(t, w, blockStart, entries)
}

func writeTestLargeTilesDataWithVolume(
	t *testing.T,
	w LargeTilesWriter,
	blockStart time.Time,
	entries []testLargeTileEntry,
) error {
	if err := w.Open(); err != nil {
		return err
	}
	ctx := context.NewContext()

	encoder := m3tsz.NewEncoder(blockStart, nil, true, encoding.NewOptions())
	defer encoder.Close()
	ctrl := gomock.NewController(t)
	schema := namespace.NewMockSchemaDescr(ctrl)
	encoder.SetSchema(schema)
	var dp ts.Datapoint

	for i := range entries {
		encoder.Reset(blockStart, 0, schema)
		dp.Timestamp = blockStart

		for _, v := range entries[i].values {
			dp.Value = v
			if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
				return err
			}

			dp.Timestamp = dp.Timestamp.Add(10 * time.Minute)
		}

		stream, ok := encoder.Stream(ctx)
		require.True(t, ok)
		segment, err := stream.Segment()
		if err != nil {
			return err
		}
		entries[i].data = append(segment.Head.Bytes(), segment.Tail.Bytes()...)
		stream.Finalize()

		tagIter := ident.NewTagsIterator(entries[i].Tags())
		if err := w.Write(ctx, encoder, ident.StringID(entries[i].id), tagIter); err != nil {
			return err
		}
	}
	return nil
}
