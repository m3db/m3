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

package fs

import (
	"bufio"
	"bytes"
	"crypto/rand"
	"encoding/json"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/persist"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func shardsSet(shards ...uint32) map[uint32]struct{} {
	r := make(map[uint32]struct{})
	for _, shard := range shards {
		r[shard] = struct{}{}
	}
	return r
}

type indexWriteTestSetup struct {
	now            xtime.UnixNano
	rootDir        string
	filePathPrefix string
	blockSize      time.Duration
	blockStart     xtime.UnixNano
	fileSetID      FileSetFileIdentifier
}

func newIndexWriteTestSetup(t *testing.T) indexWriteTestSetup {
	now := xtime.ToUnixNano(time.Now().UTC())
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	blockSize := 12 * time.Hour
	blockStart := now.Truncate(blockSize)
	fileSetID := FileSetFileIdentifier{
		FileSetContentType: persist.FileSetIndexContentType,
		Namespace:          ident.StringID("metrics"),
		BlockStart:         blockStart,
	}
	return indexWriteTestSetup{
		now:            now,
		rootDir:        dir,
		filePathPrefix: filePathPrefix,
		blockSize:      blockSize,
		blockStart:     blockStart,
		fileSetID:      fileSetID,
	}
}

func (s indexWriteTestSetup) cleanup() {
	os.RemoveAll(s.rootDir)
}

type testIndexReadWriteOptions struct {
	IndexReaderOptions testIndexReaderOptions
}

func TestIndexSimpleReadWrite(t *testing.T) {
	tests := []struct {
		TestOptions testIndexReadWriteOptions
	}{
		{
			TestOptions: testIndexReadWriteOptions{
				IndexReaderOptions: testIndexReaderOptions{
					AutovalidateIndexSegments: true,
				},
			},
		},
		{
			TestOptions: testIndexReadWriteOptions{
				IndexReaderOptions: testIndexReaderOptions{
					AutovalidateIndexSegments: true,
				},
			},
		},
	}

	for _, test := range tests {
		test := test
		name, err := json.Marshal(test)
		require.NoError(t, err)
		t.Run(string(name), func(t *testing.T) {
			testIndexSimpleReadWrite(t, test.TestOptions)
		})
	}
}

func testIndexSimpleReadWrite(t *testing.T, testOpts testIndexReadWriteOptions) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	test := newIndexWriteTestSetup(t)
	defer test.cleanup()

	writer := newTestIndexWriter(t, test.filePathPrefix)
	err := writer.Open(IndexWriterOpenOptions{
		Identifier:  test.fileSetID,
		BlockSize:   test.blockSize,
		FileSetType: persist.FileSetFlushType,
		Shards:      shardsSet(1, 3, 5),
	})
	require.NoError(t, err)

	testSegments := []testIndexSegment{
		{
			segmentType:  idxpersist.IndexSegmentType("fst"),
			majorVersion: 1,
			minorVersion: 2,
			files: []testIndexSegmentFile{
				{idxpersist.IndexSegmentFileType("first"), randDataFactorOfBuffSize(t, 1.5)},
				{idxpersist.IndexSegmentFileType("second"), randDataFactorOfBuffSize(t, 2.5)},
			},
		},
		{
			segmentType:  idxpersist.IndexSegmentType("trie"),
			majorVersion: 3,
			minorVersion: 4,
			files: []testIndexSegmentFile{
				{idxpersist.IndexSegmentFileType("first"), randDataFactorOfBuffSize(t, 1.5)},
				{idxpersist.IndexSegmentFileType("second"), randDataFactorOfBuffSize(t, 2.5)},
				{idxpersist.IndexSegmentFileType("third"), randDataFactorOfBuffSize(t, 3)},
			},
		},
	}
	writeTestIndexSegments(t, ctrl, writer, testSegments)

	err = writer.Close()
	require.NoError(t, err)

	reader := newTestIndexReader(t, test.filePathPrefix,
		testOpts.IndexReaderOptions)
	result, err := reader.Open(IndexReaderOpenOptions{
		Identifier:  test.fileSetID,
		FileSetType: persist.FileSetFlushType,
	})
	require.NoError(t, err)
	require.Equal(t, shardsSet(1, 3, 5), result.Shards)

	readTestIndexSegments(t, ctrl, reader, testSegments)

	err = reader.Validate()
	require.NoError(t, err)

	err = reader.Close()
	require.NoError(t, err)
}

func newTestIndexWriter(t *testing.T, filePathPrefix string) IndexFileSetWriter {
	writer, err := NewIndexWriter(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize))
	require.NoError(t, err)
	return writer
}

type testIndexReaderOptions struct {
	AutovalidateIndexSegments bool
}

func newTestIndexReader(
	t *testing.T,
	filePathPrefix string,
	opts testIndexReaderOptions,
) IndexFileSetReader {
	reader, err := NewIndexReader(testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetIndexReaderAutovalidateIndexSegments(opts.AutovalidateIndexSegments))
	require.NoError(t, err)
	return reader
}

func randDataFactorOfBuffSize(t *testing.T, factor float64) []byte {
	length := int(factor * float64(defaultBufferedReaderSize()))
	buffer := bytes.NewBuffer(nil)
	src := io.LimitReader(rand.Reader, int64(length))
	_, err := io.Copy(buffer, src)
	require.NoError(t, err)
	return buffer.Bytes()
}

type testIndexSegment struct {
	segmentType  idxpersist.IndexSegmentType
	majorVersion int
	minorVersion int
	metadata     []byte
	files        []testIndexSegmentFile
}

type testIndexSegmentFile struct {
	segmentFileType idxpersist.IndexSegmentFileType
	data            []byte
}

func writeTestIndexSegments(
	t *testing.T,
	ctrl *gomock.Controller,
	writer IndexFileSetWriter,
	v []testIndexSegment,
) {
	for _, s := range v {
		fileSet := NewMockIndexSegmentFileSetWriter(ctrl)
		fileSet.EXPECT().SegmentType().Return(s.segmentType).AnyTimes()
		fileSet.EXPECT().MajorVersion().Return(s.majorVersion)
		fileSet.EXPECT().MinorVersion().Return(s.minorVersion)
		fileSet.EXPECT().SegmentMetadata().Return(s.metadata)

		var files []idxpersist.IndexSegmentFileType
		for _, f := range s.files {
			files = append(files, f.segmentFileType)
		}
		fileSet.EXPECT().Files().Return(files).AnyTimes()

		for _, f := range s.files {
			f := f
			// Make sure we're actually trying to test writing out file contents
			require.True(t, len(f.data) > 0)

			fileSet.EXPECT().
				WriteFile(f.segmentFileType, gomock.Any()).
				DoAndReturn(func(_ idxpersist.IndexSegmentFileType, w io.Writer) error {
					_, err := w.Write(f.data)
					return err
				})
		}

		err := writer.WriteSegmentFileSet(fileSet)
		require.NoError(t, err)
	}
}

func readTestIndexSegments(
	t *testing.T,
	ctrl *gomock.Controller,
	reader IndexFileSetReader,
	v []testIndexSegment,
) {
	require.Equal(t, len(v), reader.SegmentFileSets())

	for _, s := range v {
		result, err := reader.ReadSegmentFileSet()
		require.NoError(t, err)

		assert.Equal(t, s.segmentType, result.SegmentType())
		assert.Equal(t, s.majorVersion, result.MajorVersion())
		assert.Equal(t, s.minorVersion, result.MinorVersion())
		assert.Equal(t, s.metadata, result.SegmentMetadata())

		require.Equal(t, len(s.files), len(result.Files()))

		for i, expected := range s.files {
			actual := result.Files()[i]

			require.Equal(t, expected.segmentFileType, actual.SegmentFileType())

			// Assert read data is correct
			actualData, err := ioutil.ReadAll(actual)
			require.NoError(t, err)
			assert.Equal(t, expected.data, actualData)

			// Assert bytes data (should be mmap'd byte slice) is also correct
			directData, err := actual.Mmap()
			require.NoError(t, err)
			assert.True(t, bytes.Equal(expected.data, directData.Bytes))

			err = actual.Close()
			require.NoError(t, err)
		}
	}

	// Ensure last read is io.EOF
	_, err := reader.ReadSegmentFileSet()
	require.Equal(t, io.EOF, err)
}

var (
	defaultBufferedReaderLock sync.Mutex
	defaultBufferedReader     *bufio.Reader
)

func defaultBufferedReaderSize() int {
	// Pre go1.10 it wasn't possible to query the size of the buffered reader
	// however in go1.10 it is possible
	defaultBufferedReaderLock.Lock()
	defer defaultBufferedReaderLock.Unlock()

	if defaultBufferedReader == nil {
		defaultBufferedReader = bufio.NewReader(nil)
	}
	return defaultBufferedReader.Size()
}
