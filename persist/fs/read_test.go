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
	"hash/adler32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3db/generated/proto/schema"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	testWriterStart = time.Now()
	testBlockSize   = 2 * time.Hour
)

func TestReadEmptyIndexUnreadData(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, errReadIndexEntryZeroSize, err)

	assert.NoError(t, r.Close())
}

func TestReadCorruptIndexEntry(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	assert.NoError(t, err)

	reader := r.(*reader)
	reader.indexUnread = nil

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.NoError(t, r.Close())
}

func TestReadDataError(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)
	dataFile := w.(*writer).dataFdWithDigest.fd.Name()
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)

	// Close out the dataFd and expect an error on next read
	reader := r.(*reader)
	assert.NoError(t, reader.dataFdWithDigest.fd.Close())

	_, _, err = r.Read()
	assert.Error(t, err)

	// Restore the file to cleanly close
	reader.dataFdWithDigest.fd, err = os.Open(dataFile)
	assert.NoError(t, err)

	assert.NoError(t, r.Close())
}

func TestReadDataUnexpectedSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))

	// Remove a byte
	writer := w.(*writer)
	assert.NoError(t, writer.dataFdWithDigest.fd.Truncate(int64(writer.currOffset-1)))

	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, errReadNotExpectedSize, err)

	assert.NoError(t, r.Close())
}

func TestReadBadMarker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)

	// Copy the marker out
	actualMarker := make([]byte, markerLen)
	assert.Equal(t, markerLen, copy(actualMarker, marker))

	// Mess up the marker
	marker[0] = marker[0] + 1

	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))

	// Reset the marker
	marker = actualMarker

	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, errReadMarkerNotFound, err)

	assert.NoError(t, r.Close())
}

func TestReadWrongIdx(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	err = w.Open(0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	assert.NoError(t, err)

	// Replace the expected idx with 123
	entry := &schema.IndexEntry{Idx: 123}
	b, err := proto.Marshal(entry)
	assert.NoError(t, err)
	b = append(proto.EncodeVarint(uint64(len(b))), b...)
	reader := r.(*reader)
	reader.indexUnread = b
	_, _, err = r.Read()
	assert.Error(t, err)

	typedErr, ok := err.(ErrReadWrongIdx)
	assert.True(t, ok)
	if ok {
		assert.NotEmpty(t, typedErr.Error())

		// Want 123
		assert.Equal(t, int64(123), typedErr.ExpectedIdx)
		// Got 0
		assert.Equal(t, int64(0), typedErr.ActualIdx)
	}

	assert.NoError(t, r.Close())
}

func TestReadNoCheckpointFile(t *testing.T) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	shard := uint32(0)
	err := w.Open(shard, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	shardDir := path.Join(filePathPrefix, strconv.Itoa(int(shard)))
	checkpointFile := filepathFromTime(shardDir, testWriterStart, checkpointFileSuffix)
	require.True(t, fileExists(checkpointFile))
	os.Remove(checkpointFile)

	r := NewReader(filePathPrefix)
	err = r.Open(shard, testWriterStart)
	require.Equal(t, errCheckpointFileNotFound, err)
}

func testReadOpen(
	t *testing.T,
	fileData map[string][]byte,
	expectedErr error,
) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	shard := uint32(0)
	start := time.Unix(1000, 0)
	w := NewWriter(testBlockSize, filePathPrefix, nil)
	assert.NoError(t, w.Open(uint32(shard), start))
	assert.NoError(t, w.Write("foo", []byte{0x1}))
	assert.NoError(t, w.Close())

	for suffix, data := range fileData {
		shardDir := path.Join(filePathPrefix, strconv.Itoa(int(shard)))
		digestFile := filepathFromTime(shardDir, start, suffix)
		fd, err := os.OpenFile(digestFile, os.O_WRONLY|os.O_TRUNC, os.FileMode(0666))
		require.NoError(t, err)
		_, err = fd.Write(data)
		require.NoError(t, err)
		fd.Close()
	}

	r := NewReader(filePathPrefix)
	err := r.Open(shard, time.Unix(1000, 0))
	require.Equal(t, expectedErr, err)
}

func TestReadOpenDigestOfDigestMismatch(t *testing.T) {
	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       []byte{0x1},
			indexFileSuffix:      []byte{0x2},
			dataFileSuffix:       []byte{0x3},
			digestFileSuffix:     []byte{0x2, 0x0, 0x2, 0x0, 0x3, 0x0, 0x3, 0x0, 0x4, 0x0, 0x4, 0x0},
			checkpointFileSuffix: []byte{0x12, 0x0, 0x7a, 0x0},
		},
		errCheckSumMismatch,
	)
}

func TestReadOpenInfoDigestMismatch(t *testing.T) {
	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       []byte{0xa},
			indexFileSuffix:      []byte{0x2},
			dataFileSuffix:       []byte{0x3},
			digestFileSuffix:     []byte{0x2, 0x0, 0x2, 0x0, 0x3, 0x0, 0x3, 0x0, 0x4, 0x0, 0x4, 0x0},
			checkpointFileSuffix: []byte{0x13, 0x0, 0x7a, 0x0},
		},
		errCheckSumMismatch,
	)
}

func TestReadOpenIndexDigestMismatch(t *testing.T) {
	b, err := proto.Marshal(&schema.IndexInfo{})
	require.NoError(t, err)
	digest := adler32.New()
	_, err = digest.Write(b)
	require.NoError(t, err)
	buf := make([]byte, digestLen)
	writeDigest(digest, buf)
	digestOfDigest := append(buf, []byte{0x3, 0x0, 0x3, 0x0, 0x4, 0x0, 0x4, 0x0}...)
	digest.Reset()
	_, err = digest.Write(digestOfDigest)
	require.NoError(t, err)
	writeDigest(digest, buf)

	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       b,
			indexFileSuffix:      []byte{0xa},
			dataFileSuffix:       []byte{0x3},
			digestFileSuffix:     digestOfDigest,
			checkpointFileSuffix: buf,
		},
		errCheckSumMismatch,
	)
}

func TestReadValidate(t *testing.T) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	shard := uint32(0)
	start := time.Unix(1000, 0)
	w := NewWriter(testBlockSize, filePathPrefix, nil)
	require.NoError(t, w.Open(shard, start))
	require.NoError(t, w.Write("foo", []byte{0x1}))
	require.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	require.NoError(t, r.Open(shard, start))
	_, _, err := r.Read()
	require.NoError(t, err)

	// Mutate expected data checksum to simulate data corruption
	reader := r.(*reader)
	reader.expectedDataDigest = 0
	require.Equal(t, errCheckSumMismatch, r.Validate())

	require.NoError(t, r.Close())
}
