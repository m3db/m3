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
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

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
	dataFile := w.(*writer).dataFd.Name()
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)

	// Close out the dataFd and expect an error on next read
	reader := r.(*reader)
	assert.NoError(t, reader.dataFd.Close())

	_, _, err = r.Read()
	assert.Error(t, err)

	// Restore the file to cleanly close
	reader.dataFd, err = os.Open(dataFile)
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
	assert.NoError(t, writer.dataFd.Truncate(int64(writer.currOffset-1)))

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

	// Replace the idx with 123 on the way out of the read method
	reader := r.(*reader)
	reader.read = func(fd *os.File, buf []byte) (int, error) {
		n, err := fd.Read(buf)
		endianness.PutUint64(buf[markerLen:], uint64(123))
		return n, err
	}
	_, _, err = r.Read()
	assert.Error(t, err)

	typedErr, ok := err.(ErrReadWrongIdx)
	assert.Equal(t, true, ok)
	if ok {
		assert.NotEmpty(t, typedErr.Error())

		// Want 0
		assert.Equal(t, int64(0), typedErr.ExpectedIdx)
		// Got 123
		assert.Equal(t, int64(123), typedErr.ActualIdx)
	}

	assert.NoError(t, r.Close())
}

func TestReadNoCheckpointFile(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	shard := uint32(0)
	err = w.Open(shard, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	shardDir := ShardDirPath(filePathPrefix, shard)
	checkpointFile := filesetPathFromTime(shardDir, testWriterStart, checkpointFileSuffix)
	require.True(t, FileExists(checkpointFile))
	os.Remove(checkpointFile)

	r := NewReader(filePathPrefix)
	err = r.Open(0, testWriterStart)
	require.Equal(t, errCheckpointFileNotFound, err)
}
