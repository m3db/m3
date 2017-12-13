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
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/m3db/bloom"
	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEntry struct {
	id   string
	data []byte
}

func newTestWriter(t *testing.T, filePathPrefix string) FileSetWriter {
	writer, err := NewWriter(NewOptions().
		SetFilePathPrefix(filePathPrefix).
		SetWriterBufferSize(testWriterBufferSize))
	require.NoError(t, err)
	return writer
}

func writeTestData(t *testing.T, w FileSetWriter, shard uint32, timestamp time.Time, entries []testEntry) {
	err := w.Open(testNs1ID, testBlockSize, shard, timestamp)
	assert.NoError(t, err)

	for i := range entries {
		assert.NoError(t, w.Write(
			ts.StringID(entries[i].id),
			bytesRefd(entries[i].data),
			digest.Checksum(entries[i].data)))
	}
	assert.NoError(t, w.Close())
}

func readTestData(t *testing.T, r FileSetReader, shard uint32, timestamp time.Time, entries []testEntry) {
	err := r.Open(testNs1ID, 0, timestamp)
	require.NoError(t, err)

	require.Equal(t, len(entries), r.Entries())
	require.Equal(t, 0, r.EntriesRead())

	bloomFilter, err := r.ReadBloomFilter()
	assert.NoError(t, err)
	// Make sure the bloom filter doesn't always return true
	assert.False(t, bloomFilter.Test([]byte("some_random_data")))
	expectedM, expectedK := bloom.EstimateFalsePositiveRate(
		uint(len(entries)), defaultIndexBloomFilterFalsePositivePercent)
	assert.Equal(t, expectedK, bloomFilter.K())
	// EstimateFalsePositiveRate always returns at least 1, so skip this check
	// if len entries is 0
	if len(entries) > 0 {
		assert.Equal(t, expectedM, bloomFilter.M())
	}

	for i := 0; i < r.Entries(); i++ {
		idFromRead, data, checksumFromRead, err := r.Read()
		require.NoError(t, err)
		defer idFromRead.Finalize()

		data.IncRef()
		defer data.DecRef()

		assert.Equal(t, entries[i].id, idFromRead.String())
		assert.True(t, bytes.Equal(entries[i].data, data.Get()))
		assert.Equal(t, digest.Checksum(entries[i].data), checksumFromRead)

		assert.Equal(t, i+1, r.EntriesRead())

		idFromReadMetadata, length, checksumFromReadMetadata, err := r.ReadMetadata()
		defer idFromReadMetadata.Finalize()

		assert.NoError(t, err)
		assert.True(t, idFromRead.Equal(idFromReadMetadata))
		assert.Equal(t, checksumFromRead, checksumFromReadMetadata)
		assert.Equal(t, len(entries[i].data), length)

		// Verify that the bloomFilter was bootstrapped properly by making sure it
		// at least contains every ID
		assert.True(t, bloomFilter.Test(idFromRead.Data().Get()))
	}

	assert.NoError(t, r.Close())
}

func TestSimpleReadWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", []byte{1, 2, 3}},
		{"bar", []byte{4, 5, 6}},
		{"baz", make([]byte, 65536)},
		{"cat", make([]byte, 100000)},
		{"echo", []byte{7, 8, 9}},
	}

	w := newTestWriter(t, filePathPrefix)
	writeTestData(t, w, 0, testWriterStart, entries)

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, entries)
}

func TestInfoReadWrite(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", []byte{1, 2, 3}},
		{"bar", []byte{4, 5, 6}},
		{"baz", make([]byte, 65536)},
		{"cat", make([]byte, 100000)},
		{"echo", []byte{7, 8, 9}},
	}

	w := newTestWriter(t, filePathPrefix)
	writeTestData(t, w, 0, testWriterStart, entries)

	infoFiles := ReadInfoFiles(filePathPrefix, testNs1ID, 0, 16, nil)
	require.Equal(t, 1, len(infoFiles))

	infoFile := infoFiles[0]
	require.True(t, testWriterStart.Equal(xtime.FromNanoseconds(infoFile.Start)))
	require.Equal(t, testBlockSize, time.Duration(infoFile.BlockSize))
	require.Equal(t, int64(len(entries)), infoFile.Entries)
}

func TestReusingReaderWriter(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	allEntries := [][]testEntry{
		{
			{"foo", []byte{1, 2, 3}},
			{"bar", []byte{4, 5, 6}},
		},
		{
			{"baz", []byte{7, 8, 9}},
		},
		{},
	}
	w := newTestWriter(t, filePathPrefix)
	for i := range allEntries {
		writeTestData(t, w, 0, testWriterStart.Add(time.Duration(i)*time.Hour), allEntries[i])
	}

	r := newTestReader(t, filePathPrefix)
	for i := range allEntries {
		readTestData(t, r, 0, testWriterStart.Add(time.Duration(i)*time.Hour), allEntries[i])
	}
}

func TestReusingWriterAfterWriteError(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	entries := []testEntry{
		{"foo", []byte{1, 2, 3}},
		{"bar", []byte{4, 5, 6}},
	}
	w := newTestWriter(t, filePathPrefix)
	shard := uint32(0)
	require.NoError(t, w.Open(testNs1ID, testBlockSize, shard, testWriterStart))

	require.NoError(t, w.Write(
		ts.StringID(entries[0].id),
		bytesRefd(entries[0].data),
		digest.Checksum(entries[0].data)))

	// Intentionally force a writer error.
	w.(*writer).err = errors.New("foo")
	require.Equal(t, "foo", w.Write(
		ts.StringID(entries[1].id),
		bytesRefd(entries[1].data),
		digest.Checksum(entries[1].data)).Error())
	w.Close()

	r := newTestReader(t, filePathPrefix)
	require.Equal(t, errCheckpointFileNotFound, r.Open(testNs1ID, shard, testWriterStart))

	// Now reuse the writer and validate the data are written as expected.
	writeTestData(t, w, shard, testWriterStart, entries)
	readTestData(t, r, shard, testWriterStart, entries)
}

func TestWriterOnlyWritesNonNilBytes(t *testing.T) {
	dir := createTempDir(t)
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	checkedBytes := func(b []byte) checked.Bytes {
		r := checked.NewBytes(b, nil)
		r.IncRef()
		return r
	}

	w := newTestWriter(t, filePathPrefix)
	err := w.Open(testNs1ID, testBlockSize, 0, testWriterStart)
	assert.NoError(t, err)

	w.WriteAll(ts.StringID("foo"), []checked.Bytes{
		checkedBytes([]byte{1, 2, 3}),
		nil,
		checkedBytes([]byte{4, 5, 6}),
	}, digest.Checksum([]byte{1, 2, 3, 4, 5, 6}))

	assert.NoError(t, w.Close())

	r := newTestReader(t, filePathPrefix)
	readTestData(t, r, 0, testWriterStart, []testEntry{
		{"foo", []byte{1, 2, 3, 4, 5, 6}},
	})
}
