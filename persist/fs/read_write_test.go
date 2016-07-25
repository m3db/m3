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

	"github.com/m3db/m3db/interfaces/m3db"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testEntry struct {
	key  string
	data []byte
}

func writeTestData(t *testing.T, w m3db.FileSetWriter, shard uint32, timestamp time.Time, entries []testEntry) {
	err := w.Open(shard, timestamp)
	assert.NoError(t, err)

	for i := range entries {
		assert.NoError(t, w.Write(entries[i].key, entries[i].data))
	}
	assert.NoError(t, w.Close())
}

func readTestData(t *testing.T, r m3db.FileSetReader, shard uint32, timestamp time.Time, entries []testEntry) {
	err := r.Open(0, timestamp)
	assert.NoError(t, err)

	assert.Equal(t, len(entries), r.Entries())
	assert.Equal(t, 0, r.EntriesRead())

	for i := 0; i < r.Entries(); i++ {
		key, data, err := r.Read()
		assert.NoError(t, err)
		assert.Equal(t, entries[i].key, key)
		assert.True(t, bytes.Equal(entries[i].data, data))

		assert.Equal(t, i+1, r.EntriesRead())
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
	}

	w := NewWriter(testBlockSize, filePathPrefix, nil)
	writeTestData(t, w, 0, testWriterStart, entries)

	r := NewReader(filePathPrefix)
	readTestData(t, r, 0, testWriterStart, entries)
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
	w := NewWriter(testBlockSize, filePathPrefix, nil)
	for i := range allEntries {
		writeTestData(t, w, 0, testWriterStart.Add(time.Duration(i)*time.Hour), allEntries[i])
	}

	r := NewReader(filePathPrefix)
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
	w := NewWriter(testBlockSize, filePathPrefix, nil)
	shard := uint32(0)
	require.NoError(t, w.Open(shard, testWriterStart))
	require.NoError(t, w.Write(entries[0].key, entries[0].data))
	// Intentionally force a writer error.
	w.(*writer).err = errors.New("foo")
	require.Equal(t, "foo", w.Write(entries[1].key, entries[1].data).Error())
	w.Close()
	r := NewReader(filePathPrefix)
	require.Equal(t, errCheckpointFileNotFound, r.Open(shard, testWriterStart))

	// Now reuse the writer and validate the data are written as expected.
	writeTestData(t, w, shard, testWriterStart, entries)
	readTestData(t, r, shard, testWriterStart, entries)
}
