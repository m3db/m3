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

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSeeker(filePathPrefix string) FileSetSeeker {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{pool.Bucket{
		Capacity: 1024,
		Count:    10,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	return NewSeeker(filePathPrefix, testReaderBufferSize, bytesPool, nil)
}

func TestSeekEmptyIndex(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)
	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.Equal(t, 0, s.Entries())
	_, err = s.Seek(ts.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSeekIDNotFound, err)
	assert.NoError(t, s.Close())
}

func TestSeekDataUnexpectedSize(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)
	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	dataFile := w.(*writer).dataFdWithDigest.Fd().Name()

	assert.NoError(t, w.Write(
		ts.StringID("foo"),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	// Truncate one byte
	assert.NoError(t, os.Truncate(dataFile, 1))

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	_, err = s.Seek(ts.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errReadNotExpectedSize, err)

	assert.NoError(t, s.Close())
}

func TestSeekBadMarker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)
	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	// Copy the marker out
	actualMarker := make([]byte, markerLen)
	assert.Equal(t, markerLen, copy(actualMarker, marker))

	// Mess up the marker
	marker[0] = marker[0] + 1

	assert.NoError(t, w.Write(
		ts.StringID("foo"),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))

	// Reset the marker
	marker = actualMarker

	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	_, err = s.Seek(ts.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errReadMarkerNotFound, err)

	assert.NoError(t, s.Close())
}

func TestIDs(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)
	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ts.StringID("foo1"),
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Write(
		ts.StringID("foo2"),
		bytesRefd([]byte{1, 2, 2}),
		digest.Checksum([]byte{1, 2, 2})))
	assert.NoError(t, w.Write(
		ts.StringID("foo3"),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	contains := func(list []ts.ID, s ts.ID) bool {
		for _, t := range list {
			if t.Equal(s) {
				return true
			}
		}
		return false
	}
	ids := s.IDs()
	assert.True(t, ids != nil)
	assert.True(t, contains(ids, ts.StringID("foo1")))
	assert.True(t, contains(ids, ts.StringID("foo2")))
	assert.True(t, contains(ids, ts.StringID("foo3")))
}

func TestSeek(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)
	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ts.StringID("foo1"),
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Write(
		ts.StringID("foo2"),
		bytesRefd([]byte{1, 2, 2}),
		digest.Checksum([]byte{1, 2, 2})))
	assert.NoError(t, w.Write(
		ts.StringID("foo3"),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	data, err := s.Seek(ts.StringID("foo3"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 3}, data.Get())

	data, err = s.Seek(ts.StringID("foo1"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 1}, data.Get())

	_, err = s.Seek(ts.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSeekIDNotFound, err)

	data, err = s.Seek(ts.StringID("foo2"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 2}, data.Get())

	assert.NoError(t, s.Close())
}

func TestReuseSeeker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(filePathPrefix)

	err = w.Open(testNamespaceID, 0, testWriterStart.Add(-time.Hour))
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ts.StringID("foo"),
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Close())

	err = w.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ts.StringID("foo"),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNamespaceID, 0, testWriterStart.Add(-time.Hour))
	assert.NoError(t, err)

	data, err := s.Seek(ts.StringID("foo"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 1}, data.Get())

	err = s.Open(testNamespaceID, 0, testWriterStart)
	assert.NoError(t, err)

	data, err = s.Seek(ts.StringID("foo"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 3}, data.Get())
}
