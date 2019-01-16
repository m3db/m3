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

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestSeeker(filePathPrefix string) DataFileSetSeeker {
	bytesPool := pool.NewCheckedBytesPool([]pool.Bucket{pool.Bucket{
		Capacity: 1024,
		Count:    10,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	bytesPool.Init()
	return NewSeeker(filePathPrefix, testReaderBufferSize, testReaderBufferSize,
		testReaderBufferSize, bytesPool, false, nil, testDefaultOpts)
}

func TestSeekEmptyIndex(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart)
	assert.NoError(t, err)
	assert.Equal(t, 0, s.Entries())
	_, err = s.SeekByID(ident.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSeekIDNotFound, err)
	assert.NoError(t, s.Close())
}

// func TestSeekDataUnexpectedSize(t *testing.T) {
// 	dir, err := ioutil.TempDir("", "testdb")
// 	if err != nil {
// 		t.Fatal(err)
// 	}
// 	filePathPrefix := filepath.Join(dir, "")
// 	defer os.RemoveAll(dir)

// 	w := newTestWriter(t, filePathPrefix)
// 	writerOpts := DataWriterOpenOptions{
// 		BlockSize: testBlockSize,
// 		Identifier: FileSetFileIdentifier{
// 			Namespace:  testNs1ID,
// 			Shard:      0,
// 			BlockStart: testWriterStart,
// 		},
// 	}
// 	err = w.Open(writerOpts)
// 	assert.NoError(t, err)
// 	dataFile := w.(*writer).dataFdWithDigest.Fd().Name()

// 	assert.NoError(t, w.Write(
// 		ident.StringID("foo"), ident.Tags{},
// 		bytesRefd([]byte{1, 2, 3}),
// 		digest.Checksum([]byte{1, 2, 3})))
// 	assert.NoError(t, w.Close())

// 	// Truncate one byte
// 	assert.NoError(t, os.Truncate(dataFile, 1))

// 	s := newTestSeeker(filePathPrefix)
// 	err = s.Open(testNs1ID, 0, testWriterStart)
// 	assert.NoError(t, err)

// 	_, err = s.SeekByID(ident.StringID("foo"))
// 	assert.Error(t, err)
// 	assert.Equal(t, errNotEnoughBytes, err)

// 	assert.NoError(t, s.Close())
// }

func TestSeekBadChecksum(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)

	// Write data with wrong checksum
	assert.NoError(t, w.Write(
		ident.StringID("foo"), ident.Tags{},
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 4})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart)
	assert.NoError(t, err)

	_, err = s.SeekByID(ident.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSeekChecksumMismatch, err)

	assert.NoError(t, s.Close())
}

// TestSeek is a basic sanity test that we can seek IDs that have been written,
// as well as received errSeekIDNotFound for IDs that were not written.
func TestSeek(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo1"),
		ident.NewTags(ident.StringTag("num", "1")),
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Write(
		ident.StringID("foo2"),
		ident.NewTags(ident.StringTag("num", "2")),
		bytesRefd([]byte{1, 2, 2}),
		digest.Checksum([]byte{1, 2, 2})))
	assert.NoError(t, w.Write(
		ident.StringID("foo3"),
		ident.NewTags(ident.StringTag("num", "3")),
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart)
	assert.NoError(t, err)

	data, err := s.SeekByID(ident.StringID("foo3"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 3}, data.Bytes())

	data, err = s.SeekByID(ident.StringID("foo1"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 1}, data.Bytes())

	_, err = s.SeekByID(ident.StringID("foo"))
	assert.Error(t, err)
	assert.Equal(t, errSeekIDNotFound, err)

	data, err = s.SeekByID(ident.StringID("foo2"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 2}, data.Bytes())

	assert.NoError(t, s.Close())
}

// TestSeekIDNotExists is similar to TestSeek, but it covers more edge cases
// around IDs not existing.
func TestSeekIDNotExists(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo10"), ident.Tags{},
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Write(
		ident.StringID("foo20"), ident.Tags{},
		bytesRefd([]byte{1, 2, 2}),
		digest.Checksum([]byte{1, 2, 2})))
	assert.NoError(t, w.Write(
		ident.StringID("foo30"), ident.Tags{},
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart)
	assert.NoError(t, err)

	// Test errSeekIDNotFound when we scan far enough into the index file that
	// we're sure that the ID we're looking for doesn't exist (because the index
	// file is sorted). In this particular case, we would know foo21 doesn't exist
	// once we've scanned all the way to foo30 (which does exist).
	_, err = s.SeekByID(ident.StringID("foo21"))
	assert.Equal(t, errSeekIDNotFound, err)

	// Test errSeekIDNotFound when we scan to the end of the index file (foo40
	// would be located at the end of the index file based on the writes we've made)
	_, err = s.SeekByID(ident.StringID("foo40"))
	assert.Equal(t, errSeekIDNotFound, err)

	assert.NoError(t, s.Close())
}

func TestReuseSeeker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)

	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart.Add(-time.Hour),
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo"), ident.Tags{},
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Close())

	writerOpts = DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo"), ident.Tags{},
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart.Add(-time.Hour))
	assert.NoError(t, err)

	data, err := s.SeekByID(ident.StringID("foo"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 1}, data.Bytes())

	err = s.Open(testNs1ID, 0, testWriterStart)
	assert.NoError(t, err)

	data, err = s.SeekByID(ident.StringID("foo"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 3}, data.Bytes())
}

func TestCloneSeeker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w := newTestWriter(t, filePathPrefix)

	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart.Add(-time.Hour),
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo"), ident.Tags{},
		bytesRefd([]byte{1, 2, 1}),
		digest.Checksum([]byte{1, 2, 1})))
	assert.NoError(t, w.Close())

	writerOpts = DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Write(
		ident.StringID("foo"), ident.Tags{},
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	s := newTestSeeker(filePathPrefix)
	err = s.Open(testNs1ID, 0, testWriterStart.Add(-time.Hour))
	assert.NoError(t, err)

	clone, err := s.ConcurrentClone()
	require.NoError(t, err)

	data, err := clone.SeekByID(ident.StringID("foo"))
	require.NoError(t, err)

	data.IncRef()
	defer data.DecRef()
	assert.Equal(t, []byte{1, 2, 1}, data.Bytes())
}
