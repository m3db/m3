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
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs/msgpack"
	"github.com/m3db/m3/src/dbnode/persist/schema"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/mmap"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testReaderBufferSize = 10
	testWriterBufferSize = 10
)

var (
	testWriterStart    = xtime.Now()
	testBlockSize      = 2 * time.Hour
	testDefaultOpts    = NewOptions() // To avoid allocing pools each test exec
	testBytesPool      pool.CheckedBytesPool
	testTagDecoderPool serialize.TagDecoderPool
)

// NB(r): This is kind of brittle, but basically msgpack expects a buffered
// reader, but we can't use a buffered reader because we need to know where
// the position of the decoder is when we need to grab bytes without copying.
//
// This var declaration by it successfully compiling means it implements the
// `bufReader` interface in msgpack decoder library (unless it changes...)
// in which case this needs to be updated.
//
// By it implementing the interface the msgpack decoder actually uses
// the reader directly without creating a buffered reader to wrap it.
// This way we can know actually where its position is and can correctly
// take a valid bytes ref address when reading bytes without copying.
//
// We're attempting to address this by making it less brittle but the author
// is not currently supportive of merging the changes:
// https://github.com/vmihailenco/msgpack/pull/155
var _ = msgpackBufReader(newReaderDecoderStream())

type msgpackBufReader interface {
	Read([]byte) (int, error)
	ReadByte() (byte, error)
	UnreadByte() error
}

func init() {
	testBytesPool = pool.NewCheckedBytesPool([]pool.Bucket{{
		Capacity: 1024,
		Count:    10,
	}}, nil, func(s []pool.Bucket) pool.BytesPool {
		return pool.NewBytesPool(s, nil)
	})
	testBytesPool.Init()
	testTagDecoderPool = serialize.NewTagDecoderPool(
		serialize.NewTagDecoderOptions(serialize.TagDecoderOptionsConfig{}),
		pool.NewObjectPoolOptions())
	testTagDecoderPool.Init()
}

func newTestReader(t *testing.T, filePathPrefix string) DataFileSetReader {
	reader, err := NewReader(testBytesPool, testDefaultOpts.
		SetFilePathPrefix(filePathPrefix).
		SetInfoReaderBufferSize(testReaderBufferSize).
		SetDataReaderBufferSize(testReaderBufferSize))
	require.NoError(t, err)
	return reader
}

func bytesRefd(data []byte) checked.Bytes {
	bytes := checked.NewBytes(data, nil)
	bytes.IncRef()
	return bytes
}

func TestReadEmptyIndexUnreadData(t *testing.T) {
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

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = r.Open(rOpenOpts)
	assert.NoError(t, err)

	_, _, _, _, err = r.Read()
	assert.Equal(t, io.EOF, err)

	assert.NoError(t, r.Close())
}

func TestReadDataError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

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
	metadata := persist.NewMetadataFromIDAndTags(
		ident.StringID("foo"),
		ident.Tags{},
		persist.MetadataOptions{})
	err = w.Open(writerOpts)
	require.NoError(t, err)
	require.NoError(t, w.Write(metadata,
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	require.NoError(t, w.Close())

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = r.Open(rOpenOpts)
	assert.NoError(t, err)

	// Close out the dataFd and use a mock to expect an error on next read
	reader := r.(*reader)
	require.NoError(t, mmap.Munmap(reader.dataMmap))
	require.NoError(t, reader.dataFd.Close())

	mockReader := digest.NewMockReaderWithDigest(ctrl)
	mockReader.EXPECT().Read(gomock.Any()).Return(0, fmt.Errorf("an error"))
	reader.dataReader = mockReader

	_, _, _, _, err = r.Read()
	assert.Error(t, err)

	// Cleanly close
	require.NoError(t, mmap.Munmap(reader.indexMmap))
	require.NoError(t, reader.indexFd.Close())
}

func TestReadDataUnexpectedSize(t *testing.T) {
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
	metadata := persist.NewMetadataFromIDAndTags(
		ident.StringID("foo"),
		ident.Tags{},
		persist.MetadataOptions{})
	err = w.Open(writerOpts)
	assert.NoError(t, err)
	dataFile := w.(*writer).dataFdWithDigest.Fd().Name()

	assert.NoError(t, w.Write(metadata,
		bytesRefd([]byte{1, 2, 3}),
		digest.Checksum([]byte{1, 2, 3})))
	assert.NoError(t, w.Close())

	// Truncate one bye
	assert.NoError(t, os.Truncate(dataFile, 1))

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      0,
			BlockStart: testWriterStart,
		},
	}
	err = r.Open(rOpenOpts)
	assert.NoError(t, err)

	_, _, _, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, errReadNotExpectedSize, err)

	assert.NoError(t, r.Close())
}

func TestReadNoCheckpointFile(t *testing.T) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	w := newTestWriter(t, filePathPrefix)
	shard := uint32(0)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testWriterStart,
		},
	}
	err := w.Open(writerOpts)
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	var (
		shardDir       = ShardDataDirPath(filePathPrefix, testNs1ID, shard)
		checkpointFile = dataFilesetPathFromTimeAndIndex(shardDir, testWriterStart, 0, checkpointFileSuffix, false)
	)
	exists, err := CompleteCheckpointFileExists(checkpointFile)
	require.NoError(t, err)
	require.True(t, exists)
	os.Remove(checkpointFile)

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: testWriterStart,
		},
	}
	err = r.Open(rOpenOpts)
	require.Equal(t, ErrCheckpointFileNotFound, err)
}

func testReadOpen(t *testing.T, fileData map[string][]byte) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	shard := uint32(0)
	start := xtime.ToUnixNano(time.Unix(1000, 0))
	shardDir := ShardDataDirPath(filePathPrefix, testNs1ID, shard)

	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: start,
		},
	}
	metadata := persist.NewMetadataFromIDAndTags(
		ident.StringID("foo"),
		ident.Tags{},
		persist.MetadataOptions{})
	assert.NoError(t, w.Open(writerOpts))

	assert.NoError(t, w.Write(metadata,
		bytesRefd([]byte{0x1}),
		digest.Checksum([]byte{0x1})))
	assert.NoError(t, w.Close())

	for suffix, data := range fileData {
		digestFile := dataFilesetPathFromTimeAndIndex(shardDir, start, 0, suffix, false)
		fd, err := os.OpenFile(digestFile, os.O_WRONLY|os.O_TRUNC, os.FileMode(0666))
		require.NoError(t, err)
		_, err = fd.Write(data)
		require.NoError(t, err)
		fd.Close()
	}

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: xtime.ToUnixNano(time.Unix(1000, 0)),
		},
	}
	require.Error(t, r.Open(rOpenOpts))
}

func TestReadOpenDigestOfDigestMismatch(t *testing.T) {
	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       {0x1},
			indexFileSuffix:      {0x2},
			dataFileSuffix:       {0x3},
			digestFileSuffix:     {0x2, 0x0, 0x2, 0x0, 0x3, 0x0, 0x3, 0x0, 0x4, 0x0, 0x4, 0x0},
			checkpointFileSuffix: {0x12, 0x0, 0x7a, 0x0},
		},
	)
}

func TestReadOpenInfoDigestMismatch(t *testing.T) {
	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       {0xa},
			indexFileSuffix:      {0x2},
			dataFileSuffix:       {0x3},
			digestFileSuffix:     {0x2, 0x0, 0x2, 0x0, 0x3, 0x0, 0x3, 0x0, 0x4, 0x0, 0x4, 0x0},
			checkpointFileSuffix: {0x13, 0x0, 0x7a, 0x0},
		},
	)
}

func TestReadOpenIndexDigestMismatch(t *testing.T) {
	// Write the correct info digest
	enc := msgpack.NewEncoder()
	require.NoError(t, enc.EncodeIndexInfo(schema.IndexInfo{}))
	b := enc.Bytes()

	// Write the wrong index digest
	buf := digest.NewBuffer()
	buf.WriteDigest(digest.Checksum(b))
	digestOfDigest := append(buf, make([]byte, 8)...)
	buf.WriteDigest(digest.Checksum(digestOfDigest))

	testReadOpen(
		t,
		map[string][]byte{
			infoFileSuffix:       b,
			indexFileSuffix:      {0xa},
			dataFileSuffix:       {0x3},
			digestFileSuffix:     digestOfDigest,
			checkpointFileSuffix: buf,
		},
	)
}

func TestReadValidate(t *testing.T) {
	filePathPrefix := createTempDir(t)
	defer os.RemoveAll(filePathPrefix)

	shard := uint32(0)
	start := xtime.ToUnixNano(time.Unix(1000, 0))
	w := newTestWriter(t, filePathPrefix)
	writerOpts := DataWriterOpenOptions{
		BlockSize: testBlockSize,
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: start,
		},
	}
	metadata := persist.NewMetadataFromIDAndTags(
		ident.StringID("foo"),
		ident.Tags{},
		persist.MetadataOptions{})
	require.NoError(t, w.Open(writerOpts))

	assert.NoError(t, w.Write(metadata,
		bytesRefd([]byte{0x1}),
		digest.Checksum([]byte{0x1})))
	require.NoError(t, w.Close())

	r := newTestReader(t, filePathPrefix)
	rOpenOpts := DataReaderOpenOptions{
		Identifier: FileSetFileIdentifier{
			Namespace:  testNs1ID,
			Shard:      shard,
			BlockStart: start,
		},
	}
	require.NoError(t, r.Open(rOpenOpts))
	_, _, _, _, err := r.Read()
	require.NoError(t, err)

	// Mutate expected data checksum to simulate data corruption
	reader := r.(*reader)
	reader.expectedDataDigest = 0
	require.Error(t, r.Validate())

	require.NoError(t, r.Close())
}

func reads(buf dataFileSetReaderDecoderStream, m int) string {
	var b [1000]byte
	if int(buf.Remaining()) > len(b) {
		panic(fmt.Errorf("cannot read all"))
	}

	nb := 0
	for {
		n, err := buf.Read(b[nb : nb+m])
		nb += n
		if err == io.EOF {
			break
		}
	}
	return string(b[0:nb])
}

func TestDecoderStream(t *testing.T) {
	var texts [31]string
	str := ""
	all := ""
	for i := 0; i < len(texts)-1; i++ {
		texts[i] = str + "\n"
		all += texts[i]
		str += string(rune(i%26 + 'a'))
	}
	texts[len(texts)-1] = all

	buf := newReaderDecoderStream()
	for i := 0; i < len(texts); i++ {
		text := texts[i]
		for j := 1; j <= 8; j++ {
			buf.Reset([]byte(text))
			s := reads(buf, j)
			if s != text {
				t.Errorf("m=%d want=%q got=%q", j, text, s)
			}
		}
	}
}

func TestDecoderStreamSkip(t *testing.T) {
	d := []byte{1, 2, 3, 4, 5}
	expectedDigest := digest.Checksum(d)
	buf := newReaderDecoderStream()
	buf.Reset(d)
	assert.Equal(t, int64(5), buf.Remaining())
	assert.NoError(t, buf.Skip(3))
	assert.Equal(t, int64(2), buf.Remaining())

	p := make([]byte, 2)
	n, err := buf.Read(p)
	assert.Equal(t, 2, n)
	assert.NoError(t, err)
	assert.Equal(t, []byte{4, 5}, p)

	assert.NoError(t, buf.reader().Validate(expectedDigest))
}

func TestDecoderStreamUnreadByte(t *testing.T) {
	segments := []string{"Hello, ", "world"}
	got := ""
	want := strings.Join(segments, "")
	r := newReaderDecoderStream()
	r.Reset([]byte(want))
	// Normal execution.
	for {
		b1, err := r.ReadByte()
		if err != nil {
			if err != io.EOF {
				t.Error("unexpected error on ReadByte:", err)
			}
			break
		}
		got += string(b1)
		// Put it back and read it again.
		if err = r.UnreadByte(); err != nil {
			t.Fatal("unexpected error on UnreadByte:", err)
		}
		b2, err := r.ReadByte()
		if err != nil {
			t.Fatal("unexpected error reading after unreading:", err)
		}
		if b1 != b2 {
			t.Fatalf("incorrect byte after unread: got %q, want %q", b1, b2)
		}
	}
	if got != want {
		t.Errorf("got %q, want %q", got, want)
	}
}

func TestDecoderStreamUnreadByteMultiple(t *testing.T) {
	segments := []string{"Hello, ", "world"}
	data := []byte(strings.Join(segments, ""))
	for n := 0; n <= len(data); n++ {
		r := newReaderDecoderStream()
		r.Reset(data)
		// Read n bytes.
		for i := 0; i < n; i++ {
			b, err := r.ReadByte()
			if err != nil {
				t.Fatalf("n = %d: unexpected error on ReadByte: %v", n, err)
			}
			if b != data[i] {
				t.Fatalf("n = %d: incorrect byte returned from ReadByte: got %q, want %q", n, b, data[i])
			}
		}
		// Unread one byte if there is one.
		if n > 0 {
			remaining := r.Remaining()
			if expect := int64(len(data) - n); remaining != expect {
				t.Errorf("n = %d: unexpected remaining before UnreadByte: got %d, want %d", n, remaining, expect)
			}
			if err := r.UnreadByte(); err != nil {
				t.Errorf("n = %d: unexpected error on UnreadByte: %v", n, err)
			}
			remaining = r.Remaining()
			if expect := int64(len(data) - n + 1); remaining != expect {
				t.Errorf("n = %d: unexpected remaining after UnreadByte: got %d, want %d", n, remaining, expect)
			}
		}
		// Test that we cannot unread any further.
		if err := r.UnreadByte(); err == nil {
			t.Errorf("n = %d: expected error on UnreadByte", n)
		}
		// Test that it can be read back with Read.
		if n > 0 {
			var c [1]byte
			_, err := r.Read(c[:])
			if err != nil {
				t.Errorf("n = %d: unexpected error on Read after UnreadByte: %v", n, err)
			}
			if c[0] != data[n-1] {
				t.Errorf("n = %d: unexpected error on Read after UnreadByte: %v != %v", n, c[0], data[n-1])
			}
		}
	}
}
