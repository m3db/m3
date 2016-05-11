package fs

import (
	"errors"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"

	"github.com/golang/protobuf/proto"

	"github.com/stretchr/testify/assert"
)

func TestReadEmptyIndexUnreadData(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
	assert.NoError(t, err)
	assert.NoError(t, w.Close())

	r, err := NewReader(filePathPrefix)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, ErrReadIndexEntryZeroSize, err)

	assert.NoError(t, r.Close())
}

func TestReadCorruptIndexEntry(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r, err := NewReader(filePathPrefix)
	assert.NoError(t, err)

	expectedErr := errors.New("synthetic error")
	reader := r.(*reader)
	reader.unmarshal = func(buf []byte, pb proto.Message) error {
		return expectedErr
	}

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)

	assert.NoError(t, r.Close())
}

func TestReadDataError(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
	assert.NoError(t, err)
	dataFile := w.(*writer).dataFd.Name()
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r, err := NewReader(filePathPrefix)
	assert.NoError(t, err)

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

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))

	// Remove a byte
	writer := w.(*writer)
	assert.NoError(t, writer.dataFd.Truncate(int64(writer.currOffset-1)))

	assert.NoError(t, w.Close())

	r, err := NewReader(filePathPrefix)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, ErrReadNotExpectedSize, err)

	assert.NoError(t, r.Close())
}

func TestReadBadMarker(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
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

	r, err := NewReader(filePathPrefix)
	assert.NoError(t, err)

	_, _, err = r.Read()
	assert.Error(t, err)
	assert.Equal(t, ErrReadMarkerNotFound, err)

	assert.NoError(t, r.Close())
}

func TestReadWrongIdx(t *testing.T) {
	dir, err := ioutil.TempDir("", "testdb")
	if err != nil {
		t.Fatal(err)
	}
	filePathPrefix := filepath.Join(dir, "")
	defer os.RemoveAll(dir)

	w, err := NewWriter(filePathPrefix, DefaultWriterOptions())
	assert.NoError(t, err)
	assert.NoError(t, w.Write("foo", []byte{1, 2, 3}))
	assert.NoError(t, w.Close())

	r, err := NewReader(filePathPrefix)
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
