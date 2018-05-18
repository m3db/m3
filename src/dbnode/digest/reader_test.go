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

package digest

import (
	"errors"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testReaderBufferSize = 10
)

func createTestFdWithDigestReader(t *testing.T) (*fdWithDigestReader, *os.File, *mockDigest) {
	fd, md := createTestFdWithDigest(t)
	reader := NewFdWithDigestReader(testReaderBufferSize).(*fdWithDigestReader)
	reader.readerWithDigest.(*readerWithDigest).digest = md
	reader.Reset(fd)
	return reader, fd, md
}

func createTestFdWithDigestContentsReader(t *testing.T) (*fdWithDigestContentsReader, *os.File, *mockDigest) {
	fdr, fd, md := createTestFdWithDigestReader(t)
	reader := NewFdWithDigestContentsReader(testReaderBufferSize).(*fdWithDigestContentsReader)
	reader.FdWithDigestReader = fdr
	return reader, fd, md
}

func bufferFor(t *testing.T, fd *os.File) []byte {
	stat, err := fd.Stat()
	require.NoError(t, err)
	size := int(stat.Size())
	return make([]byte, size)
}

func TestFdWithDigestReaderReset(t *testing.T) {
	reader, _, _ := createTestFdWithDigestReader(t)
	require.NotNil(t, reader.Fd())
	reader.Reset(nil)
	require.Nil(t, reader.Fd())
}

func TestFdWithDigestReadBytesFileReadError(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	toRead := make([]byte, 1)
	_, err := reader.Read(toRead)
	require.Error(t, err)
}

func TestFdWithDigestReadBytesDigestWriteError(t *testing.T) {
	reader, fd, md := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := make([]byte, 1)
	_, err := fd.Write(b)
	require.NoError(t, err)
	_, err = fd.Seek(-1, 1)
	require.NoError(t, err)

	md.err = errors.New("foo")
	reader.Reset(fd)
	_, err = reader.Read(b)
	require.Equal(t, "foo", err.Error())
}

func TestFdWithDigestReadBytesSuccess(t *testing.T) {
	reader, fd, md := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)

	n, err := reader.Read(b)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, b, md.b)
}

func TestFdWithDigestReadBytesBuffered(t *testing.T) {
	reader, fd, md := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := make([]byte, 154)
	b[153] = byte(1)
	fd.Write(b)
	fd.Seek(0, 0)

	for _, readSize := range []int{27, 127} {
		toRead := make([]byte, readSize)
		n, err := reader.Read(toRead)
		require.NoError(t, err)
		require.Equal(t, readSize, n)
	}

	require.Equal(t, b, md.b)
}

func TestFdWithDigestReadAllFileReadError(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	reader.Reset(nil)
	buf := bufferFor(t, fd)
	_, err := reader.ReadAllAndValidate(buf, 0)
	require.Error(t, err)
}

func testFdWithDigestReadAllValidation(t *testing.T, expectedDigest uint32, expectedErr error) {
	reader, fd, md := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)

	md.digest = 1
	buf := bufferFor(t, fd)
	_, err := reader.ReadAllAndValidate(buf, expectedDigest)
	require.Equal(t, expectedErr, err)
}

func TestFdWithDigestReadAllValidationError(t *testing.T) {
	testFdWithDigestReadAllValidation(t, 2, errChecksumMismatch)
}

func TestFdWithDigestReadAllValidationSuccess(t *testing.T) {
	testFdWithDigestReadAllValidation(t, 1, nil)
}

func TestFdWithDigestValidateDigest(t *testing.T) {
	reader := NewFdWithDigestReader(testReaderBufferSize).(*fdWithDigestReader)
	reader.readerWithDigest.(*readerWithDigest).digest = &mockDigest{digest: 123}
	require.NoError(t, reader.Validate(123))
	require.Equal(t, errChecksumMismatch, reader.Validate(100))
}

func TestFdWithDigestReaderClose(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	require.NotNil(t, reader.Fd())
	require.NoError(t, reader.Close())
	require.Nil(t, reader.Fd())
}

func TestFdWithDigestReadDigestReadError(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestContentsReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	reader.Reset(nil)
	_, err := reader.ReadDigest()
	require.Error(t, err)
}

func TestFdWithDigestReadDigestTooFewBytes(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestContentsReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)

	_, err := reader.ReadDigest()
	require.Equal(t, errReadFewerThanExpectedBytes, err)
}

func TestFdWithDigestReadDigestSuccess(t *testing.T) {
	reader, fd, _ := createTestFdWithDigestContentsReader(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0xa, 0x0, 0x0, 0x0}
	fd.Write(b)
	fd.Seek(0, 0)

	res, err := reader.ReadDigest()
	require.NoError(t, err)
	require.Equal(t, uint32(10), res)
}
