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
	"bufio"
	"errors"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

const (
	testWriterBufferSize = 10
)

func createTestFdWithDigestWriter(t *testing.T) (*fdWithDigestWriter, *os.File, *mockDigest) {
	fd, md := createTestFdWithDigest(t)
	writer := NewFdWithDigestWriter(testWriterBufferSize).(*fdWithDigestWriter)
	writer.FdWithDigest.(*fdWithDigest).digest = md
	writer.writer = bufio.NewWriterSize(nil, 2)
	writer.Reset(fd)
	return writer, fd, md
}

func createTestFdWithDigestContentsWriter(t *testing.T) (*fdWithDigestContentsWriter, *os.File, *mockDigest) {
	fwd, fd, md := createTestFdWithDigestWriter(t)
	writer := NewFdWithDigestContentsWriter(testWriterBufferSize).(*fdWithDigestContentsWriter)
	writer.FdWithDigestWriter = fwd
	return writer, fd, md
}

func TestFdWithDigestWriterReset(t *testing.T) {
	writer, _, _ := createTestFdWithDigestWriter(t)
	require.NotNil(t, writer.Fd())
	writer.Reset(nil)
	require.Nil(t, writer.Fd())
}

func TestFdWithDigestWriteBytesFileWriteError(t *testing.T) {
	writer, fd, _ := createTestFdWithDigestWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	writer.Reset(nil)
	_, err := writer.Write([]byte{0x1, 0x2, 0x3})
	require.Error(t, err)
}

func TestFdWithDigestWriteBytesDigestWriteError(t *testing.T) {
	writer, fd, md := createTestFdWithDigestWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	md.err = errors.New("foo")
	_, err := writer.Write([]byte{0x1, 0x2, 0x3})
	require.Equal(t, "foo", err.Error())
}

func TestFdWithDigestWriteBytesSuccess(t *testing.T) {
	writer, fd, md := createTestFdWithDigestWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	md.digest = 123
	data := []byte{0x1, 0x2, 0x3}
	res, err := writer.Write(data)
	require.NoError(t, err)
	require.Equal(t, len(data), res)
	require.Equal(t, data, md.b)
}

func TestFdWithDigestWriterCloseSuccess(t *testing.T) {
	writer, fd, _ := createTestFdWithDigestWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	require.NotNil(t, writer.Fd())
	require.NoError(t, writer.Close())
	require.Nil(t, writer.Fd())
}

func TestFdWithDigestWriteDigestsError(t *testing.T) {
	writer, fd, _ := createTestFdWithDigestContentsWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	writer.Reset(nil)
	require.Error(t, writer.WriteDigests(1, 2))
}

func TestFdWithDigestWriteDigestsSuccess(t *testing.T) {
	writer, fd, md := createTestFdWithDigestContentsWriter(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	expected := []byte{0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0}
	require.NoError(t, writer.WriteDigests(1, 2))
	require.Equal(t, expected, md.b)

	_, err := fd.Seek(0, 0)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(fd)
	require.NoError(t, err)
	require.Equal(t, expected, b)
}
