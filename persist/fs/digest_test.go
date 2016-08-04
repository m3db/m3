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
	"errors"
	"hash"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

type mockDigest struct {
	b      []byte
	digest uint32
	err    error
}

func (md *mockDigest) Write(p []byte) (n int, err error) {
	if md.err != nil {
		return 0, md.err
	}
	md.b = append(md.b, p...)
	return len(p), nil
}

func (md *mockDigest) Sum(b []byte) []byte { return nil }
func (md *mockDigest) Reset()              {}
func (md *mockDigest) Size() int           { return 0 }
func (md *mockDigest) BlockSize() int      { return 0 }
func (md *mockDigest) Sum32() uint32       { return md.digest }

func createFdWithDigest(t *testing.T) (fdWithDigest, *os.File, *mockDigest) {
	fd := createTempFile(t)
	md := &mockDigest{}
	fwd := fdWithDigest{fd: fd, digest: md}
	return fwd, fd, md
}

func TestFdWithDigestResetDigest(t *testing.T) {
	fwd := newFdWithDigest()
	require.Equal(t, uint32(1), fwd.digest.Sum32())
	fwd.digest.Write([]byte{0x1})
	require.Equal(t, uint32(0x20002), fwd.digest.Sum32())
	fwd.resetDigest()
	require.Equal(t, uint32(1), fwd.digest.Sum32())
}

func TestFdWithDigestWriteBytesFileWriteError(t *testing.T) {
	fwd := newFdWithDigest()
	_, err := fwd.writeBytes([]byte{0x1})
	require.Error(t, err)
}

func TestFdWithDigestWriteBytesDigestWriteError(t *testing.T) {
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	md.err = errors.New("foo")
	_, err := fwd.writeBytes([]byte{0x1})
	require.Equal(t, "foo", err.Error())
}

func TestFdWithDigestWriteBytesSuccess(t *testing.T) {
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	md.digest = 123
	data := []byte{0x1}
	res, err := fwd.writeBytes(data)
	require.NoError(t, err)
	require.Equal(t, 1, res)
	require.Equal(t, data, md.b)
}

func TestFdWithDigestWriteDigestsError(t *testing.T) {
	buf := make([]byte, digestLen)
	digests := []hash.Hash32{&mockDigest{digest: 1}, &mockDigest{digest: 2}}
	fwd := newFdWithDigest()
	require.Error(t, fwd.writeDigests(buf, digests...))
}

func TestFdWithDigestWriteDigestsSuccess(t *testing.T) {
	buf := make([]byte, digestLen)
	digests := []hash.Hash32{&mockDigest{digest: 1}, &mockDigest{digest: 2}}
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	expected := []byte{0x1, 0x0, 0x0, 0x0, 0x2, 0x0, 0x0, 0x0}
	require.NoError(t, fwd.writeDigests(buf, digests...))
	require.Equal(t, expected, md.b)
	_, err := fd.Seek(0, 0)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(fd)
	require.NoError(t, err)
	require.Equal(t, expected, b)
}

func TestFdWithDigestReadBytesFileReadError(t *testing.T) {
	fwd := newFdWithDigest()
	_, err := fwd.readBytes(nil)
	require.Error(t, err)
}

func TestFdWithDigestReadBytesDigestWriteError(t *testing.T) {
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := make([]byte, 1)
	fd.Write(b)
	fd.Seek(-1, 1)
	md.err = errors.New("foo")
	_, err := fwd.readBytes(b)
	require.Equal(t, "foo", err.Error())
}

func TestFdWithDigestReadBytesSuccess(t *testing.T) {
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)
	n, err := fwd.readBytes(b)
	require.NoError(t, err)
	require.Equal(t, 1, n)
	require.Equal(t, b, md.b)
}

func TestFdWithDigestReadAllFileReadError(t *testing.T) {
	fwd := newFdWithDigest()
	_, err := fwd.readAllAndValidate(0)
	require.Error(t, err)
}

func TestFdWithDigestReadAllValidationError(t *testing.T) {
	fwd, fd, md := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)
	md.digest = 1
	_, err := fwd.readAllAndValidate(2)
	require.Equal(t, errCheckSumMismatch, err)
}

func TestFdWithDigestReadDigestReadError(t *testing.T) {
	fwd := newFdWithDigest()
	_, err := fwd.readDigest(nil)
	require.Error(t, err)
}

func TestFdWithDigestReadDigestTooFewBytes(t *testing.T) {
	fwd, fd, _ := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0x1}
	fd.Write(b)
	fd.Seek(-1, 1)
	_, err := fwd.readDigest(b)
	require.Equal(t, errReadFewerThanExpectedBytes, err)
}

func TestFdWithDigestReadDigestSuccess(t *testing.T) {
	fwd, fd, _ := createFdWithDigest(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0xa, 0x0, 0x0, 0x0}
	fd.Write(b)
	fd.Seek(0, 0)
	res, err := fwd.readDigest(b)
	require.NoError(t, err)
	require.Equal(t, uint32(10), res)
}

func TestWriteDigest(t *testing.T) {
	digest := &mockDigest{digest: 2}
	buf := make([]byte, digestLen)
	writeDigest(digest, buf)
	require.Equal(t, []byte{0x2, 0x0, 0x0, 0x0}, buf)
}

func TestWriteDigestToFile(t *testing.T) {
	fd := createTempFile(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	buf := make([]byte, digestLen)
	digest := &mockDigest{digest: 20}
	require.NoError(t, writeDigestToFile(fd, digest, buf))
}

func TestReadDigest(t *testing.T) {
	require.Equal(t, uint32(0x1000100), readDigest([]byte{0x0, 0x1, 0x0, 0x1}))
}

func TestReadDigestFromFile(t *testing.T) {
	fd := createTempFile(t)
	defer func() {
		fd.Close()
		os.Remove(fd.Name())
	}()

	b := []byte{0xa, 0x0, 0x0, 0x0}
	fd.Write(b)
	fd.Seek(0, 0)
	res, err := readDigestFromFile(fd, b)
	require.NoError(t, err)
	require.Equal(t, uint32(10), res)
}

func TestValidateDigest(t *testing.T) {
	digest := &mockDigest{digest: 123}
	require.NoError(t, validateDigest(123, digest))
	require.Equal(t, errCheckSumMismatch, validateDigest(100, digest))
}
