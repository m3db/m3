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
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestWriteDigest(t *testing.T) {
	buf := NewBuffer()
	buf.WriteDigest(2)
	require.Equal(t, []byte{0x2, 0x0, 0x0, 0x0}, []byte(buf))
}

func TestWriteDigestToFile(t *testing.T) {
	fd := createTempFile(t)
	filePath := fd.Name()
	defer os.Remove(filePath)

	buf := NewBuffer()
	require.NoError(t, buf.WriteDigestToFile(fd, 20))
	fd.Close()

	fd, err := os.Open(filePath)
	require.NoError(t, err)
	b, err := ioutil.ReadAll(fd)
	require.NoError(t, err)
	fd.Close()
	require.Equal(t, []byte{0x14, 0x0, 0x0, 0x0}, b)
}

func TestReadDigest(t *testing.T) {
	buf := ToBuffer([]byte{0x0, 0x1, 0x0, 0x1, 0x0, 0x1})
	require.Equal(t, uint32(0x1000100), buf.ReadDigest())
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

	buf := NewBuffer()
	res, err := buf.ReadDigestFromFile(fd)
	require.NoError(t, err)
	require.Equal(t, uint32(10), res)
}
