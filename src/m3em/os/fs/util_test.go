// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	testingPrefixDir = "fs-testing-"
)

func newTempDir(t *testing.T) string {
	dir, err := ioutil.TempDir("", testingPrefixDir)
	require.NoError(t, err)
	return dir
}

func newTempFile(t *testing.T, dir string, content []byte) *os.File {
	tmpfile, err := ioutil.TempFile(dir, "example")
	require.NoError(t, err)
	n, err := tmpfile.Write(content)
	require.NoError(t, err)
	require.Equal(t, len(content), n)
	require.NoError(t, tmpfile.Close())
	return tmpfile
}

func TestRemoveContents(t *testing.T) {
	var (
		content        = []byte("temporary file content")
		tmpdir         = newTempDir(t)
		numFilesToTest = 3
	)

	// create a random files
	for i := 0; i < numFilesToTest; i++ {
		tmpfile := newTempFile(t, tmpdir, content)
		tmpfile.Close()
	}
	files, err := ioutil.ReadDir(tmpdir)
	require.NoError(t, err)
	require.Equal(t, numFilesToTest, len(files))

	// remove contents of dir, list to make sure nothing is in there
	require.NoError(t, RemoveContents(tmpdir))
	fi, err := os.Stat(tmpdir)
	require.NoError(t, err)
	require.True(t, fi.IsDir())
	files, err = ioutil.ReadDir(tmpdir)
	require.NoError(t, err)
	require.Equal(t, 0, len(files))
}
