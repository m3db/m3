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

package agent

import (
	"io/ioutil"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestMultiWrite(t *testing.T) {
	dir := newTempDir(t)
	defer os.RemoveAll(dir)

	flags := os.O_CREATE | os.O_WRONLY
	fileMode := os.FileMode(0666)
	dirMode := os.ModeDir | os.FileMode(0755)

	paths := []string{"a/b/c", "d/e", "f"}
	fullPaths := []string{}
	for _, p := range paths {
		fullPaths = append(fullPaths, path.Join(dir, p))
	}

	writer, err := newMultiWriter(fullPaths, flags, fileMode, dirMode)
	require.NoError(t, err)

	testBytes := []byte("some random string")
	n, err := writer.write(testBytes)
	require.NoError(t, err)
	require.Equal(t, len(testBytes), n)

	require.NoError(t, writer.Close())

	for _, fp := range fullPaths {
		bytes, err := ioutil.ReadFile(fp)
		require.NoError(t, err)
		require.Equal(t, testBytes, bytes)
	}
}
