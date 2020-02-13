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
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

type testDir struct {
	name             string
	initialFileCount int
}

func newTestDir(test string, fileCountByExtension map[string]int) (*testDir, error) {
	dir, err := ioutil.TempDir("", fmt.Sprintf("testdir_%s_", strings.ReplaceAll(test, " ", "")))
	if err != nil {
		return nil, err
	}

	fileCount := 0
	for ext, count := range fileCountByExtension {
		for i := 0; i < count; i++ {
			fileName := filepath.Join(dir, fmt.Sprintf("testfile_%d.%s", i, ext))
			tmpfile, err := os.Create(fileName)
			if err != nil {
				os.RemoveAll(dir)
				return nil, err
			}
			tmpfile.Close()
			fileCount++
		}
	}

	return &testDir{
		name:             dir,
		initialFileCount: fileCount,
	}, nil
}

func (d *testDir) FilesByExtension() map[string][]os.FileInfo {
	files, err := ioutil.ReadDir(d.name)
	if err != nil {
		return nil
	}

	filesByExtension := make(map[string][]os.FileInfo, d.initialFileCount)
	for _, f := range files {
		ext := strings.TrimPrefix(filepath.Ext(f.Name()), ".")
		filesByExtension[ext] = append(filesByExtension[ext], f)
	}

	return filesByExtension
}

func (d *testDir) Cleanup() {
	os.RemoveAll(d.name)
}

func TestDelete(t *testing.T) {
	simpleDeleter := NewSimpleFileDeleter()
	efficientDeleter := NewEfficientFileDeleter(2048)

	singleExt := map[string]int{
		".torrent": 123,
	}
	multiExt := map[string]int{
		"tmp": 100,
		"txt": 10,
		"sql": 1,
	}
	expectedMultiEmptyCount := map[string]int{
		"tmp": 0,
		"txt": 0,
		"sql": 0,
	}

	tests := []struct {
		name               string
		deleter            FileDeleter
		pattern            string
		filesByExt         map[string]int
		expectedFilesByExt map[string]int
	}{
		{
			name:               "simple - zero files - all pattern",
			deleter:            simpleDeleter,
			pattern:            "*",
			filesByExt:         map[string]int{},
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:               "efficient - zero files - all pattern",
			deleter:            efficientDeleter,
			pattern:            "*",
			filesByExt:         map[string]int{},
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:               "simple - single extension - all pattern",
			deleter:            simpleDeleter,
			pattern:            "*",
			filesByExt:         singleExt,
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:               "efficient - single extension - all pattern",
			deleter:            efficientDeleter,
			pattern:            "*",
			filesByExt:         singleExt,
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:               "simple - multiple extensions - all pattern",
			deleter:            simpleDeleter,
			pattern:            "*",
			filesByExt:         multiExt,
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:               "efficient - multiple extensions - all pattern",
			deleter:            efficientDeleter,
			pattern:            "*",
			filesByExt:         multiExt,
			expectedFilesByExt: expectedMultiEmptyCount,
		},
		{
			name:       "simple - multiple extensions - single ext pattern",
			deleter:    simpleDeleter,
			pattern:    "*.txt",
			filesByExt: multiExt,
			expectedFilesByExt: map[string]int{
				"tmp": 100,
				// All TXT deleted.
				"txt": 0,
				"sql": 1,
			},
		},
		{
			name:       "efficient - multiple extensions - single ext pattern",
			deleter:    efficientDeleter,
			pattern:    "*.txt",
			filesByExt: multiExt,
			expectedFilesByExt: map[string]int{
				"tmp": 100,
				// All TXT deleted.
				"txt": 0,
				"sql": 1,
			},
		},
		{
			name:       "simple - multiple extensions - single file pattern",
			deleter:    simpleDeleter,
			pattern:    "testfile_3.txt",
			filesByExt: multiExt,
			expectedFilesByExt: map[string]int{
				"tmp": 100,
				// Only 1 TXT deleted.
				"txt": 9,
				"sql": 1,
			},
		},
		{
			name:       "efficient - multiple extensions - single file pattern",
			deleter:    efficientDeleter,
			pattern:    "testfile_3.txt",
			filesByExt: multiExt,
			expectedFilesByExt: map[string]int{
				"tmp": 100,
				// Only 1 TXT deleted.
				"txt": 9,
				"sql": 1,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			dir, err := newTestDir(test.name, test.filesByExt)
			require.NoError(t, err)
			defer dir.Cleanup()

			// Verify no match.
			files := dir.FilesByExtension()
			require.Equal(t, test.filesByExt["tmp"], len(files["tmp"]))
			require.Equal(t, test.filesByExt["txt"], len(files["txt"]))
			require.Equal(t, test.filesByExt["sql"], len(files["sql"]))
			err = test.deleter.Delete(filepath.Join(dir.name, "notexists.tmp"))
			require.NoError(t, err)
			require.Equal(t, test.filesByExt["tmp"], len(files["tmp"]))
			require.Equal(t, test.filesByExt["txt"], len(files["txt"]))
			require.Equal(t, test.filesByExt["sql"], len(files["sql"]))

			// Verify all deleted.
			pattern := filepath.Join(dir.name, test.pattern)
			err = test.deleter.Delete(pattern)
			require.NoError(t, err)
			files = dir.FilesByExtension()
			require.Equal(t, test.expectedFilesByExt["tmp"], len(files["tmp"]))
			require.Equal(t, test.expectedFilesByExt["txt"], len(files["txt"]))
			require.Equal(t, test.expectedFilesByExt["sql"], len(files["sql"]))

			// Verify idempotent.
			err = test.deleter.Delete(pattern)
			require.NoError(t, err)
			files = dir.FilesByExtension()
			require.Equal(t, test.expectedFilesByExt["tmp"], len(files["tmp"]))
			require.Equal(t, test.expectedFilesByExt["txt"], len(files["txt"]))
			require.Equal(t, test.expectedFilesByExt["sql"], len(files["sql"]))
		})
	}
}
