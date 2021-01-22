// Copyright (c) 2021 Uber Technologies, Inc.
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

package profiler

import (
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var tmpDir string

func TestMain(m *testing.M) {
	if err := setup(); err != nil {
		fmt.Println("setup error")
		os.Exit(1)
	}
	code := m.Run()
	if err := teardown(); err != nil {
		fmt.Println("teardown error")
		os.Exit(1)
	}
	os.Exit(code)
}

func setup() error {
	tempDir, err := ioutil.TempDir("", "bootstrapProfileTests")
	if err != nil {
		return err
	}
	tmpDir = tempDir
	return nil
}

func teardown() error {
	return os.RemoveAll(tmpDir)
}

func TestFileProfile(t *testing.T) {
	tests := []struct {
		name      string
		fileNames []string
	}{
		{name: "testProfile1", fileNames: []string{"testProfile1.cpu.1.pb.gz", "testProfile1.heap.1.pb.gz"}},
		{name: "testProfile1", fileNames: []string{"testProfile1.cpu.2.pb.gz", "testProfile1.heap.2.pb.gz"}},
		{name: "testProfile2", fileNames: []string{"testProfile2.cpu.1.pb.gz", "testProfile2.heap.1.pb.gz"}},
	}
	sut := NewFileProfiler(tmpDir)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			profile, err := sut.StartProfile(tt.name)
			require.NoError(t, err)
			time.Sleep(100 * time.Millisecond)
			err = profile.StopProfile()
			require.NoError(t, err)

			for _, fileName := range tt.fileNames {
				fileInfo, err := os.Stat(filepath.Join(tmpDir, fileName))
				require.NoError(t, err)
				assert.Equal(t, fileName, fileInfo.Name())
			}
		})
	}
}
