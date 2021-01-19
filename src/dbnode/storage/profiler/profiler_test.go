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

func TestCPUProfile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
	}{
		{name: "1", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataCPUProfileNamePrefix.name, 1, ProfileFileExtension)},
		{name: "2", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataCPUProfileNamePrefix.name, 2, ProfileFileExtension)},
		{name: "3", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataCPUProfileNamePrefix.name, 3, ProfileFileExtension)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := StartCPUProfile(tmpDir, PeersBootstrapReadDataCPUProfileNamePrefix)
			require.NoError(t, err)
			StopCPUProfile()

			fileInfo, err := os.Stat(filepath.Join(tmpDir, tt.fileName))
			require.NoError(t, err)
			assert.Equal(t, tt.fileName, fileInfo.Name())
		})
	}
}

func TestWriteHeapProfile(t *testing.T) {
	tests := []struct {
		name     string
		fileName string
	}{
		{name: "1", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataHeapProfileNamePrefix.name, 1, ProfileFileExtension)},
		{name: "2", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataHeapProfileNamePrefix.name, 2, ProfileFileExtension)},
		{name: "3", fileName: fmt.Sprintf("%s%d%s", PeersBootstrapReadDataHeapProfileNamePrefix.name, 3, ProfileFileExtension)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WriteHeapProfile(tmpDir, PeersBootstrapReadDataHeapProfileNamePrefix)
			require.NoError(t, err)

			fileInfo, err := os.Stat(filepath.Join(tmpDir, tt.fileName))
			require.NoError(t, err)
			assert.Equal(t, tt.fileName, fileInfo.Name())
		})
	}
}
