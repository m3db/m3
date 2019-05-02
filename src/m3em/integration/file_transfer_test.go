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

// +build integration

package integration

import (
	"io/ioutil"
	"path"
	"testing"

	"github.com/m3db/m3/src/m3em/build"

	"github.com/stretchr/testify/require"
)

func TestFileTransfer(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	th := newTestHarness(t)

	// create test build
	buildContents := []byte("some long string of text\nthat goes on and on\n")
	testFile := th.newTempFile(buildContents)
	testBuildID := "target-file.out"
	testBinary := build.NewServiceBuild(testBuildID, testFile.Name())

	// create test config
	confContents := []byte("some longer string of text\nthat goes on, on and on\n")
	testConfigID := "target-file.conf"
	testConfig := build.NewServiceConfig(testConfigID, confContents)

	th.Start()
	defer th.Close()
	node := th.nodeService

	require.NoError(t, node.Setup(testBinary, testConfig, "tok", false))

	// test copied build file contents
	buildOutputPath := path.Join(th.agentOptions.WorkingDirectory(), testBuildID)
	obsBytes, err := ioutil.ReadFile(buildOutputPath)
	require.NoError(t, err)
	require.Equal(t, buildContents, obsBytes)

	// test copied config file contents
	configOutputPath := path.Join(th.agentOptions.WorkingDirectory(), testConfigID)
	obsBytes, err = ioutil.ReadFile(configOutputPath)
	require.NoError(t, err)
	require.Equal(t, confContents, obsBytes)

	th.logger.Info("verified contents")
}
