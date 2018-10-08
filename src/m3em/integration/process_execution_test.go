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
	"fmt"
	"io/ioutil"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3em/build"
	"github.com/m3db/m3/src/m3em/node"

	"github.com/stretchr/testify/require"
)

func TestProcessExecution(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	th := newTestHarness(t)

	// create test build
	execScript := th.newTestScript(shortLivedTestProgram)
	testBuildID := "target-file.sh"
	testBinary := build.NewServiceBuild(testBuildID, execScript)

	// create test config
	confContents := []byte("some longer string of text\nthat goes on, on and on\n")
	testConfigID := "target-file.conf"
	testConfig := build.NewServiceConfig(testConfigID, confContents)

	svcNode := th.nodeService
	th.Start()
	defer th.Close()

	// get the files transferred over
	require.NoError(t, svcNode.Setup(testBinary, testConfig, "tok", false))

	// execute the build
	require.NoError(t, svcNode.Start())
	stopped := waitUntilAgentFinished(th.agentService, 2*time.Second)
	require.True(t, stopped)

	// get remote process stderr and ensure it is as expected
	stderrFile := path.Join(th.harnessDir, fmt.Sprintf("remote-process/%s.err", testBuildID))
	trunc, err := svcNode.GetRemoteOutput(node.RemoteProcessStderr, stderrFile)
	require.False(t, trunc)
	require.NoError(t, err)
	stderrContents, err := ioutil.ReadFile(stderrFile)
	require.NoError(t, err)
	require.Equal(t, []byte{}, stderrContents, string(stderrContents))

	// get remote process stdout and ensure it is as expected
	stdoutFile := path.Join(th.harnessDir, fmt.Sprintf("remote-process/%s.out", testBuildID))
	trunc, err = svcNode.GetRemoteOutput(node.RemoteProcessStdout, stdoutFile)
	require.False(t, trunc)
	require.NoError(t, err)
	stdoutContents, err := ioutil.ReadFile(stdoutFile)
	require.NoError(t, err)
	require.Equal(t, []byte("testing random output"), stdoutContents)
}
