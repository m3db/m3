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
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3em/build"
	"github.com/m3db/m3/src/m3em/node"

	"github.com/stretchr/testify/require"
)

func TestProcessExecutionHeartbeatingTimeout(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	th := newTestHarnessWithHearbeat(t)

	// capture notifications
	var (
		lock                sync.Mutex
		notifiedTermination = false
	)
	eventListener := node.NewListener(
		func(_ node.ServiceNode, s string) {
			require.False(t, notifiedTermination)
			lock.Lock()
			notifiedTermination = true
			lock.Unlock()
		},
		func(_ node.ServiceNode, ts time.Time) {
			require.FailNow(t, "received timeout", ts.String())
		},
		func(_ node.ServiceNode, s string) {
			require.FailNow(t, "received overwrite", s)
		},
	)

	svcNode := th.nodeService
	th.Start()
	defer th.Close()

	id := svcNode.RegisterListener(eventListener)
	defer svcNode.DeregisterListener(id)

	// create test build
	execScript := th.newTestScript(longRunningTestProgram)
	testBuildID := "target-file.sh"
	testBinary := build.NewServiceBuild(testBuildID, execScript)

	// create test config
	confContents := []byte("some longer string of text\nthat goes on, on and on\n")
	testConfigID := "target-file.conf"
	testConfig := build.NewServiceConfig(testConfigID, confContents)

	// get the files transferred over
	require.NoError(t, svcNode.Setup(testBinary, testConfig, "tok", false))

	// execute the build
	require.NoError(t, svcNode.Start())
	stopped := waitUntilAgentFinished(th.agentService, time.Second)
	require.False(t, stopped)

	// ensure process is still running
	require.False(t, notifiedTermination)

	// and is not timing out
	time.Sleep(th.nodeOptions.HeartbeatOptions().Timeout())
	require.False(t, notifiedTermination)
}
