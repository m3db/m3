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
	"io"
	"net"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3cluster/services/placement"
	"github.com/m3db/m3em/agent"
	hb "github.com/m3db/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3em/generated/proto/m3em"
	"github.com/m3db/m3em/node"

	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

type closeFn func() error

type testHarness struct {
	io.Closer

	closers           []closeFn
	t                 *testing.T
	scriptNum         int
	harnessDir        string
	iopts             instrument.Options
	logger            xlog.Logger
	agentListener     net.Listener
	agentOptions      agent.Options
	agentService      agent.Agent
	agentServer       *grpc.Server
	agentStopped      int32
	nodeOptions       node.Options
	nodeService       node.ServiceNode
	heartbeatListener net.Listener
	heartbeatServer   *grpc.Server
	heartbeatStopped  int32
}

func newTestHarnessWithHearbeat(t *testing.T) *testHarness {
	return newTestHarnessWithHearbeatOptions(t, testHeartbeatOptions())
}

func newTestHarness(t *testing.T) *testHarness {
	return newTestHarnessWithHearbeatOptions(t, nil)
}

func newTestHarnessWithHearbeatOptions(t *testing.T, hbOpts node.HeartbeatOptions) *testHarness {
	logger := xlog.NewLogger(os.Stderr)

	th := &testHarness{
		t:      t,
		logger: logger,
		iopts:  instrument.NewOptions().SetLogger(logger),
	}

	th.harnessDir = newTempDir(t)
	th.addCloser(func() error {
		return os.RemoveAll(th.harnessDir)
	})

	// create agent listener
	agentListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	th.agentListener = agentListener
	th.addCloser(func() error {
		return agentListener.Close()
	})

	// create agent (service|server)
	th.agentOptions = agent.NewOptions(th.iopts).
		SetWorkingDirectory(newSubDir(t, th.harnessDir, "agent-wd")).
		SetExecGenFn(testExecGenFn)
	service, err := agent.New(th.agentOptions)
	require.NoError(t, err)
	th.agentService = service
	th.agentServer = grpc.NewServer(grpc.MaxConcurrentStreams(16384))
	m3em.RegisterOperatorServer(th.agentServer, service)
	th.addCloser(func() error {
		th.agentServer.GracefulStop()
		return nil
	})

	// if provided valid heartbeat options, create heartbeating resources
	if hbOpts != nil {
		heartbeatListener, err := net.Listen("tcp", "127.0.0.1:0")
		require.NoError(t, err)
		th.heartbeatListener = heartbeatListener
		th.addCloser(func() error {
			return heartbeatListener.Close()
		})

		hbRouter := node.NewHeartbeatRouter(heartbeatListener.Addr().String())
		hbServer := grpc.NewServer(grpc.MaxConcurrentStreams(16384))
		hb.RegisterHeartbeaterServer(hbServer, hbRouter)
		th.heartbeatServer = hbServer
		th.addCloser(func() error {
			th.heartbeatServer.GracefulStop()
			return nil
		})
		hbOpts = hbOpts.SetEnabled(true).SetHeartbeatRouter(hbRouter)

	} else {
		hbOpts = node.NewHeartbeatOptions().SetEnabled(false)
	}

	// create options to communicate with agent
	th.nodeOptions = th.newNodeOptions(hbOpts)
	svc := placement.NewInstance()
	node, err := node.New(svc, th.nodeOptions)
	require.NoError(t, err)
	th.nodeService = node
	th.addCloser(func() error {
		return node.Close()
	})

	return th
}

func (th *testHarness) addCloser(fn closeFn) {
	th.closers = append(th.closers, fn)
}

func (th *testHarness) Start() {
	serveGRPCServer := func(server *grpc.Server, listener net.Listener, state *int32) {
		err := server.Serve(listener)
		if closed := atomic.LoadInt32(state); closed == 0 {
			require.NoError(th.t, err)
		}
	}

	if th.nodeOptions.HeartbeatOptions().Enabled() {
		go serveGRPCServer(th.heartbeatServer, th.heartbeatListener, &th.heartbeatStopped)
	}

	go serveGRPCServer(th.agentServer, th.agentListener, &th.agentStopped)
}

func (th *testHarness) StopHeartbeatServer() {
	atomic.StoreInt32(&th.heartbeatStopped, 1)
	th.heartbeatServer.Stop()
}

func (th *testHarness) Close() error {
	atomic.StoreInt32(&th.agentStopped, 1)
	atomic.StoreInt32(&th.heartbeatStopped, 1)

	var multiErr xerrors.MultiError
	for i := len(th.closers) - 1; i >= 0; i-- {
		closer := th.closers[i]
		multiErr = multiErr.Add(closer())
	}
	return multiErr.FinalError()
}

func (th *testHarness) newTempFile(contents []byte) *os.File {
	file := newTempFile(th.t, th.harnessDir, contents)
	th.addCloser(func() error {
		return os.Remove(file.Name())
	})
	return file
}

func (th *testHarness) newTestScript(program testProgram) string {
	sn := th.scriptNum
	th.scriptNum++
	file := newTestScript(th.t, th.harnessDir, sn, program)
	th.addCloser(func() error {
		return os.Remove(file)
	})
	return file
}

func testHeartbeatOptions() node.HeartbeatOptions {
	return node.NewHeartbeatOptions().
		SetEnabled(true).
		SetTimeout(2 * time.Second).
		SetCheckInterval(100 * time.Millisecond).
		SetInterval(time.Second)
}

func (th *testHarness) newNodeOptions(hbOpts node.HeartbeatOptions) node.Options {
	return node.NewOptions(th.iopts).
		SetHeartbeatOptions(hbOpts).
		SetOperatorClientFn(testOperatorClientFn(th.agentListener.Addr().String()))
}
