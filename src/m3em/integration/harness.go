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

//go:build integration
// +build integration

package integration

import (
	"io"
	"net"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/m3em/agent"
	hb "github.com/m3db/m3/src/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3/src/m3em/generated/proto/m3em"
	"github.com/m3db/m3/src/m3em/integration/resources"
	"github.com/m3db/m3/src/m3em/node"
	xgrpc "github.com/m3db/m3/src/m3em/x/grpc"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type closeFn func() error

type testHarness struct {
	io.Closer

	closers           []closeFn
	t                 *testing.T
	scriptNum         int
	harnessDir        string
	iopts             instrument.Options
	logger            *zap.Logger
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
	logger := xtest.NewLogger(t)
	useTLS := strings.ToLower(os.Getenv("TEST_TLS_COMMUNICATION")) == "true"
	if useTLS {
		logger.Info("using TLS for RPC")
	} else {
		logger.Info("not using TLS for RPC")
	}

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

	var serverCreds credentials.TransportCredentials
	if useTLS {
		serverCreds, err = resources.ServerTransportCredentials()
		require.NoError(th.t, err)
	}

	// create agent (service|server)
	th.agentOptions = agent.NewOptions(th.iopts).
		SetWorkingDirectory(newSubDir(t, th.harnessDir, "agent-wd")).
		SetExecGenFn(testExecGenFn)
	service, err := agent.New(th.agentOptions)
	require.NoError(t, err)
	th.agentService = service
	th.agentServer = xgrpc.NewServer(serverCreds)
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
		hbServer := xgrpc.NewServer(nil)
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
	th.nodeOptions = th.newNodeOptions(hbOpts, useTLS)
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

func (th *testHarness) newNodeOptions(hbOpts node.HeartbeatOptions, useTLS bool) node.Options {
	return node.NewOptions(th.iopts).
		SetHeartbeatOptions(hbOpts).
		SetOperatorClientFn(th.testOperatorClientFn(useTLS))
}

func (th *testHarness) testOperatorClientFn(useTLS bool) node.OperatorClientFn {
	endpoint := th.agentListener.Addr().String()
	dialOpt := grpc.WithInsecure()
	if useTLS {
		tc, err := resources.ClientTransportCredentials()
		require.NoError(th.t, err)
		dialOpt = grpc.WithTransportCredentials(tc)
	}
	return func() (*grpc.ClientConn, m3em.OperatorClient, error) {
		conn, err := grpc.Dial(endpoint, dialOpt)
		if err != nil {
			return nil, nil, err
		}
		return conn, m3em.NewOperatorClient(conn), err
	}
}
