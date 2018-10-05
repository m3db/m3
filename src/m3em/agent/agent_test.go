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
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"sync"
	"testing"
	"time"

	hb "github.com/m3db/m3/src/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3/src/m3em/generated/proto/m3em"
	"github.com/m3db/m3/src/m3em/os/exec"
	mockexec "github.com/m3db/m3/src/m3em/os/exec/mocks"
	xgrpc "github.com/m3db/m3/src/m3em/x/grpc"
	m3xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	context "golang.org/x/net/context"
)

func newTempDir(t *testing.T) string {
	path, err := ioutil.TempDir("", "agent-test")
	require.NoError(t, err)
	return path
}

func newTestOptions(workingDir string) Options {
	iopts := instrument.NewOptions()
	return NewOptions(iopts).
		SetHeartbeatTimeout(2 * time.Second).
		SetWorkingDirectory(workingDir).
		SetExecGenFn(func(p string, c string) (string, []string) {
			return p, nil
		})
}

type mockHeartbeatServer struct {
	sync.Mutex
	beats []hb.HeartbeatRequest
}

func (mh *mockHeartbeatServer) Heartbeat(c context.Context, h *hb.HeartbeatRequest) (*hb.HeartbeatResponse, error) {
	mh.Lock()
	defer mh.Unlock()
	mh.beats = append(mh.beats, *h)
	return &hb.HeartbeatResponse{}, nil
}

func (mh *mockHeartbeatServer) heartbeats() []hb.HeartbeatRequest {
	mh.Lock()
	defer mh.Unlock()
	beats := make([]hb.HeartbeatRequest, 0, len(mh.beats))
	beats = append(beats, mh.beats...)
	return beats
}

func TestAgentClose(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	require.NoError(t, rawAgent.Close())
}

func TestProgramCrashNotify(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	hbService := &mockHeartbeatServer{}
	hbListener, err := net.Listen("tcp", "127.0.0.1:0")
	hbServer := xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(hbServer, hbService)
	require.NoError(t, err)
	go hbServer.Serve(hbListener)
	defer hbServer.Stop()

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	var testListener exec.ProcessListener
	pm := mockexec.NewMockProcessMonitor(ctrl)
	rawAgent.newProcessMonitorFn = func(c exec.Cmd, l exec.ProcessListener) (exec.ProcessMonitor, error) {
		testListener = l
		return pm, nil
	}

	setupResp, err := rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "abc",
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      hbListener.Addr().String(),
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	rawAgent.executablePath = "someString"
	rawAgent.configPath = "otherString"

	pm.EXPECT().Start().Return(nil)
	startResp, err := rawAgent.Start(context.Background(), &m3em.StartRequest{})
	require.NoError(t, err)
	require.NotNil(t, startResp)
	time.Sleep(time.Second)

	pm.EXPECT().Stop().Do(func() {
		testListener.OnComplete()
	}).Return(nil)
	stopResp, err := rawAgent.Stop(context.Background(), &m3em.StopRequest{})
	require.NoError(t, err)
	require.NotNil(t, stopResp)

	// ensure no termination message received in hb server
	time.Sleep(time.Second)
	beats := hbService.heartbeats()
	require.NotEmpty(t, beats)
	for _, beat := range beats {
		if beat.Code == hb.HeartbeatCode_PROCESS_TERMINATION {
			require.Fail(t, "received unexpected heartbeat message")
		}
	}
}

func TestTooManyFailedHeartbeatsUnsetup(t *testing.T) {
	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	setupResp, err := rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "some-token",
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      "badaddress.com:80",
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	// ensure agent has reset itself after timeout
	time.Sleep(opts.HeartbeatTimeout() * 10)
	require.False(t, rawAgent.isSetup())
}

func TestTooManyFailedHeartbeatsStop(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	pm := mockexec.NewMockProcessMonitor(ctrl)
	rawAgent.newProcessMonitorFn = func(c exec.Cmd, l exec.ProcessListener) (exec.ProcessMonitor, error) {
		return pm, nil
	}

	setupResp, err := rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "abc",
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      "baddaddress.com:80",
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	rawAgent.executablePath = "someString"
	rawAgent.configPath = "otherString"

	gomock.InOrder(
		pm.EXPECT().Start().Return(nil),
		pm.EXPECT().Stop().Return(nil),
	)

	startResp, err := rawAgent.Start(context.Background(), &m3em.StartRequest{})
	require.NoError(t, err)
	require.NotNil(t, startResp)
	time.Sleep(time.Second)

	// ensure agent has reset itself after timeout
	time.Sleep(2 * opts.HeartbeatTimeout())
	require.False(t, rawAgent.isSetup())
}

func TestSetupOverrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	hbService := &mockHeartbeatServer{}
	hbListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	hbServer := xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(hbServer, hbService)
	go hbServer.Serve(hbListener)
	defer hbServer.Stop()

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	setupResp, err := rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "abc",
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      hbListener.Addr().String(),
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	// ensure heartbeating has started
	time.Sleep(time.Second)
	beats := hbService.heartbeats()
	require.NotEmpty(t, beats)

	// make new heartbeatServer
	newHbService := &mockHeartbeatServer{}
	newHbListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	newHbServer := xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(newHbServer, newHbService)
	go newHbServer.Serve(newHbListener)
	defer newHbServer.Stop()

	// ask agent to send messages to new hb server
	setupResp, err = rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "other",
		Force:                  true,
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      newHbListener.Addr().String(),
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	// old hb service should receive no more messages
	oldHBBeatsT0 := hbService.heartbeats()

	// ensure new heartbeating has started
	time.Sleep(time.Second)
	newHBBeats := newHbService.heartbeats()
	require.NotEmpty(t, newHBBeats)

	// old hb service should not have received any more messages
	oldHBBeatsT1 := hbService.heartbeats()
	require.Equal(t, oldHBBeatsT0, oldHBBeatsT1)
}

func TestClientReconnect(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	hbService := &mockHeartbeatServer{}
	hbListener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	// holding onto address as we'll need to open the listener again
	listenAddress := hbListener.Addr().String()

	hbServer := xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(hbServer, hbService)
	go hbServer.Serve(hbListener)

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	rawAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	setupResp, err := rawAgent.Setup(context.Background(), &m3em.SetupRequest{
		SessionToken:           "abc",
		HeartbeatEnabled:       true,
		HeartbeatFrequencySecs: 1,
		HeartbeatEndpoint:      listenAddress,
	})
	require.NoError(t, err)
	require.NotNil(t, setupResp)

	// ensure heartbeating has started
	foundHeartbeats := m3xclock.WaitUntil(func() bool {
		return len(hbService.heartbeats()) > 0
	}, 5*time.Second)
	require.True(t, foundHeartbeats)

	// crash heartbeat server
	hbServer.Stop()

	// re-start heartbeat server after a second
	time.Sleep(time.Second)
	hbService = &mockHeartbeatServer{}
	hbListener, err = net.Listen("tcp", listenAddress)
	require.NoError(t, err)
	hbServer = xgrpc.NewServer(nil)
	hb.RegisterHeartbeaterServer(hbServer, hbService)
	go hbServer.Serve(hbListener)
	defer hbServer.Stop()

	// ensure heartbeating has restarted
	foundHeartbeats = m3xclock.WaitUntil(func() bool {
		return len(hbService.heartbeats()) > 0
	}, 5*time.Second)
	require.True(t, foundHeartbeats)
}

func TestPullFile(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	tempDir := newTempDir(t)
	defer os.RemoveAll(tempDir)

	opts := newTestOptions(tempDir)
	testAgent, err := New(opts)
	require.NoError(t, err)
	opAgent, ok := testAgent.(*opAgent)
	require.True(t, ok)

	var (
		testStdoutPath = fmt.Sprintf("%s/some-path", tempDir)
		testChunkSize  = 5
		testMaxSize    = 10
		testBytes      []byte
		expectedBytes  []byte
	)

	// create testBytes 2x allowable amount
	for i := 0; i < testMaxSize; i++ {
		testBytes = append(testBytes, byte('a'))
	}
	for i := 0; i < testMaxSize; i++ {
		testBytes = append(testBytes, byte('b'))
		expectedBytes = append(expectedBytes, byte('b'))
	}

	// create file with testBytes contents
	require.NoError(t, ioutil.WriteFile(testStdoutPath, testBytes, os.FileMode(0666)))
	pm := mockexec.NewMockProcessMonitor(ctrl)
	pullServer := m3em.NewMockOperator_PullFileServer(ctrl)

	pm.EXPECT().StdoutPath().Return(testStdoutPath)
	gomock.InOrder(
		pullServer.EXPECT().Send(&m3em.PullFileResponse{
			Data: &m3em.DataChunk{
				Bytes: expectedBytes[testChunkSize:],
				Idx:   1,
			},
			Truncated: true,
		}).Return(nil),
		pullServer.EXPECT().Send(&m3em.PullFileResponse{
			Data: &m3em.DataChunk{
				Bytes: expectedBytes[testChunkSize:],
				Idx:   2,
			},
			Truncated: true,
		}).Return(nil),
	)

	opAgent.processMonitor = pm
	opAgent.token = "setup-token"
	require.Nil(t, opAgent.PullFile(&m3em.PullFileRequest{
		ChunkSize: int64(testChunkSize),
		MaxSize:   int64(testMaxSize),
		FileType:  m3em.PullFileType_PULL_FILE_TYPE_SERVICE_STDOUT,
	}, pullServer))
}
