// Copyright (c) 2015 Uber Technologies, Inc.

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

package tchannel_test

import (
	"errors"
	"fmt"
	"io"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	. "github.com/uber/tchannel-go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber/tchannel-go/raw"
	"github.com/uber/tchannel-go/testutils"
	"github.com/uber/tchannel-go/testutils/goroutines"
	"github.com/uber/tchannel-go/testutils/testreader"
	"golang.org/x/net/context"
)

// Values used in tests
var (
	testServiceName = testutils.DefaultServerName
	testArg2        = []byte("Header in arg2")
	testArg3        = []byte("Body in arg3")
)

type testHandler struct {
	sync.Mutex

	t        testing.TB
	format   Format
	caller   string
	blockErr chan error
}

func newTestHandler(t testing.TB) *testHandler {
	return &testHandler{t: t, blockErr: make(chan error)}
}

func (h *testHandler) Handle(ctx context.Context, args *raw.Args) (*raw.Res, error) {
	h.Lock()
	h.format = args.Format
	h.caller = args.Caller
	h.Unlock()

	assert.Equal(h.t, args.Caller, CurrentCall(ctx).CallerName())

	switch args.Method {
	case "block":
		<-ctx.Done()
		h.blockErr <- ctx.Err()
		return &raw.Res{
			IsErr: true,
		}, nil
	case "echo":
		return &raw.Res{
			Arg2: args.Arg2,
			Arg3: args.Arg3,
		}, nil
	case "busy":
		return &raw.Res{
			SystemErr: ErrServerBusy,
		}, nil
	case "app-error":
		return &raw.Res{
			IsErr: true,
		}, nil
	}
	return nil, errors.New("unknown method")
}

func (h *testHandler) OnError(ctx context.Context, err error) {
	stack := make([]byte, 4096)
	runtime.Stack(stack, false /* all */)
	h.t.Errorf("testHandler got error: %v stack:\n%s", err, stack)
}

func writeFlushStr(w ArgWriter, d string) error {
	if _, err := io.WriteString(w, d); err != nil {
		return err
	}
	return w.Flush()
}

func TestRoundTrip(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		handler := newTestHandler(t)
		ch.Register(raw.Wrap(handler), "echo")

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		call, err := ch.BeginCall(ctx, hostPort, testServiceName, "echo", &CallOptions{Format: JSON})
		require.NoError(t, err)

		require.NoError(t, NewArgWriter(call.Arg2Writer()).Write(testArg2))
		require.NoError(t, NewArgWriter(call.Arg3Writer()).Write(testArg3))

		var respArg2 []byte
		require.NoError(t, NewArgReader(call.Response().Arg2Reader()).Read(&respArg2))
		assert.Equal(t, testArg2, []byte(respArg2))

		var respArg3 []byte
		require.NoError(t, NewArgReader(call.Response().Arg3Reader()).Read(&respArg3))
		assert.Equal(t, testArg3, []byte(respArg3))

		assert.Equal(t, JSON, handler.format)
		assert.Equal(t, testServiceName, handler.caller)
		assert.Equal(t, JSON, call.Response().Format(), "response Format should match request Format")
	})
}

func TestDefaultFormat(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		handler := newTestHandler(t)
		ch.Register(raw.Wrap(handler), "echo")

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		arg2, arg3, resp, err := raw.Call(ctx, ch, hostPort, testServiceName, "echo", testArg2, testArg3)
		require.Nil(t, err)

		require.Equal(t, testArg2, arg2)
		require.Equal(t, testArg3, arg3)
		require.Equal(t, Raw, handler.format)
		assert.Equal(t, Raw, resp.Format(), "response Format should match request Format")
	})
}

func TestRemotePeer(t *testing.T) {
	tests := []struct {
		name       string
		remote     func() *Channel
		expectedFn func(state *RuntimeState, serverHP string) PeerInfo
	}{
		{
			name:   "ephemeral client",
			remote: func() *Channel { return testutils.NewClient(t, nil) },
			expectedFn: func(state *RuntimeState, serverHP string) PeerInfo {
				hostPort := state.RootPeers[serverHP].OutboundConnections[0].LocalHostPort
				return PeerInfo{
					HostPort:    hostPort,
					IsEphemeral: true,
					ProcessName: state.LocalPeer.ProcessName,
				}
			},
		},
		{
			name:   "listening server",
			remote: func() *Channel { return testutils.NewServer(t, nil) },
			expectedFn: func(state *RuntimeState, _ string) PeerInfo {
				return PeerInfo{
					HostPort:    state.LocalPeer.HostPort,
					IsEphemeral: false,
					ProcessName: state.LocalPeer.ProcessName,
				}
			},
		},
	}

	ctx, cancel := NewContext(time.Second)
	defer cancel()

	for _, tt := range tests {
		WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
			remote := tt.remote()
			defer remote.Close()

			gotPeer := make(chan PeerInfo, 1)
			testutils.RegisterFunc(ch, "test", func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
				gotPeer <- CurrentCall(ctx).RemotePeer()
				return &raw.Res{}, nil
			})

			_, _, _, err := raw.Call(ctx, remote, hostPort, ch.ServiceName(), "test", nil, nil)
			assert.NoError(t, err, "%v: Call failed", tt.name)
			expected := tt.expectedFn(remote.IntrospectState(nil), hostPort)
			assert.Equal(t, expected, <-gotPeer, "%v: RemotePeer mismatch", tt.name)
		})
	}
}

func TestReuseConnection(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	s1Opts := &testutils.ChannelOpts{ServiceName: "s1"}
	testutils.WithTestServer(t, s1Opts, func(ts *testutils.TestServer) {
		ch2 := ts.NewServer(&testutils.ChannelOpts{ServiceName: "s2"})
		hostPort2 := ch2.PeerInfo().HostPort
		defer ch2.Close()

		ts.Register(raw.Wrap(newTestHandler(t)), "echo")
		ch2.Register(raw.Wrap(newTestHandler(t)), "echo")

		outbound, err := ts.Server().BeginCall(ctx, hostPort2, "s2", "echo", nil)
		require.NoError(t, err)
		outboundConn, outboundNetConn := OutboundConnection(outbound)

		// Try to make another call at the same time, should reuse the same connection.
		outbound2, err := ts.Server().BeginCall(ctx, hostPort2, "s2", "echo", nil)
		require.NoError(t, err)
		outbound2Conn, _ := OutboundConnection(outbound)
		assert.Equal(t, outboundConn, outbound2Conn)

		// Wait for the connection to be marked as active in ch2.
		assert.True(t, testutils.WaitFor(time.Second, func() bool {
			return ch2.IntrospectState(nil).NumConnections > 0
		}), "ch2 does not have any active connections")

		// When ch2 tries to call ch1, it should reuse the inbound connection from ch1.
		outbound3, err := ch2.BeginCall(ctx, ts.HostPort(), "s1", "echo", nil)
		require.NoError(t, err)
		_, outbound3NetConn := OutboundConnection(outbound3)
		assert.Equal(t, outboundNetConn.RemoteAddr(), outbound3NetConn.LocalAddr())
		assert.Equal(t, outboundNetConn.LocalAddr(), outbound3NetConn.RemoteAddr())

		// Ensure all calls can complete in parallel.
		var wg sync.WaitGroup
		for _, call := range []*OutboundCall{outbound, outbound2, outbound3} {
			wg.Add(1)
			go func(call *OutboundCall) {
				defer wg.Done()
				resp1, resp2, _, err := raw.WriteArgs(call, []byte("arg2"), []byte("arg3"))
				require.NoError(t, err)
				assert.Equal(t, resp1, []byte("arg2"), "result does match argument")
				assert.Equal(t, resp2, []byte("arg3"), "result does match argument")
			}(call)
		}
		wg.Wait()
	})
}

func TestPing(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ctx, cancel := NewContext(time.Second)
		defer cancel()

		clientCh := testutils.NewClient(t, nil)
		require.NoError(t, clientCh.Ping(ctx, hostPort))
	})
}

func TestBadRequest(t *testing.T) {
	// ch will log an error when it receives a request for an unknown handler.
	opts := testutils.NewOpts().AddLogFilter("Couldn't find handler.", 1)
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		ctx, cancel := NewContext(time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, "Nowhere", "Noone", []byte("Headers"), []byte("Body"))
		require.NotNil(t, err)
		assert.Equal(t, ErrCodeBadRequest, GetSystemErrorCode(err))
	})
}

func TestNoTimeout(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ch.Register(raw.Wrap(newTestHandler(t)), "Echo")

		ctx := context.Background()
		_, _, _, err := raw.Call(ctx, ch, hostPort, "svc", "Echo", []byte("Headers"), []byte("Body"))
		require.NotNil(t, err)
		assert.Equal(t, ErrTimeoutRequired, err)
	})
}

func TestServerBusy(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ch.Register(ErrorHandlerFunc(func(ctx context.Context, call *InboundCall) error {
			if _, err := raw.ReadArgs(call); err != nil {
				return err
			}
			return ErrServerBusy
		}), "busy")

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "busy", []byte("Arg2"), []byte("Arg3"))
		require.NotNil(t, err)
		assert.Equal(t, ErrCodeBusy, GetSystemErrorCode(err), "err: %v", err)
	})
}

func TestUnexpectedHandlerError(t *testing.T) {
	opts := testutils.NewOpts().
		AddLogFilter("Unexpected handler error", 1)

	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		ch.Register(ErrorHandlerFunc(func(ctx context.Context, call *InboundCall) error {
			if _, err := raw.ReadArgs(call); err != nil {
				return err
			}
			return fmt.Errorf("nope")
		}), "nope")

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "nope", []byte("Arg2"), []byte("Arg3"))
		require.NotNil(t, err)
		assert.Equal(t, ErrCodeUnexpected, GetSystemErrorCode(err), "err: %v", err)
	})
}

type onErrorTestHandler struct {
	*testHandler
	onError func(ctx context.Context, err error)
}

func (h onErrorTestHandler) OnError(ctx context.Context, err error) {
	h.onError(ctx, err)
}

func TestTimeout(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		// onError may be called when the block call tries to write the call response.
		onError := func(ctx context.Context, err error) {
			assert.Equal(t, ErrTimeout, err, "onError err should be ErrTimeout")
			assert.Equal(t, context.DeadlineExceeded, ctx.Err(), "Context should timeout")
		}
		testHandler := onErrorTestHandler{newTestHandler(t), onError}
		ch.Register(raw.Wrap(testHandler), "block")

		ctx, cancel := NewContext(testutils.Timeout(15 * time.Millisecond))
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "block", []byte("Arg2"), []byte("Arg3"))
		assert.Equal(t, ErrTimeout, err)

		// Verify the server-side receives an error from the context.
		select {
		case err := <-testHandler.blockErr:
			assert.Equal(t, context.DeadlineExceeded, err, "Server should have received timeout")
		case <-time.After(time.Second):
			t.Errorf("Server did not receive call, may need higher timeout")
		}
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestLargeMethod(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ctx, cancel := NewContext(time.Second)
		defer cancel()

		largeMethod := testutils.RandBytes(16*1024 + 1)
		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, string(largeMethod), nil, nil)
		assert.Equal(t, ErrMethodTooLarge, err)
	})
}

func TestLargeTimeout(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ch.Register(raw.Wrap(newTestHandler(t)), "echo")

		ctx, cancel := NewContext(1000 * time.Second)
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "echo", testArg2, testArg3)
		assert.NoError(t, err, "Call failed")
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestFragmentation(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ch.Register(raw.Wrap(newTestHandler(t)), "echo")

		arg2 := make([]byte, MaxFramePayloadSize*2)
		for i := 0; i < len(arg2); i++ {
			arg2[i] = byte('a' + (i % 10))
		}

		arg3 := make([]byte, MaxFramePayloadSize*3)
		for i := 0; i < len(arg3); i++ {
			arg3[i] = byte('A' + (i % 10))
		}

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		respArg2, respArg3, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "echo", arg2, arg3)
		require.NoError(t, err)
		assert.Equal(t, arg2, respArg2)
		assert.Equal(t, arg3, respArg3)
	})
}

func TestFragmentationSlowReader(t *testing.T) {
	startReading, handlerComplete := make(chan struct{}), make(chan struct{})
	handler := func(ctx context.Context, call *InboundCall) {
		<-startReading
		<-ctx.Done()
		_, err := raw.ReadArgs(call)
		assert.Error(t, err, "ReadArgs should fail since frames will be dropped due to slow reading")
		close(handlerComplete)
	}

	// Inbound forward will timeout and cause a warning log.
	opts := testutils.NewOpts().
		AddLogFilter("Unable to forward frame", 1).
		AddLogFilter("Connection error", 1)
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		ch.Register(HandlerFunc(handler), "echo")

		arg2 := testutils.RandBytes(MaxFramePayloadSize * MexChannelBufferSize)
		arg3 := testutils.RandBytes(MaxFramePayloadSize * (MexChannelBufferSize + 1))

		ctx, cancel := NewContext(testutils.Timeout(30 * time.Millisecond))
		defer cancel()

		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "echo", arg2, arg3)
		assert.Error(t, err, "Call should timeout due to slow reader")

		close(startReading)
		select {
		case <-handlerComplete:
		case <-time.After(testutils.Timeout(70 * time.Millisecond)):
			t.Errorf("Handler not called, context timeout may be too low")
		}
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestWriteArg3AfterTimeout(t *testing.T) {
	// The channel reads and writes during timeouts, causing warning logs.
	opts := testutils.NewOpts().DisableLogVerification()
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		timedOut := make(chan struct{})

		handler := func(ctx context.Context, call *InboundCall) {
			_, err := raw.ReadArgs(call)
			assert.NoError(t, err, "Read args failed")
			response := call.Response()
			assert.NoError(t, NewArgWriter(response.Arg2Writer()).Write(nil), "Write Arg2 failed")
			writer, err := response.Arg3Writer()
			assert.NoError(t, err, "Arg3Writer failed")

			for {
				if _, err := writer.Write(testutils.RandBytes(4096)); err != nil {
					assert.Equal(t, err, ErrTimeout, "Handler should timeout")
					close(timedOut)
					return
				}
				runtime.Gosched()
			}
		}
		ch.Register(HandlerFunc(handler), "call")

		ctx, cancel := NewContext(testutils.Timeout(20 * time.Millisecond))
		defer cancel()
		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "call", nil, nil)
		assert.Equal(t, err, ErrTimeout, "Call should timeout")

		// Wait for the write to complete, make sure there's no errors.
		select {
		case <-time.After(testutils.Timeout(30 * time.Millisecond)):
			t.Errorf("Handler should have failed due to timeout")
		case <-timedOut:
		}
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestWriteErrorAfterTimeout(t *testing.T) {
	// TODO: Make this test block at different points (e.g. before, during read/write).
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		timedOut := make(chan struct{})
		done := make(chan struct{})
		handler := func(ctx context.Context, call *InboundCall) {
			<-ctx.Done()
			<-timedOut
			_, err := raw.ReadArgs(call)
			assert.Equal(t, ErrTimeout, err, "Read args should fail with timeout")
			response := call.Response()
			assert.Equal(t, ErrTimeout, response.SendSystemError(ErrServerBusy), "SendSystemError should fail")
			close(done)
		}
		ch.Register(HandlerFunc(handler), "call")

		ctx, cancel := NewContext(testutils.Timeout(30 * time.Millisecond))
		defer cancel()
		_, _, _, err := raw.Call(ctx, ch, hostPort, testServiceName, "call", nil, testutils.RandBytes(100000))
		assert.Equal(t, err, ErrTimeout, "Call should timeout")
		close(timedOut)

		select {
		case <-done:
		case <-time.After(time.Second):
			t.Errorf("Handler not called, timeout may be too low")
		}
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestWriteAfterConnectionError(t *testing.T) {
	ctx, cancel := NewContext(time.Second)
	defer cancel()

	// Closing network connections can lead to warnings in many places.
	opts := testutils.NewOpts().DisableLogVerification()
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		testutils.RegisterEcho(ch, nil)

		call, err := ch.BeginCall(ctx, hostPort, ch.ServiceName(), "echo", nil)
		require.NoError(t, err, "Call failed")

		w, err := call.Arg2Writer()
		require.NoError(t, err, "Arg2Writer failed")
		require.NoError(t, writeFlushStr(w, "initial"), "write initial failed")

		// Now close the underlying network connection, writes should fail.
		_, conn := OutboundConnection(call)
		conn.Close()

		// Writes should start failing pretty soon.
		var writeErr error
		for i := 0; i < 100; i++ {
			if writeErr = writeFlushStr(w, "f"); writeErr != nil {
				break
			}
			time.Sleep(time.Millisecond)
		}
		if assert.Error(t, writeErr, "Writes should fail after a connection is closed") {
			assert.Equal(t, ErrCodeNetwork, GetSystemErrorCode(writeErr), "write should fail due to network error")
		}
	})
}

func TestReadTimeout(t *testing.T) {
	// The error frame may fail to send since the connection closes before the handler sends it
	// or the handler connection may be closed as it sends when the other side closes the conn.
	opts := testutils.NewOpts().
		AddLogFilter("Couldn't send outbound error frame", 1).
		AddLogFilter("Connection error", 1, "site", "read frames").
		AddLogFilter("Connection error", 1, "site", "write frames").
		AddLogFilter("simpleHandler OnError", 1,
			"error", "failed to send error frame, connection state connectionClosed")
	WithVerifiedServer(t, opts, func(ch *Channel, hostPort string) {
		for i := 0; i < 10; i++ {
			ctx, cancel := NewContext(time.Second)
			handler := func(ctx context.Context, args *raw.Args) (*raw.Res, error) {
				defer cancel()
				return nil, ErrTimeout
			}
			testutils.RegisterFunc(ch, "call", handler)
			_, _, _, err := raw.Call(ctx, ch, hostPort, ch.PeerInfo().ServiceName, "call", nil, nil)
			assert.Equal(t, err, context.Canceled, "Call should fail due to cancel")
		}
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestWriteTimeout(t *testing.T) {
	WithVerifiedServer(t, nil, func(ch *Channel, hostPort string) {
		ctx, cancel := NewContext(testutils.Timeout(15 * time.Millisecond))
		defer cancel()

		call, err := ch.BeginCall(ctx, hostPort, ch.ServiceName(), "call", nil)
		require.NoError(t, err, "Call failed")

		writer, err := call.Arg2Writer()
		require.NoError(t, err, "Arg2Writer failed")

		_, err = writer.Write([]byte{1})
		require.NoError(t, err, "Write initial bytes failed")
		<-ctx.Done()

		_, err = io.Copy(writer, testreader.Looper([]byte{1}))
		assert.Equal(t, ErrTimeout, err, "Write should fail with timeout")
	})
	goroutines.VerifyNoLeaks(t, nil)
}

func TestGracefulClose(t *testing.T) {
	testutils.WithTestServer(t, nil, func(ts *testutils.TestServer) {
		ch2 := ts.NewServer(nil)
		hp2 := ch2.PeerInfo().HostPort
		defer ch2.Close()

		ctx, cancel := NewContext(time.Second)
		defer cancel()

		assert.NoError(t, ts.Server().Ping(ctx, hp2), "Ping from ch1 -> ch2 failed")
		assert.NoError(t, ch2.Ping(ctx, ts.HostPort()), "Ping from ch2 -> ch1 failed")
	})
}

func TestNetDialTimeout(t *testing.T) {
	// timeoutHostPort uses a blackholed address (RFC 6890) with a port
	// reserved for documentation. This address should always cause a timeout.
	const timeoutHostPort = "192.18.0.254:44444"
	timeoutPeriod := testutils.Timeout(50 * time.Millisecond)

	client := testutils.NewClient(t, nil)
	defer client.Close()

	started := time.Now()
	ctx, cancel := NewContext(timeoutPeriod)
	defer cancel()

	err := client.Ping(ctx, timeoutHostPort)
	if !assert.Error(t, err, "Ping to blackhole address should fail") {
		return
	}

	if strings.Contains(err.Error(), "network is unreachable") {
		t.Skipf("Skipping test, as network interface may not be available")
	}

	d := time.Since(started)
	assert.Equal(t, ErrTimeout, err, "Ping expected to fail with timeout")
	assert.True(t, d >= timeoutPeriod, "Timeout should take more than %v, took %v", timeoutPeriod, d)
}
