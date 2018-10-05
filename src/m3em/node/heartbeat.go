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

package node

import (
	"fmt"
	"sync"
	"time"

	hb "github.com/m3db/m3/src/m3em/generated/proto/heartbeat"
	"github.com/m3db/m3x/instrument"

	context "golang.org/x/net/context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
)

type heartbeatRouter struct {
	sync.RWMutex
	endpoint string
	servers  map[string]hb.HeartbeaterServer
}

// NewHeartbeatRouter returns a new heartbeat router
func NewHeartbeatRouter(endpoint string) HeartbeatRouter {
	return &heartbeatRouter{
		endpoint: endpoint,
		servers:  make(map[string]hb.HeartbeaterServer),
	}
}

func (hr *heartbeatRouter) Endpoint() string {
	return hr.endpoint
}

func (hr *heartbeatRouter) Register(id string, server hb.HeartbeaterServer) error {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.servers[id]; ok {
		return fmt.Errorf("uuid is already registered: %s", id)
	}

	hr.servers[id] = server
	return nil
}

func (hr *heartbeatRouter) Deregister(id string) error {
	hr.Lock()
	defer hr.Unlock()

	if _, ok := hr.servers[id]; !ok {
		return fmt.Errorf("unknown uuid: %s", id)
	}

	delete(hr.servers, id)
	return nil
}

func (hr *heartbeatRouter) Heartbeat(
	ctx context.Context,
	msg *hb.HeartbeatRequest,
) (*hb.HeartbeatResponse, error) {
	if msg == nil {
		return nil, grpc.Errorf(codes.InvalidArgument, "received nil heartbeat msg")
	}

	if msg.OperatorUuid == "" {
		return nil, grpc.Errorf(codes.InvalidArgument, "received heartbeat msg without operator uuid: %+v", *msg)
	}

	hr.RLock()
	defer hr.RUnlock()
	server, ok := hr.servers[msg.OperatorUuid]
	if !ok {
		return nil, grpc.Errorf(codes.InvalidArgument, "received heartbeat msg contains un-registered operator uuid: %+v", *msg)
	}

	return server.Heartbeat(ctx, msg)
}

// Receive heartbeats from remote agent: this is to ensure capture of asynchronous
// error conditions, e.g. when a child process kicked off by the agent dies, the
// associated ServiceNode is informed of the crash.
type opHeartbeatServer struct {
	sync.RWMutex
	opts            HeartbeatOptions
	iopts           instrument.Options
	listeners       *listenerGroup
	lastHeartbeat   hb.HeartbeatRequest
	lastHeartbeatTs time.Time
	running         bool
	stopChan        chan struct{}
	wg              sync.WaitGroup
}

func (h *opHeartbeatServer) Heartbeat(
	ctx context.Context,
	msg *hb.HeartbeatRequest,
) (*hb.HeartbeatResponse, error) {
	nowFn := h.opts.NowFn()

	switch msg.GetCode() {
	case hb.HeartbeatCode_HEALTHY:
		h.updateLastHeartbeat(nowFn(), msg)

	case hb.HeartbeatCode_PROCESS_TERMINATION:
		h.updateLastHeartbeat(nowFn(), msg)
		h.listeners.notifyTermination(msg.GetError())

	case hb.HeartbeatCode_OVERWRITTEN:
		h.listeners.notifyOverwrite(msg.GetError())
		h.stop()

	default:
		return nil, grpc.Errorf(codes.InvalidArgument, "received unknown heartbeat msg: %v", *msg)
	}

	return &hb.HeartbeatResponse{}, nil
}

func newHeartbeater(
	lg *listenerGroup,
	opts HeartbeatOptions,
	iopts instrument.Options,
) *opHeartbeatServer {
	h := &opHeartbeatServer{
		opts:      opts,
		iopts:     iopts,
		listeners: lg,
		stopChan:  make(chan struct{}, 1),
	}
	return h
}

func (h *opHeartbeatServer) start() error {
	h.Lock()
	defer h.Unlock()
	if h.running {
		return fmt.Errorf("already heartbeating, terminate existing process first")
	}

	h.running = true
	h.wg.Add(1)
	go h.monitorTimeout()
	return nil
}

func (h *opHeartbeatServer) lastHeartbeatTime() time.Time {
	h.RLock()
	defer h.RUnlock()
	return h.lastHeartbeatTs
}

func (h *opHeartbeatServer) monitorTimeout() {
	defer h.wg.Done()
	var (
		checkInterval      = h.opts.CheckInterval()
		timeoutInterval    = h.opts.Timeout()
		nowFn              = h.opts.NowFn()
		lastNotificationTs time.Time
	)

	for {
		select {
		case <-h.stopChan:
			return
		default:
			last := h.lastHeartbeatTime()
			if !last.IsZero() && nowFn().Sub(last) > timeoutInterval && lastNotificationTs != last {
				h.listeners.notifyTimeout(last)
				lastNotificationTs = last
			}
			// NB(prateek): we use a sleep instead of a ticker because we've observed emperically
			// the former reschedules go-routines with lower delays.
			time.Sleep(checkInterval)
		}
	}
}

func (h *opHeartbeatServer) updateLastHeartbeat(ts time.Time, msg *hb.HeartbeatRequest) {
	h.Lock()
	h.lastHeartbeat = *msg
	h.lastHeartbeatTs = ts
	h.Unlock()
}

func (h *opHeartbeatServer) stop() {
	h.Lock()
	if !h.running {
		h.Unlock()
		return
	}

	h.running = false
	h.stopChan <- struct{}{}
	h.Unlock()
	h.wg.Wait()
}
