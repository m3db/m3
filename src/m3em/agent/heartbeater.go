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
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	hb "github.com/m3db/m3em/generated/proto/heartbeat"

	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"google.golang.org/grpc"
)

type runner interface {
	Running() bool
}

type timeoutFn func(time.Time)
type errorFn func(err error)

type heartbeatOpts struct {
	operatorUUID string
	endpoint     string
	nowFn        xclock.NowFn
	timeout      time.Duration
	timeoutFn    timeoutFn
	errorFn      errorFn
}

type heatbeater struct {
	sync.RWMutex
	iopts           instrument.Options
	runner          runner
	opts            heartbeatOpts
	running         int32
	closed          int32
	msgChan         chan heartbeatMsg
	doneCh          chan struct{}
	conn            *grpc.ClientConn
	client          hb.HeartbeaterClient
	lastHeartbeatTs time.Time
}

func newHeartbeater(
	agent runner,
	opts heartbeatOpts,
	iopts instrument.Options,
) (*heatbeater, error) {
	conn, err := grpc.Dial(opts.endpoint,
		grpc.WithInsecure(),
		grpc.WithTimeout(opts.timeout))
	if err != nil {
		return nil, err
	}
	client := hb.NewHeartbeaterClient(conn)
	return &heatbeater{
		runner:  agent,
		opts:    opts,
		iopts:   iopts,
		msgChan: make(chan heartbeatMsg, 10),
		doneCh:  make(chan struct{}, 1),
		conn:    conn,
		client:  client,
	}, nil
}

func (h *heatbeater) isRunning() bool {
	return atomic.LoadInt32(&h.running) != 0
}

func (h *heatbeater) isClosed() bool {
	return atomic.LoadInt32(&h.closed) != 0
}

func (h *heatbeater) defaultHeartbeat() hb.HeartbeatRequest {
	h.RLock()
	req := hb.HeartbeatRequest{
		OperatorUuid:   h.opts.operatorUUID,
		ProcessRunning: h.runner.Running(),
	}
	h.RUnlock()
	return req
}

func (h *heatbeater) start(d time.Duration) error {
	if h.isRunning() {
		return fmt.Errorf("heartbeater already running")
	}
	atomic.StoreInt32(&h.running, 1)
	go h.heartbeatLoop(d)
	return nil
}

func (h *heatbeater) sendHealthyHeartbeat() {
	beat := h.defaultHeartbeat()
	beat.Code = hb.HeartbeatCode_HEALTHY
	h.sendHeartbeat(&beat)
}

func (h *heatbeater) heartbeatLoop(d time.Duration) {
	defer func() {
		atomic.StoreInt32(&h.running, 0)
		h.doneCh <- struct{}{}
	}()

	// explicitly send first heartbeat as soon as we start
	h.sendHealthyHeartbeat()

	for {
		select {
		case msg := <-h.msgChan:
			beat := h.defaultHeartbeat()
			switch {
			case msg.stop:
				return
			case msg.processTerminate:
				beat.Code = hb.HeartbeatCode_PROCESS_TERMINATION
				beat.Error = msg.err
			case msg.overwritten:
				beat.Code = hb.HeartbeatCode_OVERWRITTEN
				beat.Error = msg.err
			default:
				h.opts.errorFn(fmt.Errorf(
					"invalid heartbeatMsg received, one of stop|processTerminate|overwritten must be set. Received: %+v", msg))
				return
			}
			h.sendHeartbeat(&beat)

		default:
			h.sendHealthyHeartbeat()
			// NB(prateek): we use a sleep instead of a ticker because we've observed emperically
			// the former reschedules go-routines with lower delays.
			time.Sleep(d)
		}
	}
}

func (h *heatbeater) sendHeartbeat(r *hb.HeartbeatRequest) {
	h.Lock()
	defer h.Unlock()

	// mark the first send attempt as success to get a base time to compare against
	if h.lastHeartbeatTs.IsZero() {
		h.lastHeartbeatTs = h.opts.nowFn()
	}

	logger := h.iopts.Logger()
	_, err := h.client.Heartbeat(context.Background(), r)

	// sent heartbeat successfully, break out
	if err == nil {
		return
	}

	// unable to send heartbeat
	logger.Warnf("unable to send heartbeat: %v", err)
	// check if this has been happening past the permitted period
	var (
		timeSinceLastSend = h.opts.nowFn().Sub(h.lastHeartbeatTs)
		timeout           = h.opts.timeout
	)
	if timeSinceLastSend > timeout {
		logger.Warnf("unable to send heartbeats for %s; timing out", timeSinceLastSend.String())
		go h.opts.timeoutFn(h.lastHeartbeatTs)
	}
}

func (h *heatbeater) stop() error {
	if !h.isRunning() {
		return fmt.Errorf("heartbeater not running")
	}

	h.msgChan <- heartbeatMsg{stop: true}
	<-h.doneCh
	return nil
}

func (h *heatbeater) close() error {
	if h.isClosed() {
		return fmt.Errorf("heartbeater already closed")
	}

	atomic.StoreInt32(&h.closed, 1)
	h.stop()
	close(h.msgChan)
	close(h.doneCh)

	var err error
	if h.conn != nil {
		err = h.conn.Close()
		h.conn = nil
	}
	return err
}

func (h *heatbeater) notifyProcessTermination(reason string) {
	h.msgChan <- heartbeatMsg{
		processTerminate: true,
		err:              reason,
	}
}

func (h *heatbeater) notifyOverwrite(reason string) {
	h.msgChan <- heartbeatMsg{
		overwritten: true,
		err:         reason,
	}
}

type heartbeatMsg struct {
	stop             bool
	processTerminate bool
	overwritten      bool
	err              string
}
