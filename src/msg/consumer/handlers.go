// Copyright (c) 2018 Uber Technologies, Inc.
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

package consumer

import (
	"io"
	"net"

	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/server"
)

type consumerHandler struct {
	opts      Options
	mPool     *messagePool
	consumeFn ConsumeFn
	m         metrics
}

// NewConsumerHandler creates a new server handler with consumerFn.
func NewConsumerHandler(consumeFn ConsumeFn, opts Options) server.Handler {
	mPool := newMessagePool(opts.MessagePoolOptions())
	mPool.Init()
	return &consumerHandler{
		consumeFn: consumeFn,
		opts:      opts,
		mPool:     mPool,
		m:         newConsumerMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

func (h *consumerHandler) Handle(conn net.Conn) {
	c := newConsumer(conn, h.mPool, h.opts, h.m)
	c.Init()
	h.consumeFn(c)
}

func (h *consumerHandler) Close() {}

type messageHandler struct {
	opts      Options
	mPool     *messagePool
	messageFn MessageFn
	m         metrics
}

// NewMessageHandler creates a new server handler with messageFn.
func NewMessageHandler(messageFn MessageFn, opts Options) server.Handler {
	mPool := newMessagePool(opts.MessagePoolOptions())
	mPool.Init()
	return &messageHandler{
		messageFn: messageFn,
		opts:      opts,
		mPool:     mPool,
		m:         newConsumerMetrics(opts.InstrumentOptions().MetricsScope()),
	}
}

func (h *messageHandler) Handle(conn net.Conn) {
	c := newConsumer(conn, h.mPool, h.opts, h.m)
	c.Init()
	var (
		msgErr error
		msg    Message
	)
	for {
		msg, msgErr = c.Message()
		if msgErr != nil {
			break
		}
		h.messageFn(msg)
	}
	if msgErr != nil && msgErr != io.EOF {
		h.opts.InstrumentOptions().Logger().WithFields(log.NewErrField(msgErr)).Errorf("could not read message from consumer")
	}
	c.Close()
}

func (h *messageHandler) Close() {}
