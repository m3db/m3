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

package handler

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
)

const (
	tcpProtocol = "tcp"
)

var (
	errEmptyServerList = errors.New("empty server list")
	errHandlerClosed   = errors.New("handler is closed")
	errBufferQueueFull = errors.New("buffer queue is full")
)

type dropType int

const (
	dropCurrent dropType = iota + 1
	dropOldestInQueue
)

type dialWithTimeoutFn func(protocol string, addr string, timeout time.Duration) (net.Conn, error)
type writeToConnFn func(conn *net.TCPConn, data []byte) (int, error)

type forwardHandler struct {
	sync.RWMutex

	servers             []string
	log                 xlog.Logger
	connectTimeout      time.Duration
	connectionKeepAlive bool
	reconnectRetrier    xretry.Retrier

	bufCh             chan msgpack.Buffer
	dialWithTimeoutFn dialWithTimeoutFn
	writeToConnFn     writeToConnFn
	wg                sync.WaitGroup
	closed            bool
}

// NewForwardHandler creates a new forwarding handler.
// TODO(xichen): add metrics.
func NewForwardHandler(
	servers []string,
	opts ForwardHandlerOptions,
) (aggregator.Handler, error) {
	numServers := len(servers)
	if numServers == 0 {
		return nil, errEmptyServerList
	}

	h := &forwardHandler{
		servers:             servers,
		log:                 opts.InstrumentOptions().Logger(),
		connectTimeout:      opts.ConnectTimeout(),
		connectionKeepAlive: opts.ConnectionKeepAlive(),
		reconnectRetrier:    opts.ReconnectRetrier(),
		bufCh:               make(chan msgpack.Buffer, opts.QueueSize()),
		dialWithTimeoutFn:   net.DialTimeout,
		writeToConnFn:       writeToConn,
	}

	// Initiate the connections.
	h.wg.Add(numServers)
	for _, addr := range servers {
		conn, err := h.tryConnect(addr)
		if err != nil {
			h.log.WithFields(
				xlog.NewLogField("address", addr),
				xlog.NewLogErrField(err),
			).Error("error connecting to server")
		}
		go h.forwardToConn(addr, conn)
	}

	return h, nil
}

func (h *forwardHandler) Handle(buffer msgpack.Buffer) error {
	// NB(xichen): the buffer contains newly flushed data so it's preferrable to keep
	// it and drop the oldest buffer in queue in case the queue is full.
	return h.enqueue(buffer, dropOldestInQueue)
}

func (h *forwardHandler) Close() {
	h.Lock()
	if h.closed {
		h.Unlock()
		return
	}
	h.closed = true
	close(h.bufCh)
	h.Unlock()

	// Wait for the queue to be drained.
	h.wg.Wait()
}

func (h *forwardHandler) tryConnect(addr string) (*net.TCPConn, error) {
	conn, err := h.dialWithTimeoutFn(tcpProtocol, addr, h.connectTimeout)
	if err != nil {
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	tcpConn.SetKeepAlive(h.connectionKeepAlive)
	return tcpConn, nil
}

func (h *forwardHandler) enqueue(buffer msgpack.Buffer, dropType dropType) error {
	h.RLock()
	if h.closed {
		h.RUnlock()
		return errHandlerClosed
	}
	for {
		select {
		case h.bufCh <- buffer:
			h.RUnlock()
			return nil
		default:
			// TODO(xichen): add metrics for dropping current.
			if dropType == dropCurrent {
				h.RUnlock()
				buffer.Close()
				return errBufferQueueFull
			}
		}

		// Drop oldest in queue to make room for new buffer.
		select {
		case buf := <-h.bufCh:
			// TODO(xichen): add metrics for discarding buffer.
			buf.Close()
		default:
		}
	}
	panic("unreachable code")
	return nil
}

func (h *forwardHandler) forwardToConn(addr string, conn *net.TCPConn) {
	defer h.wg.Done()

	var err error
	continueFn := func(int) bool {
		h.RLock()
		closed := h.closed
		h.RUnlock()
		return !closed
	}
	connectFn := func() error {
		conn, err = h.tryConnect(addr)
		return err
	}

	for {
		// Retry establishing connection until either success or the handler is closed.
		for conn == nil {
			attemptErr := h.reconnectRetrier.AttemptWhile(continueFn, connectFn)
			if attemptErr == xretry.ErrWhileConditionFalse {
				return
			}
		}

		// Dequeue buffers and forward to server.
		for {
			buf, ok := <-h.bufCh
			if !ok {
				conn.Close()
				return
			}
			_, writeErr := h.writeToConnFn(conn, buf.Bytes())
			if writeErr == nil {
				buf.Close()
				continue
			}
			h.log.WithFields(
				xlog.NewLogField("address", addr),
				xlog.NewLogErrField(writeErr),
			).Error("error writing to server")

			// NB(xichen): the buffer contains the oldest flushed data in queue
			// so it's preferrable to drop it in case the queue is full.
			if enqueueErr := h.enqueue(buf, dropCurrent); enqueueErr != nil {
				h.log.WithFields(
					xlog.NewLogField("address", addr),
					xlog.NewLogErrField(enqueueErr),
				).Error("error enqueuing the buffer")
			}

			// Close and reset the connection.
			conn.Close()
			conn = nil
			break
		}
	}
}

func writeToConn(conn *net.TCPConn, data []byte) (int, error) {
	return conn.Write(data)
}
