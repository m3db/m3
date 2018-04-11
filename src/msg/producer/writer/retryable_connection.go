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

package writer

import (
	"bufio"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
	"go.uber.org/atomic"
)

const (
	defaultRetryForever = true
)

var (
	errConnNotInitialized = errors.New("connection not initialized")
)

type retryableConnectionMetrics struct {
	resetConn         tally.Counter
	resetError        tally.Counter
	connectError      tally.Counter
	setKeepAliveError tally.Counter
}

func newRetryableConnectionMetrics(scope tally.Scope) retryableConnectionMetrics {
	return retryableConnectionMetrics{
		resetConn:         scope.Counter("reset-conn"),
		resetError:        scope.Counter("reset-conn-error"),
		connectError:      scope.Counter("connect-error"),
		setKeepAliveError: scope.Counter("set-keep-alive-error"),
	}
}

type connectFn func(addr string) (net.Conn, error)

type retryableConnection struct {
	addr            string
	retrier         retry.Retrier
	opts            ConnectionOptions
	resetDelayNanos int64
	logger          log.Logger

	connLock       sync.RWMutex
	conn           net.Conn
	w              *bufio.Writer
	r              *bufio.Reader
	lastResetNanos int64
	initialized    bool
	closed         *atomic.Bool
	doneCh         chan struct{}
	resetCh        chan struct{}
	wg             sync.WaitGroup
	m              retryableConnectionMetrics

	nowFn     clock.NowFn
	connectFn connectFn
}

// newRetryableConnection creates a connection that is automatically
// reconnected on errors.
func newRetryableConnection(
	addr string,
	opts ConnectionOptions,
) *retryableConnection {
	if opts == nil {
		opts = NewConnectionOptions()
	}
	c := &retryableConnection{
		addr:            addr,
		retrier:         retry.NewRetrier(opts.RetryOptions().SetForever(defaultRetryForever)),
		opts:            opts,
		resetDelayNanos: int64(opts.ResetDelay()),
		logger:          opts.InstrumentOptions().Logger(),
		w:               bufio.NewWriterSize(nil, opts.WriteBufferSize()),
		r:               bufio.NewReaderSize(nil, opts.ReadBufferSize()),
		lastResetNanos:  0,
		initialized:     false,
		closed:          atomic.NewBool(false),
		doneCh:          make(chan struct{}),
		resetCh:         make(chan struct{}, 1),
		m:               newRetryableConnectionMetrics(opts.InstrumentOptions().MetricsScope()),
		nowFn:           time.Now,
	}

	c.connectFn = c.connectOnce
	if err := c.resetWithConnectFn(c.connectFn); err != nil {
		c.NotifyReset()
	}
	return c
}

func (c *retryableConnection) Read(p []byte) (int, error) {
	c.connLock.RLock()
	if !c.initialized {
		c.connLock.RUnlock()
		return 0, errConnNotInitialized
	}
	n, err := c.r.Read(p)
	c.connLock.RUnlock()
	return n, err
}

func (c *retryableConnection) Write(p []byte) (int, error) {
	c.connLock.RLock()
	if !c.initialized {
		c.connLock.RUnlock()
		return 0, errConnNotInitialized
	}
	n, err := c.w.Write(p)
	c.connLock.RUnlock()
	return n, err
}

func (c *retryableConnection) NotifyReset() {
	select {
	case c.resetCh <- struct{}{}:
	default:
	}
}

func (c *retryableConnection) Init() {
	c.wg.Add(1)
	go func() {
		c.resetConnectionForever()
		c.wg.Done()
	}()
}

func (c *retryableConnection) resetConnectionForever() {
	for {
		select {
		case <-c.resetCh:
			c.resetWithConnectFn(c.connectWithRetry)
		case <-c.doneCh:
			c.connLock.RLock()
			if c.conn != nil {
				c.conn.Close()
			}
			c.connLock.RUnlock()
			return
		}
	}
}

func (c *retryableConnection) resetWithConnectFn(fn connectFn) error {
	// Avoid resetting too frequent.
	if c.nowFn().UnixNano() < c.lastResetNanos+c.resetDelayNanos {
		return nil
	}
	c.m.resetConn.Inc(1)
	conn, err := fn(c.addr)
	if err != nil {
		c.m.resetError.Inc(1)
		c.logger.Errorf("could not connect to %s, %v", c.addr, err)
		return err
	}
	c.reset(conn)
	return nil
}

func (c *retryableConnection) connectWithRetry(addr string) (net.Conn, error) {
	continueFn := func(int) bool {
		return !c.isClosed()
	}
	var (
		conn net.Conn
		err  error
	)
	fn := func() error {
		conn, err = c.connectFn(addr)
		return err
	}
	if attemptErr := c.retrier.AttemptWhile(
		continueFn,
		fn,
	); attemptErr != nil {
		return nil, fmt.Errorf("failed to connect to address %s, %v", addr, attemptErr)
	}
	return conn, nil
}

func (c *retryableConnection) connectOnce(addr string) (net.Conn, error) {
	conn, err := net.DialTimeout("tcp", addr, c.opts.DialTimeout())
	if err != nil {
		c.m.connectError.Inc(1)
		return nil, err
	}
	tcpConn := conn.(*net.TCPConn)
	if err = tcpConn.SetKeepAlive(true); err != nil {
		c.m.setKeepAliveError.Inc(1)
	}
	return tcpConn, nil
}

func (c *retryableConnection) reset(conn net.Conn) {
	// Close the connection to wake up potential blocking write/read calls.
	if c.conn != nil {
		c.conn.Close()
	}

	c.connLock.Lock()
	defer c.connLock.Unlock()

	c.conn = conn
	c.w.Reset(conn)
	c.r.Reset(conn)
	c.initialized = true
	c.lastResetNanos = c.nowFn().UnixNano()
}

func (c *retryableConnection) Close() error {
	if !c.closed.CAS(false, true) {
		return nil
	}
	close(c.doneCh)
	c.wg.Wait()
	return nil
}

func (c *retryableConnection) isClosed() bool {
	return c.closed.Load()
}
