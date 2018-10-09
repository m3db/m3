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

package client

import (
	"errors"
	"math/rand"
	"net"
	"sync"
	"time"

	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/retry"

	"github.com/uber-go/tally"
)

const (
	tcpProtocol = "tcp"
)

var (
	errNoActiveConnection = errors.New("no active connection")
)

type sleepFn func(time.Duration)
type connectWithLockFn func() error
type writeWithLockFn func([]byte) error

// connection is a persistent connection that retries establishing
// connection with exponential backoff if the connection goes down.
type connection struct {
	sync.Mutex

	addr           string
	connTimeout    time.Duration
	writeTimeout   time.Duration
	keepAlive      bool
	initThreshold  int
	multiplier     int
	maxThreshold   int
	maxDuration    time.Duration
	writeRetryOpts retry.Options
	rngFn          retry.RngFn

	conn                    *net.TCPConn
	numFailures             int
	threshold               int
	lastConnectAttemptNanos int64
	metrics                 connectionMetrics

	// These are for testing purposes.
	nowFn             clock.NowFn
	sleepFn           sleepFn
	connectWithLockFn connectWithLockFn
	writeWithLockFn   writeWithLockFn
}

// newConnection creates a new connection.
func newConnection(addr string, opts ConnectionOptions) *connection {
	c := &connection{
		addr:           addr,
		connTimeout:    opts.ConnectionTimeout(),
		writeTimeout:   opts.WriteTimeout(),
		keepAlive:      opts.ConnectionKeepAlive(),
		initThreshold:  opts.InitReconnectThreshold(),
		multiplier:     opts.ReconnectThresholdMultiplier(),
		maxThreshold:   opts.MaxReconnectThreshold(),
		maxDuration:    opts.MaxReconnectDuration(),
		writeRetryOpts: opts.WriteRetryOptions(),
		rngFn:          rand.New(rand.NewSource(time.Now().UnixNano())).Int63n,
		nowFn:          opts.ClockOptions().NowFn(),
		sleepFn:        time.Sleep,
		threshold:      opts.InitReconnectThreshold(),
		metrics:        newConnectionMetrics(opts.InstrumentOptions().MetricsScope()),
	}
	c.connectWithLockFn = c.connectWithLock
	c.writeWithLockFn = c.writeWithLock

	c.Lock()
	if err := c.connectWithLockFn(); err != nil {
		c.numFailures++
	}
	c.Unlock()

	return c
}

// Write sends data onto the connection, and attempts to re-establish
// connection if the connection is down.
func (c *connection) Write(data []byte) error {
	var err error
	c.Lock()
	if c.conn == nil {
		if err = c.checkReconnectWithLock(); err != nil {
			c.numFailures++
			c.Unlock()
			return err
		}
	}
	if err = c.writeAttemptWithLock(data); err == nil {
		c.Unlock()
		return nil
	}
	for i := 1; i <= c.writeRetryOpts.MaxRetries(); i++ {
		if backoffDur := time.Duration(retry.BackoffNanos(
			i,
			c.writeRetryOpts.Jitter(),
			c.writeRetryOpts.BackoffFactor(),
			c.writeRetryOpts.InitialBackoff(),
			c.writeRetryOpts.MaxBackoff(),
			c.rngFn,
		)); backoffDur > 0 {
			c.sleepFn(backoffDur)
		}
		c.metrics.writeRetries.Inc(1)
		if err = c.writeAttemptWithLock(data); err == nil {
			c.Unlock()
			return nil
		}
	}
	c.numFailures++
	c.Unlock()
	return err
}

func (c *connection) Close() {
	c.Lock()
	c.closeWithLock()
	c.Unlock()
}

// writeAttemptWithLock attempts to establish a new connection and writes raw bytes
// to the connection while holding the write lock.
// If the write succeeds, c.conn is guaranteed to be a valid connection on return.
// If the write fails, c.conn is guaranteed to be nil on return.
func (c *connection) writeAttemptWithLock(data []byte) error {
	if c.conn == nil {
		if err := c.connectWithLockFn(); err != nil {
			return err
		}
	}
	if err := c.writeWithLockFn(data); err != nil {
		c.closeWithLock()
		return err
	}
	return nil
}

func (c *connection) connectWithLock() error {
	c.lastConnectAttemptNanos = c.nowFn().UnixNano()
	conn, err := net.DialTimeout(tcpProtocol, c.addr, c.connTimeout)
	if err != nil {
		c.metrics.connectError.Inc(1)
		return err
	}

	tcpConn := conn.(*net.TCPConn)
	if err := tcpConn.SetKeepAlive(c.keepAlive); err != nil {
		c.metrics.setKeepAliveError.Inc(1)
	}

	if c.conn != nil {
		c.conn.Close() // nolint: errcheck
	}
	c.conn = tcpConn
	return nil
}

func (c *connection) checkReconnectWithLock() error {
	// If we haven't accumulated enough failures to warrant another reconnect
	// and we haven't past the maximum duration since the last time we attempted
	// to connect then we simply return false without reconnecting.
	tooManyFailures := c.numFailures >= c.threshold
	sufficientTimePassed := c.nowFn().UnixNano()-c.lastConnectAttemptNanos >= c.maxDuration.Nanoseconds()
	if !tooManyFailures && !sufficientTimePassed {
		return errNoActiveConnection
	}
	err := c.connectWithLockFn()
	if err == nil {
		c.resetWithLock()
		return nil
	}

	// Only raise the threshold when it is crossed, not when the max duration is reached.
	if tooManyFailures && c.threshold < c.maxThreshold {
		newThreshold := c.threshold * c.multiplier
		if newThreshold > c.maxThreshold {
			newThreshold = c.maxThreshold
		}
		c.threshold = newThreshold
	}
	return err
}

func (c *connection) writeWithLock(data []byte) error {
	if err := c.conn.SetWriteDeadline(c.nowFn().Add(c.writeTimeout)); err != nil {
		c.metrics.setWriteDeadlineError.Inc(1)
	}
	if _, err := c.conn.Write(data); err != nil {
		c.metrics.writeError.Inc(1)
		return err
	}
	return nil
}

func (c *connection) resetWithLock() {
	c.numFailures = 0
	c.threshold = c.initThreshold
}

func (c *connection) closeWithLock() {
	if c.conn != nil {
		c.conn.Close() // nolint: errcheck
	}
	c.conn = nil
}

const (
	errorMetric     = "errors"
	errorMetricType = "error-type"
)

type connectionMetrics struct {
	connectError          tally.Counter
	writeError            tally.Counter
	writeRetries          tally.Counter
	setKeepAliveError     tally.Counter
	setWriteDeadlineError tally.Counter
}

func newConnectionMetrics(scope tally.Scope) connectionMetrics {
	return connectionMetrics{
		connectError: scope.Tagged(map[string]string{errorMetricType: "connect"}).
			Counter(errorMetric),
		writeError: scope.Tagged(map[string]string{errorMetricType: "write"}).
			Counter(errorMetric),
		writeRetries: scope.Tagged(map[string]string{"action": "write"}).Counter("retries"),
		setKeepAliveError: scope.Tagged(map[string]string{errorMetricType: "tcp-keep-alive"}).
			Counter(errorMetric),
		setWriteDeadlineError: scope.Tagged(map[string]string{errorMetricType: "set-write-deadline"}).
			Counter(errorMetric),
	}
}
