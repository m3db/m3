// Copyright (c) 2016 Uber Technologies, Inc.
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

package server

import (
	"bufio"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/close"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/net"

	"github.com/uber-go/tally"
)

type serverMetrics struct {
	openConnections tally.Gauge
	queueSize       tally.Gauge
	enqueueErrors   tally.Counter
	decodeErrors    tally.Counter
}

func newServerMetrics(scope tally.Scope) serverMetrics {
	return serverMetrics{
		openConnections: scope.Gauge("open-connections"),
		queueSize:       scope.Gauge("queue-size"),
		enqueueErrors:   scope.Counter("enqueue-errors"),
		decodeErrors:    scope.Counter("decode-errors"),
	}
}

type addConnectionFn func(conn net.Conn) bool
type removeConnectionFn func(conn net.Conn)
type handleConnectionFn func(conn net.Conn)

// Server is a server that receives incoming connections and delegates
// to the processor to process incoming data
type Server struct {
	sync.Mutex

	address      string
	listener     net.Listener
	opts         Options
	log          xlog.Logger
	iteratorPool msgpack.UnaggregatedIteratorPool

	closed    int32
	numConns  int32
	conns     []net.Conn
	wgConns   sync.WaitGroup
	queue     *packetQueue
	processor *packetProcessor
	metrics   serverMetrics

	addConnectionFn    addConnectionFn
	removeConnectionFn removeConnectionFn
	handleConnectionFn handleConnectionFn
}

// NewServer creates a new server
func NewServer(address string, aggregator aggregator.Aggregator, opts Options) *Server {
	clockOpts := opts.ClockOptions()
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope().SubScope("server")
	queue := newPacketQueue(opts.PacketQueueSize(), clockOpts, instrumentOpts.SetMetricsScope(scope))
	processor := newPacketProcessor(queue, aggregator, opts.WorkerPoolSize(), clockOpts, instrumentOpts)
	s := &Server{
		address:      address,
		opts:         opts,
		log:          instrumentOpts.Logger(),
		iteratorPool: opts.IteratorPool(),
		queue:        queue,
		processor:    processor,
		metrics:      newServerMetrics(scope),
	}

	// Set up the connection functions
	s.addConnectionFn = s.addConnection
	s.removeConnectionFn = s.removeConnection
	s.handleConnectionFn = s.handleConnection

	// Start reporting metrics
	go s.reportMetrics()

	return s
}

// ListenAndServe starts listening to new incoming connections and
// handles data from those connections
func (s *Server) ListenAndServe() (xclose.SimpleCloser, error) {
	listener, err := net.Listen("tcp", s.address)
	if err != nil {
		return nil, err
	}
	s.listener = listener
	go s.serve()
	return s, nil
}

func (s *Server) serve() error {
	connCh, errCh := xnet.StartAcceptLoop(s.listener, s.opts.Retrier())
	for conn := range connCh {
		if !s.addConnectionFn(conn) {
			conn.Close()
		} else {
			s.wgConns.Add(1)
			go s.handleConnectionFn(conn)
		}
	}
	return <-errCh
}

// Close closes all open connections and stops server from listening
func (s *Server) Close() {
	s.Lock()
	if atomic.LoadInt32(&s.closed) == 1 {
		s.Unlock()
		return
	}
	atomic.StoreInt32(&s.closed, 1)
	openConns := make([]net.Conn, len(s.conns))
	copy(openConns, s.conns)
	s.Unlock()

	// Close all open connections
	for _, conn := range openConns {
		conn.Close()
	}

	// Close the listener
	s.listener.Close()

	// Wait for all connection handlers to finish
	s.wgConns.Wait()

	// Close the packet queue
	s.queue.Close()

	// Close the packet processor
	s.processor.Close()
}

func (s *Server) addConnection(conn net.Conn) bool {
	s.Lock()
	defer s.Unlock()

	if atomic.LoadInt32(&s.closed) == 1 {
		return false
	}
	s.conns = append(s.conns, conn)
	atomic.AddInt32(&s.numConns, 1)
	return true
}

func (s *Server) removeConnection(conn net.Conn) {
	s.Lock()
	defer s.Unlock()

	numConns := len(s.conns)
	for i := 0; i < numConns; i++ {
		if s.conns[i] == conn {
			// Move the last connection to i and reduce the number of connections by 1
			s.conns[i] = s.conns[numConns-1]
			s.conns = s.conns[:numConns-1]
			atomic.AddInt32(&s.numConns, -1)
			return
		}
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		conn.Close()
		s.removeConnectionFn(conn)
		s.wgConns.Done()
	}()

	it := s.iteratorPool.Get()
	it.Reset(bufio.NewReader(conn))
	defer it.Close()

	// Iterate over the incoming metrics stream and queue up metrics
	for it.Next() {
		metric, policies := it.Value()
		if err := s.queue.Enqueue(packet{metric: metric, policies: policies}); err != nil {
			s.log.WithFields(
				xlog.NewLogField("metric", metric),
				xlog.NewLogField("policies", policies),
				xlog.NewLogErrField(err),
			).Errorf("server enqueue error")
			s.metrics.enqueueErrors.Inc(1)
		}
	}

	// If there is an error during decoding, it's likely due to a broken connection
	if err := it.Err(); err != nil {
		s.log.Errorf("decode error: %v", err)
		s.metrics.decodeErrors.Inc(1)
	}
}

func (s *Server) reportMetrics() {
	interval := s.opts.InstrumentOptions().ReportInterval()
	t := time.Tick(interval)

	for {
		<-t
		if atomic.LoadInt32(&s.closed) == 1 {
			return
		}
		s.metrics.openConnections.Update(int64(atomic.LoadInt32(&s.numConns)))
		s.metrics.queueSize.Update(int64(s.queue.Len()))
	}
}
