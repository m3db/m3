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
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/protocol/msgpack"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/net"
	"github.com/m3db/m3x/sync"

	"github.com/uber-go/tally"
)

type serverMetrics struct {
	openConnections tally.Gauge
	queueSize       tally.Gauge
	decodeErrors    tally.Counter
	invalidMetrics  tally.Counter
}

func newServerMetrics(scope tally.Scope) serverMetrics {
	return serverMetrics{
		openConnections: scope.Gauge("open-connections"),
		queueSize:       scope.Gauge("queue-size"),
		decodeErrors:    scope.Counter("decode-errors"),
		invalidMetrics:  scope.Counter("invalid-metrics"),
	}
}

type addConnectionFn func(conn net.Conn) bool
type removeConnectionFn func(conn net.Conn)
type handleConnectionFn func(conn net.Conn)
type processPacketFn func(p packet)

// Server is a server that receives incoming connections and delegates
// to the handler to process incoming data
type Server struct {
	sync.Mutex

	listener     net.Listener
	aggregator   aggregator.Aggregator
	opts         Options
	log          xlog.Logger
	iteratorPool msgpack.UnaggregatedIteratorPool

	queue     *packetQueue
	workers   xsync.WorkerPool
	wgWorkers sync.WaitGroup
	conns     []net.Conn
	wgConns   sync.WaitGroup
	closed    int32
	numConns  int32
	metrics   serverMetrics

	addConnectionFn    addConnectionFn
	removeConnectionFn removeConnectionFn
	handleConnectionFn handleConnectionFn
	processPacketFn    processPacketFn
}

// NewServer creates a new server
func NewServer(l net.Listener, agg aggregator.Aggregator, opts Options) *Server {
	instrumentOpts := opts.InstrumentOptions()
	scope := instrumentOpts.MetricsScope().SubScope("server")

	s := &Server{
		listener:     l,
		aggregator:   agg,
		opts:         opts,
		log:          instrumentOpts.Logger(),
		iteratorPool: opts.IteratorPool(),
		queue:        newPacketQueue(opts.PacketQueueSize(), instrumentOpts),
		metrics:      newServerMetrics(scope),
	}

	// Set up the connection functions
	s.addConnectionFn = s.addConnection
	s.removeConnectionFn = s.removeConnection
	s.handleConnectionFn = s.handleConnection
	s.processPacketFn = s.processPacket

	// Start the workers to process incoming data
	numWorkers := opts.WorkerPoolSize()
	s.workers = xsync.NewWorkerPool(numWorkers)
	s.workers.Init()
	s.wgWorkers.Add(numWorkers)
	s.workers.Go(s.processPackets)

	// Start reporting metrics
	go s.reportMetrics()

	return s
}

// ListenAndServe listens for new incoming connections and adds them
// to the list of known connections. The call blocks until the server
// is closed
func (s *Server) ListenAndServe() error {
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

	// Close the server
	s.listener.Close()

	// Wait for all connection handlers to finish
	s.wgConns.Wait()

	// There should be no new packets so it's safe
	// to close the packet queue
	s.queue.Close()

	// Wait for all workers to finish dequeuing existing
	// packets in the queue
	s.wgWorkers.Wait()
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
		s.queue.Enqueue(packet{metric: metric, policies: policies})
	}

	// If there is an error during decoding, it's likely due to a broken connection
	if err := it.Err(); err != nil {
		s.log.Errorf("decode error: %v", err)
		s.metrics.decodeErrors.Inc(1)
	}
}

func (s *Server) processPackets() {
	defer s.wgWorkers.Done()

	for {
		p, ok := s.queue.Dequeue()
		if !ok {
			return
		}
		s.processPacketFn(p)
	}
}

func (s *Server) processPacket(p packet) {
	switch p.metric.Type {
	case unaggregated.CounterType:
		s.aggregator.AddCounterWithPolicies(unaggregated.CounterWithPolicies{
			Counter:           p.metric.Counter(),
			VersionedPolicies: p.policies,
		})
	case unaggregated.BatchTimerType:
		s.aggregator.AddBatchTimerWithPolicies(unaggregated.BatchTimerWithPolicies{
			BatchTimer:        p.metric.BatchTimer(),
			VersionedPolicies: p.policies,
		})
	case unaggregated.GaugeType:
		s.aggregator.AddGaugeWithPolicies(unaggregated.GaugeWithPolicies{
			Gauge:             p.metric.Gauge(),
			VersionedPolicies: p.policies,
		})
	default:
		s.metrics.invalidMetrics.Inc(1)
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
