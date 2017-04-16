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

package msgpack

import (
	"errors"
	"sync"

	"github.com/m3db/m3collector/backend"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

var (
	errServerIsOpenOrClosed    = errors.New("server is already open or closed")
	errServerIsNotOpenOrClosed = errors.New("server is not open or closed")
)

type serverState int

const (
	serverNotOpen serverState = iota
	serverOpen
	serverClosed
)

// server partitions metrics and send them via different routes based on their partitions.
type server struct {
	sync.RWMutex

	opts      ServerOptions
	writerMgr instanceWriterManager
	topology  topology
	state     serverState
}

// NewServer creates a new server.
func NewServer(opts ServerOptions) backend.Server {
	writerMgr := newInstanceWriterManager(opts)
	s := &server{
		opts:      opts,
		writerMgr: writerMgr,
		topology:  newTopology(writerMgr, opts),
	}
	return s
}

func (s *server) Open() error {
	s.Lock()
	defer s.Unlock()

	if s.state != serverNotOpen {
		return errServerIsOpenOrClosed
	}
	s.state = serverOpen
	return s.topology.Open()
}

func (s *server) WriteCounterWithPolicies(
	id []byte,
	val int64,
	vp policy.VersionedPolicies,
) error {
	mu := unaggregated.MetricUnion{
		Type:       unaggregated.CounterType,
		ID:         id,
		CounterVal: val,
	}
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	err := s.topology.Route(mu, vp)
	s.RUnlock()
	return err
}

func (s *server) WriteBatchTimerWithPolicies(
	id []byte,
	val []float64,
	vp policy.VersionedPolicies,
) error {
	mu := unaggregated.MetricUnion{
		Type:          unaggregated.BatchTimerType,
		ID:            id,
		BatchTimerVal: val,
	}
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	err := s.topology.Route(mu, vp)
	s.RUnlock()
	return err
}

func (s *server) WriteGaugeWithPolicies(
	id []byte,
	val float64,
	vp policy.VersionedPolicies,
) error {
	mu := unaggregated.MetricUnion{
		Type:     unaggregated.GaugeType,
		ID:       id,
		GaugeVal: val,
	}
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	err := s.topology.Route(mu, vp)
	s.RUnlock()
	return err
}

func (s *server) Flush() error {
	s.RLock()
	if s.state != serverOpen {
		s.RUnlock()
		return errServerIsNotOpenOrClosed
	}
	err := s.writerMgr.Flush()
	s.RUnlock()
	return err
}

func (s *server) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.state != serverOpen {
		return errServerIsNotOpenOrClosed
	}
	s.state = serverClosed
	s.topology.Close()
	return s.writerMgr.Close()
}
