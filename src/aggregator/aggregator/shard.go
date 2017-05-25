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

package aggregator

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

var (
	errAggregatorShardClosed = errors.New("aggregator shard is closed")
)

type addMetricWithPoliciesListFn func(mu unaggregated.MetricUnion, pl policy.PoliciesList) error

type aggregatorShard struct {
	sync.RWMutex

	shard                       uint32
	closed                      bool
	metricMap                   *metricMap
	addMetricWithPoliciesListFn addMetricWithPoliciesListFn
}

func newAggregatorShard(shard uint32, opts Options) *aggregatorShard {
	s := &aggregatorShard{
		shard:     shard,
		metricMap: newMetricMap(opts),
	}
	s.addMetricWithPoliciesListFn = s.metricMap.AddMetricWithPoliciesList
	return s
}

func (s *aggregatorShard) ID() uint32 { return s.shard }

func (s *aggregatorShard) AddMetricWithPoliciesList(
	mu unaggregated.MetricUnion,
	pl policy.PoliciesList,
) error {
	s.RLock()
	if s.closed {
		s.RUnlock()
		return errAggregatorShardClosed
	}
	err := s.addMetricWithPoliciesListFn(mu, pl)
	s.RUnlock()
	return err
}

func (s *aggregatorShard) Tick(target time.Duration) tickResult {
	return s.metricMap.Tick(target)
}

func (s *aggregatorShard) Close() {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return
	}
	s.closed = true
	s.metricMap.Close()
}
