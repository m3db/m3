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

package client

import (
	"sync"

	"github.com/uber-go/tally"
)

type sessionMetrics struct {
	sync.RWMutex
	writeSuccess               tally.Counter
	writeErrors                tally.Counter
	writeNodesRespondingErrors []tally.Counter
	fetchSuccess               tally.Counter
	fetchErrors                tally.Counter
	fetchNodesRespondingErrors []tally.Counter
	streamFromPeersMetrics     map[uint32]streamFromPeersMetrics
}

func newSessionMetrics(scope tally.Scope) sessionMetrics {
	return sessionMetrics{
		writeSuccess:           scope.Counter("write.success"),
		writeErrors:            scope.Counter("write.errors"),
		fetchSuccess:           scope.Counter("fetch.success"),
		fetchErrors:            scope.Counter("fetch.errors"),
		streamFromPeersMetrics: make(map[uint32]streamFromPeersMetrics),
	}
}

type streamFromPeersMetrics struct {
	fetchBlocksFromPeers      tally.Gauge
	metadataFetches           tally.Gauge
	metadataFetchBatchCall    tally.Counter
	metadataFetchBatchSuccess tally.Counter
	metadataFetchBatchError   tally.Counter
	metadataReceived          tally.Counter
	blocksEnqueueChannel      tally.Gauge
}

func (s *session) incWriteMetrics(consistencyResultErr error, respErrs int32) {
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		s.metrics.writeNodesRespondingErrors[idx].Inc(1)
	}
	if consistencyResultErr == nil {
		s.metrics.writeSuccess.Inc(1)
	} else {
		s.metrics.writeErrors.Inc(1)
	}
}

func (s *session) incFetchMetrics(consistencyResultErr error, respErrs int32) {
	if idx := s.nodesRespondingErrorsMetricIndex(respErrs); idx >= 0 {
		s.metrics.fetchNodesRespondingErrors[idx].Inc(1)
	}
	if consistencyResultErr == nil {
		s.metrics.fetchSuccess.Inc(1)
	} else {
		s.metrics.fetchErrors.Inc(1)
	}
}

func (s *session) nodesRespondingErrorsMetricIndex(respErrs int32) int32 {
	idx := respErrs - 1
	replicas := int32(s.Replicas())
	if respErrs > replicas {
		// Cap to the max replicas, we might get more errors
		// when a node is initializing a shard causing replicas + 1
		// nodes to respond to operations
		idx = replicas - 1
	}
	return idx
}
