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

package r2

import (
	"sync"
	"time"

	"github.com/uber-go/tally"
)

const (
	metricSuccess = "success"
	metricError   = "error"
)

type serviceMetrics struct {
	sync.RWMutex

	scope   tally.Scope
	metrics map[serviceMetricID]serviceMetric
}

type serviceMetric struct {
	timer   tally.Timer
	counter tally.Counter
}

type serviceMetricID struct {
	path   string
	method string
	status string
}

func (sm *serviceMetrics) recordMetric(path, method, namespace string, d time.Duration, err error) {
	status := metricSuccess
	if err != nil {
		status = metricError
	}

	id := newServiceMetricID(path, method, status)

	sm.RLock()
	m, exists := sm.metrics[id]
	sm.RUnlock()
	if exists {
		m.Record(d)
		return
	}

	sm.Lock()
	defer sm.Unlock()
	scope := sm.scope.Tagged(map[string]string{
		"namespace":   namespace,
		"http-method": method,
		"route-path":  path,
	})
	m = newServiceMetric(scope, status)
	sm.metrics[id] = m
	m.Record(d)
}

func newServiceMetrics(scope tally.Scope) serviceMetrics {
	return serviceMetrics{scope: scope, metrics: make(map[serviceMetricID]serviceMetric)}
}

func newServiceMetricID(path, method, status string) serviceMetricID {
	return serviceMetricID{
		path:   path,
		method: method,
		status: status,
	}
}

func newServiceMetric(s tally.Scope, status string) serviceMetric {
	return serviceMetric{
		timer:   s.Timer(status),
		counter: s.Counter(status),
	}
}

func (m *serviceMetric) Record(d time.Duration) {
	m.timer.Record(d)
	m.counter.Inc(1)
}
