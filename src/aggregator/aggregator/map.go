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

package aggregator

import (
	"sync"

	"github.com/m3db/m3aggregator/id"
	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/clock"
)

type metricMap struct {
	sync.RWMutex

	opts      Options
	nowFn     clock.NowFn
	entryPool EntryPool

	lists   *MetricLists
	entries map[id.Hash]*Entry
}

func newMetricMap(lists *MetricLists, opts Options) *metricMap {
	return &metricMap{
		opts:      opts,
		nowFn:     opts.ClockOptions().NowFn(),
		entryPool: opts.EntryPool(),
		lists:     lists,
		entries:   make(map[id.Hash]*Entry),
	}
}

func (m *metricMap) AddMetricWithPolicies(
	mu unaggregated.MetricUnion,
	policies policy.VersionedPolicies,
) error {
	e := m.findOrCreate(mu.ID)
	err := e.AddMetricWithPolicies(mu, policies)
	e.DecWriter()
	return err
}

func (m *metricMap) DeleteExpired() {
	now := m.nowFn()
	m.Lock()
	for key, entry := range m.entries {
		if entry.MaybeExpire(now) {
			delete(m.entries, key)
		}
	}
	m.Unlock()
}

func (m *metricMap) findOrCreate(mid metric.ID) *Entry {
	idHash := id.HashFn(mid)
	m.RLock()
	e, exists := m.entries[idHash]
	if exists {
		// NB(xichen): it is important to increase number of writers
		// within a lock so we can account for active acounters
		// when deleting expired entries
		e.IncWriter()
		m.RUnlock()
		return e
	}
	m.RUnlock()

	m.Lock()
	e, exists = m.entries[idHash]
	if !exists {
		e = m.entryPool.Get()
		e.ResetSetData(m.lists)
		m.entries[idHash] = e
	}
	e.IncWriter()
	m.Unlock()

	return e
}
