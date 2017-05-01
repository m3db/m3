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

package block

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

const (
	wiredListEventsChannelLength = 65536
	wiredListSampleGaugesEvery   = 100
)

// WiredList is a database block wired list.
type WiredList struct {
	sync.RWMutex

	// Max wired blocks, must use atomic store and load to access.
	maxWired int64

	root      dbBlock
	len       int
	updatesCh chan *dbBlock
	doneCh    chan struct{}

	metrics wiredListMetrics
}

type wiredListMetrics struct {
	unwireable           tally.Gauge
	limit                tally.Gauge
	evicted              tally.Counter
	evictedAfterDuration tally.Timer
}

// NewWiredList returns a new database block wired list.
func NewWiredList(
	runtimeOptsMgr runtime.OptionsManager,
	iopts instrument.Options,
) *WiredList {
	scope := iopts.MetricsScope().
		SubScope("wired-list")
	l := &WiredList{
		metrics: wiredListMetrics{
			unwireable:           scope.Gauge("unwireable"),
			limit:                scope.Gauge("limit"),
			evicted:              scope.Counter("evicted"),
			evictedAfterDuration: scope.Timer("evicted-after-duration"),
		},
	}
	l.root.next = &l.root
	l.root.prev = &l.root
	runtimeOptsMgr.RegisterListener(l)
	return l
}

// SetRuntimeOptions sets the current runtime options to
// be consumed by the wired list
func (l *WiredList) SetRuntimeOptions(value runtime.Options) {
	atomic.StoreInt64(&l.maxWired, int64(value.MaxWiredBlocks()))
}

// Start starts processing the wired list
func (l *WiredList) Start() {
	l.Lock()
	defer l.Unlock()
	if l.updatesCh != nil {
		return
	}

	l.updatesCh = make(chan *dbBlock, wiredListEventsChannelLength)
	l.doneCh = make(chan struct{}, 1)
	go func() {
		i := 0
		for v := range l.updatesCh {
			l.processUpdateBlock(v)
			if i%wiredListSampleGaugesEvery == 0 {
				l.metrics.unwireable.Update(float64(l.len))
				l.metrics.limit.Update(float64(atomic.LoadInt64(&l.maxWired)))
			}
			i++
		}
		l.doneCh <- struct{}{}
	}()
}

// Stop stops processing the wired list
func (l *WiredList) Stop() {
	close(l.updatesCh)
	<-l.doneCh

	l.Lock()
	defer l.Unlock()
	l.updatesCh = nil
	close(l.doneCh)
	l.doneCh = nil
}

func (l *WiredList) update(v *dbBlock) {
	// Trigger for processing
	l.updatesCh <- v
}

func (l *WiredList) processUpdateBlock(v *dbBlock) {
	entry := v.wiredListEntry()

	unwireable := entry.wired &&
		entry.retriever != nil &&
		entry.retrieveID != nil

	// If already in list
	if l.exists(v) {
		// Push to back if wired and can unwire
		if unwireable {
			l.pushBack(v)
			return
		}

		// Otherwise remove from the list as not able to unwire anymore
		l.remove(v)
		return
	}

	// Not in the list, if wired and can unwire add it
	if unwireable {
		l.pushBack(v)
	}
}

func (l *WiredList) insert(v, at *dbBlock) {
	n := at.next
	at.next = v
	v.prev = at
	v.next = n
	v.nextPrevUpdatedAtUnixNano = time.Now().UnixNano()
	n.prev = v
	l.len++

	maxWired := int(atomic.LoadInt64(&l.maxWired))
	if maxWired <= 0 {
		// Not enforcing max wired blocks
		return
	}

	// Try to unwire all blocks possible
	for bl := l.root.next; l.len > maxWired && bl != &l.root; bl = bl.next {
		if bl.unwire() {
			// Successfully unwired the block
			l.remove(bl)

			l.metrics.evicted.Inc(1)

			lastUpdatedAt := time.Unix(0, bl.nextPrevUpdatedAtUnixNano)
			l.metrics.evictedAfterDuration.Record(time.Since(lastUpdatedAt))
		}
	}
}

func (l *WiredList) remove(v *dbBlock) {
	if !l.exists(v) {
		// Already removed
		return
	}
	v.prev.next = v.next
	v.next.prev = v.prev
	v.next = nil // avoid memory leaks
	v.prev = nil // avoid memory leaks
	l.len--
	return
}

func (l *WiredList) pushBack(v *dbBlock) {
	if l.exists(v) {
		l.moveToBack(v)
		return
	}
	l.insert(v, l.root.prev)
}

func (l *WiredList) moveToBack(v *dbBlock) {
	if !l.exists(v) || l.root.prev == v {
		return
	}
	l.remove(v)
	l.insert(v, l.root.prev)
}

func (l *WiredList) exists(v *dbBlock) bool {
	return v.next != nil || v.prev != nil
}
