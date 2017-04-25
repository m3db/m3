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

	maxWired  int
	root      dbBlock
	len       int
	updatesCh chan *dbBlock
	doneCh    chan struct{}

	metrics wiredListMetrics
}

type wiredListMetrics struct {
	wired   tally.Gauge
	max     tally.Gauge
	evicted tally.Counter
}

// NewWiredList returns a new database block wired list.
func NewWiredList(
	maxWiredBlocks int,
	iopts instrument.Options,
) *WiredList {
	scope := iopts.MetricsScope().
		SubScope("wired-list")
	l := &WiredList{
		maxWired: maxWiredBlocks,
		metrics: wiredListMetrics{
			wired:   scope.Gauge("wired"),
			max:     scope.Gauge("max"),
			evicted: scope.Counter("evicted"),
		},
	}
	l.root.next = &l.root
	l.root.prev = &l.root
	return l
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
				l.metrics.wired.Update(float64(l.len))
				l.metrics.max.Update(float64(l.maxWired))
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
}

func (l *WiredList) update(v *dbBlock) {
	// Trigger for processing
	l.updatesCh <- v
}

func (l *WiredList) processUpdateBlock(v *dbBlock) {
	entry := v.wiredListEntry()

	// If already in list
	if l.exists(v) {
		// Push to back if already wired
		if entry.wired {
			l.pushBack(v)
			return
		}

		// Otherwise remove from the list as not wired any longer
		l.remove(v)
		return
	}

	// Not in the list, if wired add it
	if entry.wired {
		l.pushBack(v)
	}
}

func (l *WiredList) insert(v, at *dbBlock) {
	n := at.next
	at.next = v
	v.prev = at
	v.next = n
	n.prev = v
	l.len++

	if l.maxWired <= 0 {
		// Not enforcing max wired blocks
		return
	}

	// Try to unwire all blocks possible
	for bl := l.root.next; l.len > l.maxWired && bl != &l.root; bl = bl.next {
		if bl.unwire() {
			// Successfully unwired the block
			l.remove(bl)
			l.metrics.evicted.Inc(1)
		}
		bl = bl.next
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
