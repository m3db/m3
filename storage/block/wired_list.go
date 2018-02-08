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

// The wired list is the primary data structure that is used to support the LRU
// caching policy. It is a global (per-database) structure that is shared
// between all namespaces, shards, and series. It is responsible for determining
// which blocks should be kept "wired" (cached) in memory, and which should be
// closed and fetched again from disk if they need to be retrieved in the future.
//
// The WiredList is basically a specialized LRU, except that it doesn't store the
// data itself, it just keeps track of which data is currently in memory and makes
// decisions about which data to remove from memory. Updating the Wired List is
// asynchronous: callers put an operation to modify the list into a channel and
// a background goroutine pulls from that channels and performs updates to the
// list which may include removing items from memory ("unwiring" blocks).
//
// The WiredList itself does not allocate a per-entry datastructure to keep track
// of what is active and what is not. Instead, it creates a "virtual list" ontop
// of the existing blocks that are in memory by manipulating struct-level pointers
// on the DatabaseBlocks which are "owned" by the list. In other words, the
// DatabaseBlocks are scattered among numerous namespaces/shards/series, but they
// existed in virtual sorted order via the prev/next pointers they contain, but
// which are only manipulated by the WiredList.
//
// The WiredList fits into the lifecycles of M3DB in three different ways:
// 		1) It is started and stopped in the Database
// 		2) It is "updated" everytime a block is retrieved from disk and
// 		   everytime a block that is already in memory is read.
// 		3) It notifies other structures when it has evicted a block from
// 		   memory (so that they can update their corresponding data-structures).

package block

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/runtime"
	"github.com/m3db/m3x/instrument"

	"github.com/uber-go/tally"
)

const (
	wiredListEventsChannelLength = 65536
	wiredListSampleGaugesEvery   = 100
)

// WiredList is a database block wired list.
// TODO: Consider renaming to BlockLRU?
type WiredList struct {
	sync.Mutex

	nowFn clock.NowFn

	// Max wired blocks, must use atomic store and load to access.
	maxWired int64

	root      dbBlock
	length    int
	updatesCh chan DatabaseBlock
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
	copts clock.Options,
) *WiredList {
	scope := iopts.MetricsScope().
		SubScope("wired-list")
	l := &WiredList{
		nowFn: copts.NowFn(),
		metrics: wiredListMetrics{
			unwireable:           scope.Gauge("unwireable"),
			limit:                scope.Gauge("limit"),
			evicted:              scope.Counter("evicted"),
			evictedAfterDuration: scope.Timer("evicted-after-duration"),
		},
	}
	l.root.setNext(&l.root)
	l.root.setPrev(&l.root)
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

	l.updatesCh = make(chan DatabaseBlock, wiredListEventsChannelLength)
	l.doneCh = make(chan struct{}, 1)
	go func() {
		i := 0
		for v := range l.updatesCh {
			l.processUpdateBlock(v)
			if i%wiredListSampleGaugesEvery == 0 {
				l.metrics.unwireable.Update(float64(l.length))
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

// Update places the block into the channel of blocks which are waiting to notify the
// wired list that they were accessed. All updates must be processed through this channel
// to force synchronization.
//
// We use a channel and a background processing goroutine to reduce blocking / lock contention.
func (l *WiredList) Update(v DatabaseBlock) {
	l.updatesCh <- v
}

// processUpdateBlock inspects a block that has been modified or read recently
// and determines what outcome its state should have on the wired list. In the LRU
// caching policy, there are only two possible types of dbBlocks:
// 		1) Sealed blocks which have just been rotated out of the active write buffers,
// 		   but have not been flushed to disk yet.
// 		2) Sealed blocks which have been flushed to disk and are only in memory
// 		   because a read caused them to be temporarily cached.
//
// Blocks of type 1 must not be closed / "unwired" otherwise they will never get
// flushed to disk and we'll lose that data. Blocks of type 2 can be safely closed /
// "unwired" because they've already been flished.
//
// The processUpdateBlock function must distinguish between these two types of blocks.
// If the block is unwireable (I.E has been read from disk), then its safe to push it
// back because if it eventually gets removed from the wired list and closed, we can
// always retrieve it again later. If the block is not unwireable, it must not be
// pushed back (or really ever inserted into the Wired List) because it will be closed
// when its kicked out and we'll lose the unflushed data. This effectively prevents
// blocks of type 1 from ever making it into the WiredList in the first place because
// this method is the only way blocks are ever added into the WiredList.
func (l *WiredList) processUpdateBlock(v DatabaseBlock) {
	entry := v.wiredListEntry()
	// TODO: Simplify this, can probably just rely on WasRetrieved
	unwireable := !entry.Closed && ((entry.Retriever != nil && entry.RetrieveID != nil) || entry.WasRetrieved)

	// If a block is still unwireable then its worth keeping track of in the wired list
	// so we push it back.
	if unwireable {
		l.pushBack(v)
		return
	}

	// If a block is not unwireable, there is no point in keeping track of it in the wired list
	// so we remove it or just don't add it in the first place since the remove method is a noop
	// for blocks that aren't already in the WiredList and the pushBack method used above is the
	// only way for blocks to be added to the WiredList.
	l.remove(v)
}

func (l *WiredList) insertAfter(v, at DatabaseBlock) {
	now := l.nowFn()

	n := at.next()
	at.setNext(v)
	v.setPrev(at)
	v.setNext(n)
	v.setNextPrevUpdatedAtUnixNano(now.UnixNano())
	n.setPrev(v)
	l.length++

	maxWired := int(atomic.LoadInt64(&l.maxWired))
	if maxWired <= 0 {
		// Not enforcing max wired blocks
		return
	}

	// Try to unwire all blocks possible
	for bl := l.root.next(); l.length > maxWired && bl != &l.root; bl = bl.next() {
		// Evict the block before closing it so that callers of series.ReadEncoded()
		// don't get errors about trying to read from a closed block.
		// TODO: Does this eliminate the race entirely (since the WiredList only
		// performs updates one at a time and in order) or do we need to be defensive
		// about errors relating to reading from closed blocks in the ReadEncoded path?
		if owner := bl.Owner(); owner != nil {
			// Used wiredListEntry method instead of RetrieveID() and StartTime()
			// to guarantee consistent view (since blocks are pooled / can be closed
			// by other parts of the code.)
			wlEntry := bl.wiredListEntry()
			// Its possible that the block has already been closed / reset / put back into
			// the pool by the Series itself. In that case, its possible for the ID to be
			// nil. To prevent implementors from having to deal with that, we check here
			// before calling OnEvictedFromWiredList. Note its also possible that the block
			// has already been reused and the ID corresponds to a different block entirely
			// than what it was when it was placed in the WiredList. TODO: Dirty bit?
			if wlEntry.RetrieveID != nil {
				owner.OnEvictedFromWiredList(wlEntry.RetrieveID, wlEntry.StartTime)
			}
		}

		// TODO: Swap remove and Close() order so that we can distinguish between "clean"
		// closes and "dirty" closes.
		bl.Close()
		// Successfully unwired the block
		l.remove(bl)

		l.metrics.evicted.Inc(1)

		lastUpdatedAt := time.Unix(0, bl.nextPrevUpdatedAtUnixNano())
		l.metrics.evictedAfterDuration.Record(now.Sub(lastUpdatedAt))
	}
}

func (l *WiredList) remove(v DatabaseBlock) {
	if !l.exists(v) {
		// Already removed
		return
	}
	v.prev().setNext(v.next())
	v.next().setPrev(v.prev())
	v.setNext(nil) // avoid memory leaks
	v.setPrev(nil) // avoid memory leaks
	l.length--
}

func (l *WiredList) pushBack(v DatabaseBlock) {
	if l.exists(v) {
		l.moveToBack(v)
		return
	}
	l.insertAfter(v, l.root.prev())
}

func (l *WiredList) moveToBack(v DatabaseBlock) {
	if !l.exists(v) || l.root.prev() == v {
		return
	}
	l.remove(v)
	l.insertAfter(v, l.root.prev())
}

func (l *WiredList) exists(v DatabaseBlock) bool {
	return v.next() != nil || v.prev() != nil
}
