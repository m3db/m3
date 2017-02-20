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

package fs

import (
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ratelimit"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"

	"github.com/uber-go/tally"
)

const (
	bytesPerMegabit  = 1024 * 1024 / 8
	nsPerMillisecond = int64(time.Millisecond / time.Nanosecond)
)

type sleepFn func(time.Duration)

type shardMetrics struct {
	worked     tally.Gauge
	slept      tally.Gauge
	workedInNs int64
	sleptInNs  int64
}

func newShardMetrics(scope tally.Scope, shard uint32) *shardMetrics {
	s := scope.Tagged(
		map[string]string{
			"shard": strconv.Itoa(int(shard)),
		},
	)
	return &shardMetrics{
		worked: s.Gauge("worked"),
		slept:  s.Gauge("slept"),
	}
}

func (m *shardMetrics) reset() {
	atomic.StoreInt64(&m.workedInNs, 0)
	atomic.StoreInt64(&m.sleptInNs, 0)
}

func (m *shardMetrics) recordWorked(d time.Duration) {
	atomic.AddInt64(&m.workedInNs, int64(d))
}

func (m *shardMetrics) recordSlept(d time.Duration) {
	atomic.AddInt64(&m.sleptInNs, int64(d))
}

func (m *shardMetrics) report() {
	m.worked.Update(float64(atomic.LoadInt64(&m.workedInNs) / nsPerMillisecond))
	m.slept.Update(float64(atomic.LoadInt64(&m.sleptInNs) / nsPerMillisecond))
}

// persistManager is responsible for persisting series segments onto local filesystem.
// It is not thread-safe.
type persistManager struct {
	sync.RWMutex

	opts           Options
	scope          tally.Scope
	filePathPrefix string
	rateLimitOpts  ratelimit.Options
	nowFn          clock.NowFn
	sleepFn        sleepFn
	writer         FileSetWriter
	start          time.Time
	count          int
	bytesWritten   int64
	metrics        map[uint32]*shardMetrics
	currMetrics    *shardMetrics

	// segmentHolder is a two-item slice that's reused to hold pointers to the
	// head and the tail of each segment so we don't need to allocate memory
	// and gc it shortly after.
	segmentHolder []checked.Bytes
}

// NewPersistManager creates a new filesystem persist manager
func NewPersistManager(opts Options) persist.Manager {
	scope := opts.InstrumentOptions().MetricsScope().SubScope("persist")
	filePathPrefix := opts.FilePathPrefix()
	writerBufferSize := opts.WriterBufferSize()
	blockSize := opts.RetentionOptions().BlockSize()
	newFileMode := opts.NewFileMode()
	newDirectoryMode := opts.NewDirectoryMode()
	writer := NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	pm := &persistManager{
		opts:           opts,
		scope:          scope,
		filePathPrefix: filePathPrefix,
		rateLimitOpts:  opts.RateLimitOptions(),
		nowFn:          opts.ClockOptions().NowFn(),
		sleepFn:        time.Sleep,
		writer:         writer,
		metrics:        make(map[uint32]*shardMetrics),
		segmentHolder:  make([]checked.Bytes, 2),
	}

	go pm.reportLoop()
	return pm
}

func (pm *persistManager) persist(
	id ts.ID,
	segment ts.Segment,
	checksum uint32,
) error {
	pm.RLock()
	currMetrics := pm.currMetrics
	pm.RUnlock()

	var (
		start = pm.nowFn()
		slept time.Duration
	)
	rateLimitMbps := pm.rateLimitOpts.LimitMbps()
	if pm.rateLimitOpts.LimitEnabled() && rateLimitMbps > 0.0 {
		if pm.start.IsZero() {
			pm.start = start
		} else if pm.count >= pm.rateLimitOpts.LimitCheckEvery() {
			target := time.Duration(float64(time.Second) * float64(pm.bytesWritten) / float64(rateLimitMbps*bytesPerMegabit))
			if elapsed := start.Sub(pm.start); elapsed < target {
				slept = target - elapsed
				pm.sleepFn(slept)
			}
			pm.count = 0
		}
	}

	pm.segmentHolder[0] = segment.Head
	pm.segmentHolder[1] = segment.Tail
	err := pm.writer.WriteAll(id, pm.segmentHolder, checksum)
	pm.count++
	pm.bytesWritten += int64(segment.Len())

	end := pm.nowFn()
	worked := end.Sub(start) - slept
	if currMetrics != nil {
		currMetrics.recordWorked(worked)
		currMetrics.recordSlept(slept)
	}

	return err
}

func (pm *persistManager) close() error {
	err := pm.writer.Close()
	pm.reset()
	return err
}

func (pm *persistManager) reset() {
	pm.start = timeZero
	pm.count = 0
	pm.bytesWritten = 0
}

func (pm *persistManager) Prepare(namespace ts.ID, shard uint32, blockStart time.Time) (persist.PreparedPersist, error) {
	pm.Lock()
	currMetrics, exists := pm.metrics[shard]
	if !exists {
		currMetrics = newShardMetrics(pm.scope, shard)
		pm.metrics[shard] = currMetrics
	}
	currMetrics.reset()
	pm.currMetrics = currMetrics
	pm.Unlock()

	// NB(xichen): explicitly not using a defer to avoid creating lambdas on
	// the heap and causing additional GC
	start := pm.nowFn()
	var prepared persist.PreparedPersist

	// NB(xichen): if the checkpoint file for blockStart already exists, bail.
	// This allows us to retry failed flushing attempts because they wouldn't
	// have created the checkpoint file.
	if FilesetExistsAt(pm.filePathPrefix, namespace, shard, blockStart) {
		end := pm.nowFn()
		currMetrics.recordWorked(end.Sub(start))
		return prepared, nil
	}
	if err := pm.writer.Open(namespace, shard, blockStart); err != nil {
		end := pm.nowFn()
		currMetrics.recordWorked(end.Sub(start))
		return prepared, err
	}

	prepared.Persist = pm.persist
	prepared.Close = pm.close

	end := pm.nowFn()
	currMetrics.recordWorked(end.Sub(start))
	return prepared, nil
}

func (pm *persistManager) SetRateLimitOptions(value ratelimit.Options) {
	pm.rateLimitOpts = value
}

func (pm *persistManager) RateLimitOptions() ratelimit.Options {
	return pm.rateLimitOpts
}

func (pm *persistManager) reportLoop() {
	interval := pm.opts.InstrumentOptions().ReportInterval()
	t := time.NewTimer(interval)
	for range t.C {
		pm.RLock()
		currMetrics := pm.currMetrics
		pm.RUnlock()
		if currMetrics != nil {
			currMetrics.report()
		}
	}
}
