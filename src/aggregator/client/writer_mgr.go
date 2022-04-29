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

package client

import (
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3/src/cluster/placement"
	xerrors "github.com/m3db/m3/src/x/errors"
	xsync "github.com/m3db/m3/src/x/sync"

	"github.com/uber-go/tally"
	"golang.org/x/sys/cpu"
)

var (
	errInstanceWriterManagerClosed = errors.New("instance writer manager closed")
)

const (
	_queueMetricReportInterval = 10 * time.Second
	_queueMetricBuckets        = 8
	_queueMetricBucketStart    = 64
)

// instanceWriterManager manages instance writers.
type instanceWriterManager interface {
	// AddInstances adds instances.
	AddInstances(instances []placement.Instance) error

	// RemoveInstances removes instancess.
	RemoveInstances(instances []placement.Instance) error

	// Write writes a metric payload.
	Write(
		instance placement.Instance,
		shardID uint32,
		payload payloadUnion,
	) error

	// Flush flushes buffered metrics.
	Flush() error

	// Close closes the writer manager.
	Close() error
}

type writerManagerMetrics struct {
	instancesAdded      tally.Counter
	instancesRemoved    tally.Counter
	queueLen            tally.Histogram
	dirtyWritersPercent tally.Histogram
}

func newWriterManagerMetrics(scope tally.Scope) writerManagerMetrics {
	buckets := append(
		tally.ValueBuckets{0},
		tally.MustMakeExponentialValueBuckets(_queueMetricBucketStart, 2, _queueMetricBuckets)...,
	)

	percentBuckets := append(
		tally.ValueBuckets{0},
		tally.MustMakeLinearValueBuckets(5, 5, 20)...,
	)

	return writerManagerMetrics{
		instancesAdded: scope.Tagged(map[string]string{
			"action": "add",
		}).Counter("instances"),
		instancesRemoved: scope.Tagged(map[string]string{
			"action": "remove",
		}).Counter("instances"),
		queueLen:            scope.Histogram("queue-length", buckets),
		dirtyWritersPercent: scope.Histogram("dirty-writers-percent", percentBuckets),
	}
}

type writerManager struct {
	sync.RWMutex
	wg      sync.WaitGroup
	doneCh  chan struct{}
	opts    Options
	writers map[string]*refCountedWriter
	closed  bool
	metrics writerManagerMetrics
	_       cpu.CacheLinePad
	pool    xsync.PooledWorkerPool
}

func newInstanceWriterManager(opts Options) (instanceWriterManager, error) {
	wm := &writerManager{
		opts:    opts,
		writers: make(map[string]*refCountedWriter),
		metrics: newWriterManagerMetrics(opts.InstrumentOptions().MetricsScope()),
		doneCh:  make(chan struct{}),
	}

	pool, err := xsync.NewPooledWorkerPool(
		opts.FlushWorkerCount(),
		xsync.NewPooledWorkerPoolOptions().SetKillWorkerProbability(0.05),
	)
	if err != nil {
		return nil, err
	}

	wm.pool = pool
	wm.pool.Init()

	wm.wg.Add(1)
	go wm.reportMetricsLoop()

	if opts.ForceFlushEvery() > 0 {
		wm.wg.Add(1)
		go func() {
			wm.flushLoop(opts.ForceFlushEvery())
		}()
	}

	return wm, nil
}

func (mgr *writerManager) AddInstances(instances []placement.Instance) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}
	for _, instance := range instances {
		id := instance.ID()
		writer, exists := mgr.writers[id]
		if !exists {
			instrumentOpts := mgr.opts.InstrumentOptions()
			scope := instrumentOpts.MetricsScope()
			opts := mgr.opts.SetInstrumentOptions(instrumentOpts.SetMetricsScope(scope.SubScope("writer")))
			writer = newRefCountedWriter(instance, opts)
			mgr.writers[id] = writer
			mgr.metrics.instancesAdded.Inc(1)
		}
		writer.IncRef()
	}
	return nil
}

func (mgr *writerManager) RemoveInstances(instances []placement.Instance) error {
	mgr.Lock()
	defer mgr.Unlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}
	for _, instance := range instances {
		id := instance.ID()
		writer, exists := mgr.writers[id]
		if !exists {
			continue
		}
		if writer.DecRef() == 0 {
			delete(mgr.writers, id)
			mgr.metrics.instancesRemoved.Inc(1)
		}
	}
	return nil
}

func (mgr *writerManager) Write(
	instance placement.Instance,
	shardID uint32,
	payload payloadUnion,
) error {
	mgr.RLock()
	if mgr.closed {
		mgr.RUnlock()
		return errInstanceWriterManagerClosed
	}
	id := instance.ID()
	writer, exists := mgr.writers[id]
	if !exists {
		mgr.RUnlock()
		return fmt.Errorf("writer for instance %s is not found", id)
	}
	writer.dirty.Store(true)
	err := writer.Write(shardID, payload)
	mgr.RUnlock()

	return err
}

func (mgr *writerManager) Flush() error {
	mgr.RLock()
	defer mgr.RUnlock()

	if mgr.closed {
		return errInstanceWriterManagerClosed
	}

	var (
		errCh  = make(chan error, 1)
		mErrCh = make(chan xerrors.MultiError, 1)
		wg     sync.WaitGroup
	)

	numDirty := 0
	for _, w := range mgr.writers {
		if !w.dirty.Load() {
			continue
		}
		numDirty++
		w := w
		wg.Add(1)
		mgr.pool.Go(func() {
			defer wg.Done()

			w.dirty.CAS(true, false)
			if err := w.Flush(); err != nil {
				errCh <- err
			}
		})
	}

	percentInUse := 0.0
	if numDirty > 0 && len(mgr.writers) > 0 {
		percentInUse = 100.0 * (float64(numDirty) / float64(len(mgr.writers)))
	}
	mgr.metrics.dirtyWritersPercent.RecordValue(percentInUse)

	go func() {
		multiErr := xerrors.NewMultiError()
		for err := range errCh {
			multiErr = multiErr.Add(err)
		}
		mErrCh <- multiErr
	}()
	wg.Wait()
	close(errCh)

	multiErr := <-mErrCh
	return multiErr.FinalError()
}

func (mgr *writerManager) Close() error {
	mgr.Lock()

	if mgr.closed {
		mgr.Unlock()
		return errInstanceWriterManagerClosed
	}

	mgr.closed = true
	for _, writer := range mgr.writers {
		writer.Close()
	}

	close(mgr.doneCh)
	mgr.Unlock()
	mgr.wg.Wait()

	return nil
}

func (mgr *writerManager) reportMetricsLoop() {
	defer mgr.wg.Done()

	ticker := time.NewTicker(_queueMetricReportInterval)
	defer ticker.Stop()

	for {
		select {
		case <-mgr.doneCh:
			return
		case <-ticker.C:
			mgr.reportMetrics()
		}
	}
}

func (mgr *writerManager) reportMetrics() {
	mgr.RLock()
	defer mgr.RUnlock()

	for _, writer := range mgr.writers {
		mgr.metrics.queueLen.RecordValue(float64(writer.QueueSize()))
	}
}

func (mgr *writerManager) flushLoop(d time.Duration) {
	defer mgr.wg.Done()

	ticker := time.NewTicker(d)
	defer ticker.Stop()

	for {
		select {
		case <-mgr.doneCh:
			return
		case <-ticker.C:
			mgr.Flush() //nolint:errcheck
		}
	}
}
