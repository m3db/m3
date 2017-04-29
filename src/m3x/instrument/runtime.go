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

package instrument

import (
	"os"
	"runtime"
	"sync/atomic"
	"time"

	"github.com/shirou/gopsutil/process"
	"github.com/uber-go/tally"
)

// StartReportingExtendedMetrics starts reporting extended metrics.
func StartReportingExtendedMetrics(
	scope tally.Scope,
	reportInterval time.Duration,
) (*ExtendedMetricsReporter, error) {
	reporter, err := NewExtendedMetricsReporter(scope, reportInterval)
	if err != nil {
		return nil, err
	}
	reporter.Start()
	return reporter, nil
}

type runtimeMetrics struct {
	NumGoRoutines   tally.Gauge
	GoMaxProcs      tally.Gauge
	MemoryAllocated tally.Gauge
	MemoryHeap      tally.Gauge
	MemoryHeapIdle  tally.Gauge
	MemoryHeapInuse tally.Gauge
	MemoryStack     tally.Gauge
	NumGC           tally.Counter
	GcPauseMs       tally.Timer
	lastNumGC       uint32
}

type processMetrics struct {
	NumFds      tally.Gauge
	NumFdErrors tally.Counter
	process     *process.Process
}

// ExtendedMetricsReporter reports an extended set of metrics.
// The reporter is not thread-safe.
type ExtendedMetricsReporter struct {
	reportInterval time.Duration
	runtime        runtimeMetrics
	process        processMetrics
	started        bool
	quit           chan struct{}
}

// NewExtendedMetricsReporter creates a new extended metrics reporter.
func NewExtendedMetricsReporter(
	scope tally.Scope,
	reportInterval time.Duration,
) (*ExtendedMetricsReporter, error) {
	pid := os.Getpid()
	process, err := process.NewProcess(int32(pid))
	if err != nil {
		return nil, err
	}

	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)

	runtimeScope := scope.SubScope("runtime")
	memoryScope := runtimeScope.SubScope("memory")
	processScope := scope.SubScope("process")

	return &ExtendedMetricsReporter{
		reportInterval: reportInterval,
		runtime: runtimeMetrics{
			NumGoRoutines:   runtimeScope.Gauge("num-goroutines"),
			GoMaxProcs:      runtimeScope.Gauge("gomaxprocs"),
			MemoryAllocated: memoryScope.Gauge("allocated"),
			MemoryHeap:      memoryScope.Gauge("heap"),
			MemoryHeapIdle:  memoryScope.Gauge("heapidle"),
			MemoryHeapInuse: memoryScope.Gauge("heapinuse"),
			MemoryStack:     memoryScope.Gauge("stack"),
			NumGC:           memoryScope.Counter("num-gc"),
			GcPauseMs:       memoryScope.Timer("gc-pause-ms"),
			lastNumGC:       memstats.NumGC,
		},
		process: processMetrics{
			NumFds:      processScope.Gauge("num-fds"),
			NumFdErrors: processScope.Counter("num-fd-errors"),
			process:     process,
		},
		started: false,
		quit:    make(chan struct{}),
	}, nil
}

// Start starts the reporter thread that periodically emits metrics.
func (r *ExtendedMetricsReporter) Start() {
	if r.started {
		return
	}
	go func() {
		ticker := time.NewTicker(r.reportInterval)
		for {
			select {
			case <-ticker.C:
				r.report()
			case <-r.quit:
				ticker.Stop()
				return
			}
		}
	}()
	r.started = true
}

// Stop stops reporting of runtime metrics.
// The reporter cannot be started again after it's been stopped.
func (r *ExtendedMetricsReporter) Stop() {
	close(r.quit)
}

func (r *ExtendedMetricsReporter) report() {
	r.reportRuntimeMetrics()
	r.reportProcessMetrics()
}

func (r *ExtendedMetricsReporter) reportRuntimeMetrics() {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)

	r.runtime.NumGoRoutines.Update(float64(runtime.NumGoroutine()))
	r.runtime.GoMaxProcs.Update(float64(runtime.GOMAXPROCS(0)))
	r.runtime.MemoryAllocated.Update(float64(memStats.Alloc))
	r.runtime.MemoryHeap.Update(float64(memStats.HeapAlloc))
	r.runtime.MemoryHeapIdle.Update(float64(memStats.HeapIdle))
	r.runtime.MemoryHeapInuse.Update(float64(memStats.HeapInuse))
	r.runtime.MemoryStack.Update(float64(memStats.StackInuse))

	// memStats.NumGC is a perpetually incrementing counter (unless it wraps at 2^32).
	num := memStats.NumGC
	lastNum := atomic.SwapUint32(&r.runtime.lastNumGC, num)
	if delta := num - lastNum; delta > 0 {
		r.runtime.NumGC.Inc(int64(delta))
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256.
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.runtime.GcPauseMs.Record(time.Duration(pause))
		}
	}
}

func (r *ExtendedMetricsReporter) reportProcessMetrics() {
	numFds, err := r.process.process.NumFDs()
	if err != nil {
		r.process.NumFdErrors.Inc(1)
		return
	}
	r.process.NumFds.Update(float64(numFds))
}
