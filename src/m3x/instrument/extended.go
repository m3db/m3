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
	"fmt"
	"os"
	"runtime"
	"strings"
	"sync/atomic"
	"time"

	"github.com/m3db/m3x/process"

	"github.com/uber-go/tally"
)

// ExtendedMetricsType is a type of extended metrics to report.
type ExtendedMetricsType int

const (
	// NoExtendedMetrics describes no extended metrics.
	NoExtendedMetrics ExtendedMetricsType = iota

	// SimpleExtendedMetrics describes just a simple level of extended metrics:
	// - number of active goroutines
	// - number of configured gomaxprocs
	SimpleExtendedMetrics

	// ModerateExtendedMetrics describes a moderately verbose level of extended metrics:
	// - number of active goroutines
	// - number of configured gomaxprocs
	// - number of file descriptors
	ModerateExtendedMetrics

	// DetailedExtendedMetrics describes a detailed level of extended metrics:
	// - number of active goroutines
	// - number of configured gomaxprocs
	// - number of file descriptors
	// - memory allocated running count
	// - memory used by heap
	// - memory used by heap that is idle
	// - memory used by heap that is in use
	// - memory used by stack
	// - number of garbage collections
	// - GC pause times
	DetailedExtendedMetrics

	// DefaultExtendedMetricsType is the default extended metrics level.
	DefaultExtendedMetricsType = SimpleExtendedMetrics
)

var (
	validExtendedMetricsTypes = []ExtendedMetricsType{
		NoExtendedMetrics,
		SimpleExtendedMetrics,
		ModerateExtendedMetrics,
		DetailedExtendedMetrics,
	}
)

func (t ExtendedMetricsType) String() string {
	switch t {
	case NoExtendedMetrics:
		return "none"
	case SimpleExtendedMetrics:
		return "simple"
	case ModerateExtendedMetrics:
		return "moderate"
	case DetailedExtendedMetrics:
		return "detailed"
	}
	return "unknown"
}

// UnmarshalYAML unmarshals an ExtendedMetricsType into a valid type from string.
func (t *ExtendedMetricsType) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}
	if str == "" {
		*t = DefaultExtendedMetricsType
		return nil
	}
	strs := make([]string, len(validExtendedMetricsTypes))
	for _, valid := range validExtendedMetricsTypes {
		if str == valid.String() {
			*t = valid
			return nil
		}
		strs = append(strs, "'"+valid.String()+"'")
	}
	return fmt.Errorf("invalid ExtendedMetricsType '%s' valid types are: %s",
		str, strings.Join(strs, ", "))
}

// StartReportingExtendedMetrics starts reporting extended metrics.
func StartReportingExtendedMetrics(
	scope tally.Scope,
	reportInterval time.Duration,
	metricsType ExtendedMetricsType,
) *ExtendedMetricsReporter {
	reporter := NewExtendedMetricsReporter(scope, reportInterval, metricsType)
	reporter.Start()
	return reporter
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
	NumFDs      tally.Gauge
	NumFDErrors tally.Counter
	pid         int
}

// ExtendedMetricsReporter reports an extended set of metrics.
// The reporter is not thread-safe.
type ExtendedMetricsReporter struct {
	reportInterval time.Duration
	metricsType    ExtendedMetricsType
	runtime        runtimeMetrics
	process        processMetrics
	started        bool
	quit           chan struct{}
}

// NewExtendedMetricsReporter creates a new extended metrics reporter.
func NewExtendedMetricsReporter(
	scope tally.Scope,
	reportInterval time.Duration,
	metricsType ExtendedMetricsType,
) *ExtendedMetricsReporter {
	r := &ExtendedMetricsReporter{
		reportInterval: reportInterval,
		metricsType:    metricsType,
		started:        false,
		quit:           make(chan struct{}),
	}
	if r.metricsType == NoExtendedMetrics {
		return r
	}

	runtimeScope := scope.SubScope("runtime")
	processScope := scope.SubScope("process")
	r.runtime.NumGoRoutines = runtimeScope.Gauge("num-goroutines")
	r.runtime.GoMaxProcs = runtimeScope.Gauge("gomaxprocs")
	r.process.NumFDs = processScope.Gauge("num-fds")
	r.process.NumFDErrors = processScope.Counter("num-fd-errors")
	r.process.pid = os.Getpid()
	if r.metricsType == SimpleExtendedMetrics {
		return r
	}

	var memstats runtime.MemStats
	runtime.ReadMemStats(&memstats)
	memoryScope := runtimeScope.SubScope("memory")
	r.runtime.MemoryAllocated = memoryScope.Gauge("allocated")
	r.runtime.MemoryHeap = memoryScope.Gauge("heap")
	r.runtime.MemoryHeapIdle = memoryScope.Gauge("heapidle")
	r.runtime.MemoryHeapInuse = memoryScope.Gauge("heapinuse")
	r.runtime.MemoryStack = memoryScope.Gauge("stack")
	r.runtime.NumGC = memoryScope.Counter("num-gc")
	r.runtime.GcPauseMs = memoryScope.Timer("gc-pause-ms")
	r.runtime.lastNumGC = memstats.NumGC
	return r
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
	if r.metricsType == NoExtendedMetrics {
		return
	}

	r.runtime.NumGoRoutines.Update(float64(runtime.NumGoroutine()))
	r.runtime.GoMaxProcs.Update(float64(runtime.GOMAXPROCS(0)))
	if r.metricsType < DetailedExtendedMetrics {
		return
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
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
	if r.metricsType < ModerateExtendedMetrics {
		return
	}
	numFDs, err := process.NumFDs(r.process.pid)
	if err == nil {
		r.process.NumFDs.Update(float64(numFDs))
	} else {
		r.process.NumFDErrors.Inc(1)
	}
}
