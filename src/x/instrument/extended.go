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
	"runtime"
	"strings"
	"time"

	xerrors "github.com/m3db/m3/src/x/errors"

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
	strs := make([]string, 0, len(validExtendedMetricsTypes))
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

// StartReportingExtendedMetrics creates a extend metrics reporter and starts
// the reporter returning it so it may be stopped if successfully started.
func StartReportingExtendedMetrics(
	scope tally.Scope,
	reportInterval time.Duration,
	metricsType ExtendedMetricsType,
) (Reporter, error) {
	reporter := NewExtendedMetricsReporter(scope, reportInterval, metricsType)
	if err := reporter.Start(); err != nil {
		return nil, err
	}
	return reporter, nil
}

// cumulativeCounter emits a cumulative memory stat counter as a metric counter.
// the counter tracks the last value, so it can emit deltas to the underlying counter metric.
type cumulativeCounter struct {
	counter tally.Counter
	last    uint64
}

func newCumulativeCounter(scope tally.Scope, name string) cumulativeCounter {
	return cumulativeCounter{
		counter: scope.Counter(name),
	}
}

// update the cumulative counter and returns the previous last value.
func (c *cumulativeCounter) update(new uint64) uint64 {
	prevLast := c.last
	c.counter.Inc(int64(new - c.last))
	c.last = new
	return prevLast
}

type runtimeMetrics struct {
	NumGoRoutines       tally.Gauge
	GoMaxProcs          tally.Gauge
	MemoryAllocated     tally.Gauge // Note: this is the same as MemoryHeapAlloc
	MemoryTotalAlloc    cumulativeCounter
	MemorySys           tally.Gauge
	MemoryLookups       cumulativeCounter
	MemoryMallocs       cumulativeCounter
	MemoryFrees         cumulativeCounter
	MemoryHeapAlloc     tally.Gauge
	MemoryHeapSys       tally.Gauge
	MemoryHeapIdle      tally.Gauge
	MemoryHeapInuse     tally.Gauge
	MemoryHeapReleased  tally.Gauge
	MemoryStackInuse    tally.Gauge
	MemoryStackSys      tally.Gauge
	MemoryMSpanInuse    tally.Gauge
	MemoryMSpanSys      tally.Gauge
	MemoryMCacheInuse   tally.Gauge
	MemoryMCacheSys     tally.Gauge
	MemoryBucketHashSys tally.Gauge
	MemoryGCSys         tally.Gauge
	MemoryOtherSys      tally.Gauge
	NextGC              tally.Gauge
	LastGC              tally.Gauge
	PauseTotalNs        cumulativeCounter
	NumGC               cumulativeCounter
	NumForcedGC         cumulativeCounter
	GCCPUFraction       tally.Gauge
	GcPauseMs           tally.Timer
}

func newRuntimeMetrics(metricsType ExtendedMetricsType, scope tally.Scope) runtimeMetrics {
	m := runtimeMetrics{}
	if metricsType == NoExtendedMetrics {
		return m
	}

	runtimeScope := scope.SubScope("runtime")
	m.NumGoRoutines = runtimeScope.Gauge("num-goroutines")
	m.GoMaxProcs = runtimeScope.Gauge("gomaxprocs")
	if metricsType < DetailedExtendedMetrics {
		return m
	}

	memoryScope := runtimeScope.SubScope("memory")
	m.MemoryAllocated = memoryScope.Gauge("allocated")
	m.MemoryTotalAlloc = newCumulativeCounter(memoryScope, "total-allocated")
	m.MemorySys = memoryScope.Gauge("sys")
	m.MemoryLookups = newCumulativeCounter(memoryScope, "lookups")
	m.MemoryMallocs = newCumulativeCounter(memoryScope, "mallocs")
	m.MemoryFrees = newCumulativeCounter(memoryScope, "frees")
	m.MemoryHeapAlloc = memoryScope.Gauge("heap")
	m.MemoryHeapSys = memoryScope.Gauge("heap-sys")
	m.MemoryHeapIdle = memoryScope.Gauge("heapidle")
	m.MemoryHeapInuse = memoryScope.Gauge("heapinuse")
	m.MemoryHeapReleased = memoryScope.Gauge("heap-released")
	m.MemoryStackInuse = memoryScope.Gauge("stack")
	m.MemoryStackSys = memoryScope.Gauge("stack-sys")
	m.MemoryMSpanInuse = memoryScope.Gauge("mspan-inuse")
	m.MemoryMSpanSys = memoryScope.Gauge("mspan-sys")
	m.MemoryMCacheInuse = memoryScope.Gauge("mcache-inuse")
	m.MemoryMCacheSys = memoryScope.Gauge("mcache-sys")
	m.MemoryBucketHashSys = memoryScope.Gauge("bucket-hash-sys")
	m.MemoryGCSys = memoryScope.Gauge("gc-sys")
	m.MemoryOtherSys = memoryScope.Gauge("other-sys")
	m.NextGC = memoryScope.Gauge("next-gc")
	m.LastGC = memoryScope.Gauge("last-gc")
	m.PauseTotalNs = newCumulativeCounter(memoryScope, "pause-total")
	m.NumGC = newCumulativeCounter(memoryScope, "num-gc")
	m.NumForcedGC = newCumulativeCounter(memoryScope, "num-forced-gc")
	m.GCCPUFraction = memoryScope.Gauge("gc-cpu-fraction")
	m.GcPauseMs = memoryScope.Timer("gc-pause-ms")

	return m
}

func (r *runtimeMetrics) report(metricsType ExtendedMetricsType) {
	if metricsType == NoExtendedMetrics {
		return
	}

	r.NumGoRoutines.Update(float64(runtime.NumGoroutine()))
	r.GoMaxProcs.Update(float64(runtime.GOMAXPROCS(0)))
	if metricsType < DetailedExtendedMetrics {
		return
	}

	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	r.MemoryAllocated.Update(float64(memStats.Alloc))
	r.MemoryTotalAlloc.update(memStats.TotalAlloc)
	r.MemorySys.Update(float64(memStats.Sys))
	r.MemoryLookups.update(memStats.Lookups)
	r.MemoryMallocs.update(memStats.Mallocs)
	r.MemoryFrees.update(memStats.Frees)
	r.MemoryHeapAlloc.Update(float64(memStats.HeapAlloc))
	r.MemoryHeapSys.Update(float64(memStats.HeapSys))
	r.MemoryHeapIdle.Update(float64(memStats.HeapIdle))
	r.MemoryHeapInuse.Update(float64(memStats.HeapInuse))
	r.MemoryHeapReleased.Update(float64(memStats.HeapReleased))
	r.MemoryStackInuse.Update(float64(memStats.StackInuse))
	r.MemoryStackSys.Update(float64(memStats.StackSys))
	r.MemoryMSpanInuse.Update(float64(memStats.MSpanInuse))
	r.MemoryMSpanSys.Update(float64(memStats.MSpanSys))
	r.MemoryMCacheInuse.Update(float64(memStats.MCacheInuse))
	r.MemoryMCacheSys.Update(float64(memStats.MCacheSys))
	r.MemoryBucketHashSys.Update(float64(memStats.BuckHashSys))
	r.MemoryGCSys.Update(float64(memStats.GCSys))
	r.MemoryOtherSys.Update(float64(memStats.OtherSys))
	r.NextGC.Update(float64(memStats.NextGC))
	r.LastGC.Update(float64(memStats.LastGC))
	r.PauseTotalNs.update(memStats.PauseTotalNs)
	lastNumGC := r.NumGC.update(uint64(memStats.NumGC))
	r.NumForcedGC.update(uint64(memStats.NumForcedGC))
	r.GCCPUFraction.Update(memStats.GCCPUFraction)

	num := memStats.NumGC
	lastNum := uint32(lastNumGC)
	if delta := num - lastNum; delta > 0 {
		if delta > 255 {
			// too many GCs happened, the timestamps buffer got wrapped around. Report only the last 256.
			lastNum = num - 256
		}
		for i := lastNum; i != num; i++ {
			pause := memStats.PauseNs[i%256]
			r.GcPauseMs.Record(time.Duration(pause))
		}
	}
}

type extendedMetricsReporter struct {
	baseReporter
	processReporter Reporter

	metricsType ExtendedMetricsType
	runtime     runtimeMetrics
}

// NewExtendedMetricsReporter creates a new extended metrics reporter
// that reports runtime and process metrics.
func NewExtendedMetricsReporter(
	scope tally.Scope,
	reportInterval time.Duration,
	metricsType ExtendedMetricsType,
) Reporter {
	r := new(extendedMetricsReporter)
	r.metricsType = metricsType
	r.init(reportInterval, func() {
		r.runtime.report(r.metricsType)
	})

	if r.metricsType >= ModerateExtendedMetrics {
		// ProcessReporter can be quite slow in some situations (specifically
		// counting FDs for processes that have many of them) so it runs on
		// its own report loop.
		r.processReporter = NewProcessReporter(scope, reportInterval)
	}

	r.runtime = newRuntimeMetrics(metricsType, scope)
	return r
}

func (e *extendedMetricsReporter) Start() error {
	if err := e.baseReporter.Start(); err != nil {
		return err
	}

	if e.processReporter != nil {
		if err := e.processReporter.Start(); err != nil {
			return err
		}
	}

	return nil
}

func (e *extendedMetricsReporter) Stop() error {
	multiErr := xerrors.NewMultiError()

	if err := e.baseReporter.Stop(); err != nil {
		multiErr = multiErr.Add(err)
	}

	if e.processReporter != nil {
		if err := e.processReporter.Stop(); err != nil {
			multiErr = multiErr.Add(err)
		}
	}

	return multiErr.FinalError()
}
