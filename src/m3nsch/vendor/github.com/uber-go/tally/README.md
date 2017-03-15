# :heavy_check_mark: tally [![GoDoc][doc-img]][doc] [![Build Status][ci-img]][ci] [![Coverage Status][cov-img]][cov]

Fast, buffered, hierarchical stats collection in Go.

## Installation
`go get -u github.com/uber-go/tally`

## Abstract

Tally provides a common interface for emitting metrics, while letting you not worry about the velocity of metrics emission.  

By default it buffers counters and gauges at a specified interval but does not buffer timer values.  This is primarily so timer values can have all their values sampled if desired and if not they can be sampled as histograms.

## Structure

- Scope: Keeps track of metrics, and their common metadata.
- Metrics: Counters, Gauges, Timers.
- Reporter: Implemented by you. Accepts aggregated values from the scope. Forwards the aggregated values to your metrics ingestion pipeline.

### Acquire a Scope ###
```go
reporter = NewMyStatsReporter()  // Implement as you will
tags := map[string]string{
	"dc": "east-1",
	"type": "master",
}
reportEvery := 1 * time.Second
scope := tally.NewRootScope("some_prefix", tags, reporter, reportEvery, tally.DefaultSeparator)
```

### Get/Create a metric, use it ###
```go
// Get a counter, increment a counter
reqCounter := scope.Counter("requests")  // cache me
reqCounter.Inc(1)

queueGauge := scope.Gauge("queue_length")  // cache me
queueGauge.Update(42)
```

### Report your metrics ###
Use the inbuilt statsd reporter:

```go
import (
	"github.com/cactus/go-statsd-client/statsd"
	"github.com/uber-go/tally"
	tallystatsd "github.com/uber-go/tally/statsd"
	// ...
)

client, err := statsd.NewClient("statsd.aggregator.local:1234", "")
// ...

opts := tallystatsd.NewOptions().SetSampleRate(1.0)
reporter = tallystatsd.NewStatsdReporter(client, opts)
tags := map[string]string{
	"dc": "east-1",
	"type": "master",
}
reportEvery := 1 * time.Second
scope := tally.NewRootScope("some_prefix", tags, reporter, reportEvery, tally.DefaultSeparator)
```

Implement your own reporter using the `StatsReporter` interface:

```go
type StatsReporter interface {
	// ReportCounter reports a counter value
	ReportCounter(name string, tags map[string]string, value int64)

	// ReportGauge reports a gauge value
	ReportGauge(name string, tags map[string]string, value float64)

	// ReportTimer reports a timer value
	ReportTimer(name string, tags map[string]string, interval time.Duration)

	// Capabilities returns a description of metrics reporting capabilities
	Capabilities() Capabilities

	// Flush is expected to be called by a Scope when it completes a round or reporting
	Flush()
}
```

Or implement your own metrics implementation that matches the tally `Scope` interface to use different buffering semantics:

```go
type Scope interface {
	// Counter returns the Counter object corresponding to the name
	Counter(name string) Counter

	// Gauge returns the Gauge object corresponding to the name
	Gauge(name string) Gauge

	// Timer returns the Timer object corresponding to the name
	Timer(name string) Timer

	// Tagged returns a new child scope with the given tags and current tags
	Tagged(tags map[string]string) Scope

	// SubScope returns a new child scope appending a further name prefix
	SubScope(name string) Scope

	// Capabilities returns a description of metrics reporting capabilities
	Capabilities() Capabilities
}
```

## Performance

This stuff needs to be fast. With that in mind, we avoid locks and unnecessary memory allocations.

```
BenchmarkCounterInc-8               	200000000	         7.68 ns/op
BenchmarkReportCounterNoData-8      	300000000	         4.88 ns/op
BenchmarkReportCounterWithData-8    	100000000	        21.6 ns/op
BenchmarkGaugeSet-8                 	100000000	        16.0 ns/op
BenchmarkReportGaugeNoData-8        	100000000	        10.4 ns/op
BenchmarkReportGaugeWithData-8      	50000000	        27.6 ns/op
BenchmarkTimerInterval-8            	50000000	        37.7 ns/op
BenchmarkTimerReport-8              	300000000	         5.69 ns/op
```

<hr>
Released under the [MIT License](LICENSE).

[doc-img]: https://godoc.org/github.com/uber-go/tally?status.svg
[doc]: https://godoc.org/github.com/uber-go/tally
[ci-img]: https://travis-ci.org/uber-go/tally.svg?branch=master
[ci]: https://travis-ci.org/uber-go/tally
[cov-img]: https://coveralls.io/repos/github/uber-go/tally/badge.svg?branch=master
[cov]: https://coveralls.io/github/uber-go/tally?branch=master
[glide.lock]: https://github.com/uber-go/tally/blob/master/glide.lock
[v1]: https://github.com/uber-go/tally/milestones
