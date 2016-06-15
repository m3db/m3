package metrics

import (
	"time"

	"github.com/cactus/go-statsd-client/statsd"
)

// StatsReporter is the the interface used to report stats
type StatsReporter interface {
	IncCounter(name string, tags map[string]string, value int64)
	UpdateGauge(name string, tags map[string]string, value int64)
	RecordTimer(name string, tags map[string]string, d time.Duration)
}

// NullStatsReporter is a stats reporter that discards the statistics
var NullStatsReporter StatsReporter = nullStatsReporter{}

type nullStatsReporter struct{}

func (nullStatsReporter) IncCounter(name string, tags map[string]string, value int64)      {}
func (nullStatsReporter) UpdateGauge(name string, tags map[string]string, value int64)     {}
func (nullStatsReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {}

type cactusStatsReporter struct {
	statter statsd.Statter
}

// NewCactusStatsReporter creates a new cactus statsd reporter
func NewCactusStatsReporter(statter statsd.Statter) StatsReporter {
	return &cactusStatsReporter{statter: statter}
}

func (s *cactusStatsReporter) IncCounter(name string, tags map[string]string, value int64) {
	s.statter.Inc(name, value, 1.0)
}

func (s *cactusStatsReporter) UpdateGauge(name string, tags map[string]string, value int64) {
	s.statter.Gauge(name, value, 1.0)
}

func (s *cactusStatsReporter) RecordTimer(name string, tags map[string]string, d time.Duration) {
	s.statter.TimingDuration(name, d, 1.0)
}
