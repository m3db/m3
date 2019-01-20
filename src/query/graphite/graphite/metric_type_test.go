package graphite

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/query/graphite/context"
	xtest "github.com/m3db/m3/src/query/graphite/testing"
	"github.com/m3db/m3/src/query/graphite/ts"

	"github.com/stretchr/testify/assert"
)

func TestMetricTypeMappings(t *testing.T) {
	ctx := context.New()
	series := ts.NewSeries(ctx, "foo.bar.baz", time.Now(), ts.NewValues(ctx, 1000, 1000))

	// nil tags should default to counts
	series.Tags = nil
	m := MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Counts, m)
	s, err := MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, CountsStr, s)

	// missing should default to counts
	series.Tags = map[string]string{"random": "none"}
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Counts, m)
	ca := ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationSum, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, CountsStr, s)

	// gauge
	series.Tags[TypeStr] = GaugeStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Gauges, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationAvg, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, GaugesStr, s)

	// gauges
	series.Tags[TypeStr] = GaugesStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Gauges, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationAvg, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, GaugesStr, s)

	// timer
	series.Tags[TypeStr] = TimerStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Timers, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationAvg, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, TimersStr, s)

	// timers
	series.Tags[TypeStr] = TimersStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Timers, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationAvg, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, TimersStr, s)

	// counter
	series.Tags[TypeStr] = CounterStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Counts, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationSum, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, CountsStr, s)

	// counts
	series.Tags[TypeStr] = CountsStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Counts, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationSum, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Nil(t, err)
	assert.Equal(t, CountsStr, s)

	// ratios
	series.Tags[TypeStr] = RatiosStr
	m = MetricTypeFromTags(series.Tags)
	xtest.Equalish(t, Ratios, m)
	ca = ConsolidationFuncFromTags(series.Tags)
	assert.Equal(t, ts.ConsolidationAvg, ca)
	s, err = MetricTypeStrFromTags(series.Tags)
	assert.Empty(t, s)
	assert.Error(t, err, "unknown type in the tags")
}

func TestConsolidationFuncForMetricType(t *testing.T) {
	ca := ConsolidationFuncForMetricType(Counts)
	assert.Equal(t, ts.ConsolidationSum, ca)

	ca = ConsolidationFuncForMetricType(Timers)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(Gauges)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(Ratios)
	assert.Equal(t, ts.ConsolidationAvg, ca)

	ca = ConsolidationFuncForMetricType(MetricType(999))
	assert.Equal(t, ts.ConsolidationAvg, ca)
}

func TestMetricTypeFromID(t *testing.T) {
	assert.Equal(t, metric.UnknownType, MetricTypeFromID("servers.m3-kv05-dca1.disk.sdd.rkb"))
	assert.Equal(t, metric.CounterType, MetricTypeFromID("stats.sjc1.counts.m3+calls+dc=sjc1"))
	assert.Equal(t, metric.GaugeType, MetricTypeFromID("stats.sjc1.gauges.m3+task+dc=sjc1"))
	assert.Equal(t, metric.TimerType, MetricTypeFromID("stats.sjc1.timers.haproxy.beagle.response_time.p99"))
}
