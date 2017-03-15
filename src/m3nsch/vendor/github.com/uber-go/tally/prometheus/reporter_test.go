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

package prometheus

import (
	"fmt"
	"testing"
	"time"

	prom "github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCounter(t *testing.T) {
	registry := prom.NewRegistry()
	r := NewReporter(Options{Registerer: registry})
	name := "test_counter"
	tags := map[string]string{
		"foo":  "bar",
		"test": "everything",
	}
	tags2 := map[string]string{
		"foo":  "baz",
		"test": "something",
	}

	count := r.AllocateCounter(name, tags)
	count.ReportCount(1)
	count.ReportCount(2)

	count = r.AllocateCounter(name, tags2)
	count.ReportCount(2)

	assertMetric(t, gather(t, registry), metric{
		name:  name,
		mtype: dto.MetricType_COUNTER,
		instances: []instance{
			{
				labels:  tags,
				counter: counterValue(3),
			},
			{
				labels:  tags2,
				counter: counterValue(2),
			},
		},
	})
}

func TestGauge(t *testing.T) {
	registry := prom.NewRegistry()
	r := NewReporter(Options{Registerer: registry})
	name := "test_gauge"
	tags := map[string]string{
		"foo":  "bar",
		"test": "everything",
	}

	gauge := r.AllocateGauge(name, tags)
	gauge.ReportGauge(15)
	gauge.ReportGauge(30)

	assertMetric(t, gather(t, registry), metric{
		name:  name,
		mtype: dto.MetricType_GAUGE,
		instances: []instance{
			{
				labels: tags,
				gauge:  gaugeValue(30),
			},
		},
	})
}

func TestTimerHistogram(t *testing.T) {
	registry := prom.NewRegistry()
	r := NewReporter(Options{
		Registerer:       registry,
		DefaultTimerType: HistogramTimerType,
		DefaultHistogramBuckets: []float64{
			50 * ms,
			250 * ms,
			1000 * ms,
			2500 * ms,
			10000 * ms,
		},
	})

	name := "test_timer"
	tags := map[string]string{
		"foo":  "bar",
		"test": "everything",
	}
	tags2 := map[string]string{
		"foo":  "baz",
		"test": "something",
	}
	vals := []time.Duration{
		23 * time.Millisecond,
		223 * time.Millisecond,
		320 * time.Millisecond,
	}
	vals2 := []time.Duration{
		1742 * time.Millisecond,
		3232 * time.Millisecond,
	}

	timer := r.AllocateTimer(name, tags)
	for _, v := range vals {
		timer.ReportTimer(v)
	}

	timer = r.AllocateTimer(name, tags2)
	for _, v := range vals2 {
		timer.ReportTimer(v)
	}

	assertMetric(t, gather(t, registry), metric{
		name:  name,
		mtype: dto.MetricType_HISTOGRAM,
		instances: []instance{
			{
				labels: tags,
				histogram: histogramValue(histogramVal{
					sampleCount: uint64(len(vals)),
					sampleSum:   durationFloatSum(vals),
					buckets: []histogramValBucket{
						{upperBound: 0.05, count: 1},
						{upperBound: 0.25, count: 2},
						{upperBound: 1.00, count: 3},
						{upperBound: 2.50, count: 3},
						{upperBound: 10.00, count: 3},
					},
				}),
			},
			{
				labels: tags2,
				histogram: histogramValue(histogramVal{
					sampleCount: uint64(len(vals2)),
					sampleSum:   durationFloatSum(vals2),
					buckets: []histogramValBucket{
						{upperBound: 0.05, count: 0},
						{upperBound: 0.25, count: 0},
						{upperBound: 1.00, count: 0},
						{upperBound: 2.50, count: 1},
						{upperBound: 10.00, count: 2},
					},
				}),
			},
		},
	})
}

func TestTimerSummary(t *testing.T) {
	registry := prom.NewRegistry()
	r := NewReporter(Options{
		Registerer:       registry,
		DefaultTimerType: SummaryTimerType,
		DefaultSummaryObjectives: map[float64]float64{
			0.5:   0.01,
			0.75:  0.001,
			0.95:  0.001,
			0.99:  0.001,
			0.999: 0.0001,
		},
	})

	name := "test_timer"
	tags := map[string]string{
		"foo":  "bar",
		"test": "everything",
	}
	tags2 := map[string]string{
		"foo":  "baz",
		"test": "something",
	}
	vals := []time.Duration{
		23 * time.Millisecond,
		223 * time.Millisecond,
		320 * time.Millisecond,
	}
	vals2 := []time.Duration{
		1742 * time.Millisecond,
		3232 * time.Millisecond,
	}

	timer := r.AllocateTimer(name, tags)
	for _, v := range vals {
		timer.ReportTimer(v)
	}

	timer = r.AllocateTimer(name, tags2)
	for _, v := range vals2 {
		timer.ReportTimer(v)
	}

	assertMetric(t, gather(t, registry), metric{
		name:  name,
		mtype: dto.MetricType_SUMMARY,
		instances: []instance{
			{
				labels: tags,
				summary: summaryValue(summaryVal{
					sampleCount: uint64(len(vals)),
					sampleSum:   durationFloatSum(vals),
					quantiles: []summaryValQuantile{
						{quantile: 0.50, value: 0.223},
						{quantile: 0.75, value: 0.32},
						{quantile: 0.95, value: 0.32},
						{quantile: 0.99, value: 0.32},
						{quantile: 0.999, value: 0.32},
					},
				}),
			},
			{
				labels: tags2,
				summary: summaryValue(summaryVal{
					sampleCount: uint64(len(vals2)),
					sampleSum:   durationFloatSum(vals2),
					quantiles: []summaryValQuantile{
						{quantile: 0.50, value: 1.742},
						{quantile: 0.75, value: 3.232},
						{quantile: 0.95, value: 3.232},
						{quantile: 0.99, value: 3.232},
						{quantile: 0.999, value: 3.232},
					},
				}),
			},
		},
	})
}

func TestOnRegisterError(t *testing.T) {
	var captured []error

	registry := prom.NewRegistry()
	r := NewReporter(Options{
		Registerer: registry,
		OnRegisterError: func(err error) {
			captured = append(captured, err)
		},
	})

	c := r.AllocateCounter("bad-name", nil)
	c.ReportCount(2)
	c.ReportCount(4)
	c = r.AllocateCounter("bad.name", nil)
	c.ReportCount(42)
	c.ReportCount(84)

	assert.Equal(t, 2, len(captured))
}

func gather(t *testing.T, r prom.Gatherer) []*dto.MetricFamily {
	metrics, err := r.Gather()
	require.NoError(t, err)
	return metrics
}

func counterValue(v float64) *dto.Counter {
	return &dto.Counter{Value: &v}
}

func gaugeValue(v float64) *dto.Gauge {
	return &dto.Gauge{Value: &v}
}

type histogramVal struct {
	sampleCount uint64
	sampleSum   float64
	buckets     []histogramValBucket
}

type histogramValBucket struct {
	count      uint64
	upperBound float64
}

func histogramValue(v histogramVal) *dto.Histogram {
	r := &dto.Histogram{
		SampleCount: &v.sampleCount,
		SampleSum:   &v.sampleSum,
	}
	for _, b := range v.buckets {
		b := b // or else the addresses we take will be static
		r.Bucket = append(r.Bucket, &dto.Bucket{
			CumulativeCount: &b.count,
			UpperBound:      &b.upperBound,
		})
	}
	return r
}

type summaryVal struct {
	sampleCount uint64
	sampleSum   float64
	quantiles   []summaryValQuantile
}

type summaryValQuantile struct {
	quantile float64
	value    float64
}

func summaryValue(v summaryVal) *dto.Summary {
	r := &dto.Summary{
		SampleCount: &v.sampleCount,
		SampleSum:   &v.sampleSum,
	}
	for _, q := range v.quantiles {
		q := q // or else the addresses we take will be static
		r.Quantile = append(r.Quantile, &dto.Quantile{
			Quantile: &q.quantile,
			Value:    &q.value,
		})
	}
	return r
}

func durationFloatSum(v []time.Duration) float64 {
	var sum float64
	for _, d := range v {
		sum += durationFloat(d)
	}
	return sum
}

func durationFloat(d time.Duration) float64 {
	return float64(d) / float64(time.Second)
}

type metric struct {
	name      string
	mtype     dto.MetricType
	instances []instance
}

type instance struct {
	labels    map[string]string
	counter   *dto.Counter
	gauge     *dto.Gauge
	histogram *dto.Histogram
	summary   *dto.Summary
}

func assertMetric(
	t *testing.T,
	metrics []*dto.MetricFamily,
	query metric,
) {
	q := query
	msgFmt := func(msg string, v ...interface{}) string {
		prefix := fmt.Sprintf("assert fail for metric name=%s, type=%s: ",
			q.name, q.mtype.String())
		return fmt.Sprintf(prefix+msg, v...)
	}
	for _, m := range metrics {
		if m.GetName() != q.name || m.GetType() != q.mtype {
			continue
		}
		if len(q.instances) == 0 {
			require.Fail(t, msgFmt("no instances to assert"))
		}
		for _, i := range q.instances {
			found := false
			for _, j := range m.GetMetric() {
				if len(i.labels) != len(j.GetLabel()) {
					continue
				}

				notMatched := make(map[string]string, len(i.labels))
				for k, v := range i.labels {
					notMatched[k] = v
				}

				for _, pair := range j.GetLabel() {
					notMatchedValue, matches := notMatched[pair.GetName()]
					if matches && pair.GetValue() == notMatchedValue {
						delete(notMatched, pair.GetName())
					}
				}

				if len(notMatched) != 0 {
					continue
				}

				found = true

				switch {
				case i.counter != nil:
					require.NotNil(t, j.GetCounter())
					assert.Equal(t, i.counter.GetValue(), j.GetCounter().GetValue())
				case i.gauge != nil:
					require.NotNil(t, j.GetGauge())
					assert.Equal(t, i.gauge.GetValue(), j.GetGauge().GetValue())
				case i.histogram != nil:
					require.NotNil(t, j.GetHistogram())
					assert.Equal(t, i.histogram.GetSampleCount(), j.GetHistogram().GetSampleCount())
					assert.Equal(t, i.histogram.GetSampleSum(), j.GetHistogram().GetSampleSum())
					require.Equal(t, len(i.histogram.GetBucket()), len(j.GetHistogram().GetBucket()))
					for idx, b := range i.histogram.GetBucket() {
						actual := j.GetHistogram().GetBucket()[idx]
						assert.Equal(t, b.GetCumulativeCount(), actual.GetCumulativeCount())
						assert.Equal(t, b.GetUpperBound(), actual.GetUpperBound())
					}
				case i.summary != nil:
					require.NotNil(t, j.GetSummary())
					assert.Equal(t, i.summary.GetSampleCount(), j.GetSummary().GetSampleCount())
					assert.Equal(t, i.summary.GetSampleSum(), j.GetSummary().GetSampleSum())
					require.Equal(t, len(i.summary.GetQuantile()), len(j.GetSummary().GetQuantile()))
					for idx, q := range i.summary.GetQuantile() {
						actual := j.GetSummary().GetQuantile()[idx]
						assert.Equal(t, q.GetQuantile(), actual.GetQuantile())
						assert.Equal(t, q.GetValue(), actual.GetValue())
					}
				}
			}
			if !found {
				require.Fail(t, msgFmt("instance not found labels=%v", i.labels))
			}
		}
		return
	}
	require.Fail(t, msgFmt("metric not found"))
}
