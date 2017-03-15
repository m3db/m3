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

package m3

import (
	"testing"

	m3thrift "github.com/uber-go/tally/m3/thrift"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/require"
)

func TestM3ResourcePoolMetric(t *testing.T) {
	p := newResourcePool(thrift.NewTCompactProtocolFactory())

	var v int64
	cm := p.getMetric()
	cmv := p.getValue()
	cm.MetricValue = cmv
	cv := p.getCount()
	cmv.Count = cv
	cv.I64Value = &v
	cm.Tags = map[*m3thrift.MetricTag]bool{createTag(p, "t1", "v1"): true}

	gm := p.getMetric()
	gmv := p.getValue()
	gm.MetricValue = gmv
	gv := p.getGauge()
	gmv.Gauge = gv
	gv.I64Value = &v

	tm := p.getMetric()
	tmv := p.getValue()
	tm.MetricValue = tmv
	tv := p.getTimer()
	tmv.Timer = tv
	tv.I64Value = &v

	p.releaseMetric(tm)
	p.releaseMetric(gm)
	p.releaseMetric(cm)

	cm2 := p.getMetric()
	gm2 := p.getMetric()
	tm2 := p.getMetric()

	require.Nil(t, cm2.MetricValue)
	require.Nil(t, gm2.MetricValue)
	require.Nil(t, tm2.MetricValue)
}

func TestM3ResourcePoolMetricValue(t *testing.T) {
	p := newResourcePool(thrift.NewTCompactProtocolFactory())
	var v int64
	cmv := p.getValue()
	cv := p.getCount()
	cmv.Count = cv
	cv.I64Value = &v

	gmv := p.getValue()
	gv := p.getGauge()
	gmv.Gauge = gv
	gv.I64Value = &v

	tmv := p.getValue()
	tv := p.getTimer()
	tmv.Timer = tv
	tv.I64Value = &v

	p.releaseMetricValue(tmv)
	p.releaseMetricValue(gmv)
	p.releaseMetricValue(cmv)

	cmv2 := p.getValue()
	gmv2 := p.getValue()
	tmv2 := p.getValue()

	require.Nil(t, cmv2.Count)
	require.Nil(t, gmv2.Gauge)
	require.Nil(t, tmv2.Timer)
}

func TestM3ResourcePoolBatch(t *testing.T) {
	p := newResourcePool(thrift.NewTCompactProtocolFactory())
	b := p.getBatch()
	b.Metrics = append(b.Metrics, p.getMetric())
	p.releaseBatch(b)
	b2 := p.getBatch()
	require.Equal(t, 0, len(b2.Metrics))
}
