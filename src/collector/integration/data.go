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

package integration

import (
	"bytes"

	"github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

type idGenerator func(int) id.ID

// nolint: structcheck
type outputResult struct {
	idGen        idGenerator
	policiesList policy.PoliciesList
}

type outputResults []outputResult

// nolint: structcheck
type metricWithPoliciesList struct {
	metric       interface{}
	policiesList policy.PoliciesList
}

type resultsByTypeAscIDAsc []metricWithPoliciesList

func (r resultsByTypeAscIDAsc) Len() int      { return len(r) }
func (r resultsByTypeAscIDAsc) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func (r resultsByTypeAscIDAsc) Less(i, j int) bool {
	it, iid := metricTypeAndIDFrom(r[i].metric)
	jt, jid := metricTypeAndIDFrom(r[j].metric)
	if it < jt {
		return true
	}
	if it > jt {
		return false
	}
	return bytes.Compare(iid, jid) < 0
}

func metricTypeAndIDFrom(v interface{}) (unaggregated.Type, []byte) {
	var (
		t  unaggregated.Type
		id []byte
	)
	switch value := v.(type) {
	case unaggregated.Counter:
		t = unaggregated.CounterType
		id = value.ID
	case unaggregated.BatchTimer:
		t = unaggregated.BatchTimerType
		id = value.ID
	case unaggregated.Gauge:
		t = unaggregated.GaugeType
		id = value.ID
	}
	return t, id
}
