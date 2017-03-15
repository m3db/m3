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

	"github.com/apache/thrift/lib/go/thrift"
)

const (
	updaters = 10
	updates  = 1000
	numIds   = 10

	testID = "stats.$dc.gauges.m3+" +
		"servers.my-internal-server-$dc.network.eth0_tx_colls+" +
		"dc=$dc,domain=production.$zone,env=production,pipe=$pipe,service=servers,type=gauge"
)

func BenchmarkNewMetric(b *testing.B) {
	protocolFactory := thrift.NewTCompactProtocolFactory()
	resourcePool := newResourcePool(protocolFactory)
	benchReporter := &reporter{resourcePool: resourcePool}

	for n := 0; n < b.N; n++ {
		benchReporter.newMetric("foo", nil, counterType)
	}
}

func BenchmarkCalulateSize(b *testing.B) {
	protocolFactory := thrift.NewTCompactProtocolFactory()
	resourcePool := newResourcePool(protocolFactory)
	benchReporter := &reporter{resourcePool: resourcePool}

	val := int64(123456)
	met := benchReporter.newMetric("foo", nil, counterType)
	met.MetricValue.Count.I64Value = &val

	for n := 0; n < b.N; n++ {
		benchReporter.calculateSize(met)
	}
}
