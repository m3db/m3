// Copyright (c) 2019 Uber Technologies, Inc.
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

package influxdb

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

type test struct {
	in, outMetric, outMetricTail, outLabel string
}

func TestPromRewriter(t *testing.T) {
	r := newPromRewriter()
	tests := []test{{"foo", "foo", "foo", "foo"},
		{".bar", "_bar", "_bar", "_bar"},
		{"b.ar", "b_ar", "b_ar", "b_ar"},
		{":bar", ":bar", ":bar", "_bar"},
		{"ba:r", "ba:r", "ba:r", "ba_r"},
		{"9bar", "_bar", "9bar", "_bar"},
	}
	for _, test := range tests {
		in1 := []byte(test.in)
		r.rewriteMetric(in1)
		assert.Equal(t, test.outMetric, string(in1))
		in2 := []byte(test.in)
		r.rewriteMetricTail(in2)
		assert.Equal(t, test.outMetricTail, string(in2))
		in3 := []byte(test.in)
		r.rewriteLabel(in3)
		assert.Equal(t, test.outLabel, string(in3))
	}
}
