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
	"regexp"
)

type regexpRewriter struct {
	okStart, okRest [256]bool
	replacement     byte
}

func newRegexpRewriter(startRe, restRe string) *regexpRewriter {
	createArray := func(okRe string) (ret [256]bool) {
		re := regexp.MustCompile(okRe)
		// Check for only 7 bit non-control ASCII characters
		for i := 32; i < 128; i++ {
			if re.Match([]byte{byte(i)}) {
				ret[i] = true
			}
		}
		return
	}
	return &regexpRewriter{okStart: createArray(startRe), okRest: createArray(restRe), replacement: byte('_')}
}

func (rr *regexpRewriter) rewrite(input []byte) {
	if len(input) == 0 {
		return
	}
	if !rr.okStart[input[0]] {
		input[0] = rr.replacement
	}
	for i := 1; i < len(input); i++ {
		if !rr.okRest[input[i]] {
			input[i] = rr.replacement
		}
	}
}

// Utility, which handles both __name__ ('metric') tag, as well as
// rest of tags ('labels')
//
// It allow using any influxdb client, rewriting the tag names + the
// magic __name__ tag to match what Prometheus expects
type promRewriter struct {
	metric, metricTail, label *regexpRewriter
}

func newPromRewriter() *promRewriter {
	return &promRewriter{
		metric: newRegexpRewriter(
			"[a-zA-Z_:]",
			"[a-zA-Z0-9_:]"),
		metricTail: newRegexpRewriter(
			"[a-zA-Z0-9_:]",
			"[a-zA-Z0-9_:]"),
		label: newRegexpRewriter(
			"[a-zA-Z_]", "[a-zA-Z0-9_]")}
}

func (pr *promRewriter) rewriteMetric(data []byte) {
	pr.metric.rewrite(data)
}

func (pr *promRewriter) rewriteMetricTail(data []byte) {
	pr.metricTail.rewrite(data)
}

func (pr *promRewriter) rewriteLabel(data []byte) {
	pr.label.rewrite(data)
}
