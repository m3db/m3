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

package graphite

import (
	"regexp"
	"testing"

	"github.com/m3db/m3/src/query/graphite/ts"
)

var (
	applicationCountsRegex = regexp.MustCompile(`^stats(\.[^\.]+)?\.counts\..*`)
	benchMixIDs            = []string{
		"stats.bar.counts.donkey.kong.barrels",
		"stats.bar.counts.test",
		"stats.bar.gauges.donkey.kong.barrels",
		"stats.bar.timers.test",
		"fake.bar.counts.test",
		"servers.testabc-bar.counts.test",
	}
)

func BenchmarkFindConsolidationApproach(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for _, id := range benchMixIDs {
			FindConsolidationApproach(id)
		}
	}
}

func BenchmarkFindConsolidationApproachRegex(b *testing.B) {
	for n := 0; n < b.N; n++ {
		for _, id := range benchMixIDs {
			findConsolidationApproachRegex(id)
		}
	}
}

func findConsolidationApproachRegex(id string) ts.ConsolidationApproach {
	if applicationCountsRegex.MatchString(id) {
		return ts.ConsolidationSum
	}

	return ts.ConsolidationAvg
}
