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

package ts

import (
	"fmt"

	"github.com/m3db/m3/src/query/graphite/stats"
)

// ConsolidationApproach defines an approach to consolidating multiple datapoints
type ConsolidationApproach string

// The standard set of consolidation functions
const (
	ConsolidationAvg     ConsolidationApproach = "avg"
	ConsolidationMin     ConsolidationApproach = "min"
	ConsolidationMax     ConsolidationApproach = "max"
	ConsolidationSum     ConsolidationApproach = "sum"
	ConsolidationAverage ConsolidationApproach = "average" // just an alias to avg but for backward-compatibility
)

// SafeFunc returns a boolean indicating whether this is a valid consolidation approach,
// and if so, the corresponding ConsolidationFunc.
func (ca ConsolidationApproach) SafeFunc() (ConsolidationFunc, bool) {
	f, ok := consolidationFuncs[ca]
	return f, ok
}

// Func returns the ConsolidationFunc implementing the ConsolidationApproach
func (ca ConsolidationApproach) Func() ConsolidationFunc {
	f, ok := ca.SafeFunc()
	if !ok {
		panic(fmt.Sprintf("No consolidation func for %s", ca))
	}

	return f
}

// FromStatistics retrieves the statistic used for consolidation
func (ca ConsolidationApproach) FromStatistics(a stats.Statistics) float64 {
	switch ca {
	case ConsolidationAvg, ConsolidationAverage:
		return a.Mean
	case ConsolidationMin:
		return a.Min
	case ConsolidationMax:
		return a.Max
	case ConsolidationSum:
		return a.Sum
	default:
		panic(fmt.Sprintf("No aggregation access for %s", ca))
	}
}

var (
	consolidationFuncs = map[ConsolidationApproach]ConsolidationFunc{
		ConsolidationAvg:     Avg,
		ConsolidationAverage: Avg,
		ConsolidationMin:     Min,
		ConsolidationMax:     Max,
		ConsolidationSum:     Sum,
	}
)
