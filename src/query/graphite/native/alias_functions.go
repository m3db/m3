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

package native

import (
	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/ts"
)

// alias takes one metric or a wildcard seriesList and a string in quotes.
// Prints the string instead of the metric name in the legend.
func alias(ctx *common.Context, series singlePathSpec, a string) (ts.SeriesList, error) {
	return common.Alias(ctx, ts.SeriesList(series), a)
}

// aliasByMetric takes a seriesList and applies an alias derived from the base
// metric name.
func aliasByMetric(ctx *common.Context, series singlePathSpec) (ts.SeriesList, error) {
	return common.AliasByMetric(ctx, ts.SeriesList(series))
}

// aliasByNode renames a time series result according to a subset of the nodes
// in its hierarchy.
func aliasByNode(ctx *common.Context, seriesList singlePathSpec, nodes ...int) (ts.SeriesList, error) {
	return common.AliasByNode(ctx, ts.SeriesList(seriesList), nodes...)
}

// aliasSub runs series names through a regex search/replace.
func aliasSub(ctx *common.Context, input singlePathSpec, search, replace string) (ts.SeriesList, error) {
	return common.AliasSub(ctx, ts.SeriesList(input), search, replace)
}
