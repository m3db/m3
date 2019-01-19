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
