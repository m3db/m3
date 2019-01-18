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
