package graphite

import (
	"regexp"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/ts"
)

const (
	metricsTypeNodeIdx = 2
	timerCountSuffix   = ".count"
)

// A RetentionPeriod emulates a Graphite retention period on top of M3,
// consolidating raw datapoints into a defined time window using a specific
// consolidation function.
type RetentionPeriod struct {
	pattern  *regexp.Regexp
	policies []*RetentionPolicy
}

// A RetentionPolicy describes how Graphite aggregates a given metric within a
// given archive
type RetentionPolicy struct {
	TTL           time.Duration
	UnitPerStep   time.Duration
	Consolidation ts.ConsolidationApproach
	RoundToUnit   bool
}

var (
	// NB(mmihic): These need to be ordered
	graphiteRetentionPeriods = []*RetentionPeriod{
		{regexp.MustCompile(`^stats\.sjc1\.artemis\..*\.storm\.`), []*RetentionPolicy{
			{time.Hour * 24 * 180, time.Second * 60, ts.ConsolidationAvg, true},
			{time.Hour * 24 * 365 * 2, time.Second * 600, ts.ConsolidationAvg, true},
		}},
		{regexp.MustCompile(`^stats(\.[^\.]+)?\.counts\..*`), []*RetentionPolicy{
			{time.Hour * 24 * 2, time.Second * 10, ConsolidationFuncForMetricType(Counts), false},
			{time.Hour * 24 * 90, time.Second * 60, ConsolidationFuncForMetricType(Counts), false},
			{time.Hour * 24 * 365, time.Second * 600, ConsolidationFuncForMetricType(Counts), false},
		}},
		{regexp.MustCompile(`^stats(\.[^\.]+)?\.timers\..*\.count$`), []*RetentionPolicy{
			{time.Hour * 24 * 2, time.Second * 10, ts.ConsolidationSum, false},
			{time.Hour * 24 * 90, time.Second * 60, ts.ConsolidationSum, false},
		}},
		{regexp.MustCompile(`^stats\..*`), []*RetentionPolicy{
			{time.Hour * 24 * 2, time.Second * 10, ts.ConsolidationAvg, false},
			{time.Hour * 24 * 90, time.Second * 60, ts.ConsolidationAvg, false},
			{time.Hour * 24 * 365, time.Second * 600, ts.ConsolidationAvg, false},
		}},
		{regexp.MustCompile(`^statsdex(\.[^\.]+)?\..*`), []*RetentionPolicy{
			{time.Hour * 24 * 2, time.Second * 10, ts.ConsolidationAvg, false},
			{time.Hour * 24 * 90, time.Second * 60, ts.ConsolidationAvg, false},
			{time.Hour * 24 * 365, time.Second * 600, ts.ConsolidationAvg, false},
		}},
	}

	defaultRetentionPeriod = &RetentionPeriod{
		regexp.MustCompile(".*"), []*RetentionPolicy{
			{time.Second * 129600, time.Second * 60, ts.ConsolidationAvg, true},
		},
	}

	m3ServerRetentionPeriod = &RetentionPolicy{
		time.Second * 129600, time.Second * 60, ts.ConsolidationAvg, true,
	}
)

// AggregatedMetrics matches metrics going through the aggregation tier
func AggregatedMetrics(id string) bool {
	return strings.HasPrefix(id, "stats.")
}

// FindConsolidationApproach finds the consolidation approach for an ID
// much faster than finding the retention policies via regexp, this
// should be kept in sync with the list of retention policies above
func FindConsolidationApproach(id string) ts.ConsolidationApproach {
	if AggregatedMetrics(id) {
		switch ExtractNthMetricPart(id, metricsTypeNodeIdx) {
		case CountsStr:
			return ts.ConsolidationSum
		case TimersStr:
			if strings.HasSuffix(id, timerCountSuffix) {
				// Timer count
				return ts.ConsolidationSum
			}
		}
	}

	return ts.ConsolidationAvg
}

// FindRetentionPolicy finds the retention policy for the given metric id and
// distance back in time being searched
func FindRetentionPolicy(id string, age time.Duration) *RetentionPolicy {
	for _, period := range graphiteRetentionPeriods {
		if period.pattern.MatchString(id) {
			for _, policy := range period.policies {
				if age < policy.TTL {
					return policy
				}
			}
		}
	}

	return defaultRetentionPeriod.policies[0]
}
