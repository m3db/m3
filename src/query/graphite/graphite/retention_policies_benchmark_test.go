package graphite

import (
	"regexp"
	"testing"

	"github.com/m3db/m3/src/query/graphite/ts"
)

var (
	applicationCountsRegex = regexp.MustCompile("^stats(\\.[^\\.]+)?\\.counts\\..*")
	benchMixIDs            = []string{
		"stats.sjc1.counts.donkey.kong.barrels",
		"stats.sjc1.counts.test",
		"stats.sjc1.gauges.donkey.kong.barrels",
		"stats.sjc1.timers.test",
		"fake.sjc1.counts.test",
		"servers.testabc-sjc1.counts.test",
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
