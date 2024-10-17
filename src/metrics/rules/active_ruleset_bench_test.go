package rules

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/id/m3"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"
)

var benchmarkCases = []struct {
	name string
	id   string
}{{
	name: "rollup ID",
	id: fmt.Sprintf(
		"m3+%s+m3_rollup=true,rtagName1=val1,rtagName2=val2",
		newTestRuleName("rollup", 5),
	),
}, {
	name: "mapping ID",
	id:   "m3+test+rtagName1=val1,rtagName2=val2",
}}

// nolint: lll
// Results (2024-10-18, Macbook Pro M1 Max):
//
// goos: darwin
// goarch: arm64
// pkg: github.com/m3db/m3/src/metrics/rules
// BenchmarkActiveRuleSet_ReverseMatch
// BenchmarkActiveRuleSet_ReverseMatch/rollup_ID
// BenchmarkActiveRuleSet_ReverseMatch/rollup_ID-10         	  230866	      5346 ns/op	    4304 B/op	      36 allocs/op
// BenchmarkActiveRuleSet_ReverseMatch/mapping_ID
// BenchmarkActiveRuleSet_ReverseMatch/mapping_ID-10        	   21996	     54009 ns/op	  108913 B/op	     467 allocs/op
// PASS
func BenchmarkActiveRuleSet_ReverseMatch(b *testing.B) {
	for _, tc := range benchmarkCases {
		b.Run(tc.name, func(b *testing.B) {
			b.ReportAllocs()

			id := m3.NewID([]byte(tc.id), nil)

			deps := setupActiveRuleSetBenchmark(b)
			sanityCheckBenchmark(b, deps, id)

			// N.B.: this is actually a heavy alloc, it turns out.
			typesOptions := aggregation.NewTypesOptions()

			b.ResetTimer()

			var benchResult MatchResult
			for i := 0; i < b.N; i++ {
				r, _ := deps.RuleSet.ReverseMatch(
					id,
					0,
					// max int
					math.MaxInt64,
					metric.CounterType,
					aggregation.Sum,
					false,
					typesOptions,
				)
				benchResult = r
			}
			require.NotEmpty(b, benchResult.ForExistingIDAt(0))
		})
	}
}

// TestActiveRuleSet_Benchmark makes sure the benchmark continues to give valid results.
func TestActiveRuleSet_Benchmark(t *testing.T) {
	for _, tc := range benchmarkCases {
		t.Run(tc.name, func(t *testing.T) {
			id := m3.NewID([]byte(tc.id), nil)

			deps := setupActiveRuleSetBenchmark(t)
			sanityCheckBenchmark(t, deps, id)
		})
	}
}

type ruleSetBenchmarkDeps struct {
	RuleSet        *activeRuleSet
	AggTypeOptions aggregation.TypesOptions
}

func setupActiveRuleSetBenchmark(t testing.TB) ruleSetBenchmarkDeps {
	rollupRules := newBenchmarkRollupRules(t, 20, 10)
	mappingRules := newBenchmarkMappingRules(t, 20, 10)
	activeRules := newActiveRuleSet(0, mappingRules, rollupRules,
		filters.TagsFilterOptions{
			NameTagKey:          []byte("name"),
			NameAndTagsFn:       m3.NameAndTags,
			SortedTagIteratorFn: m3.NewSortedTagIterator,
		},
		m3.NewRollupID,
		m3.IsRollupID,
		map[uint64]struct{}{},
	)

	return ruleSetBenchmarkDeps{
		RuleSet:        activeRules,
		AggTypeOptions: aggregation.NewTypesOptions(),
	}
}

func sanityCheckBenchmark(t testing.TB, deps ruleSetBenchmarkDeps, id metricid.ID) {
	matchResult, err := deps.RuleSet.ReverseMatch(
		id,
		0,
		// max int
		math.MaxInt64,
		metric.CounterType,
		aggregation.Sum,
		false,
		deps.AggTypeOptions,
	)
	require.NoError(t, err)

	// Check most recent snapshot.
	matchResults := matchResult.ForExistingIDAt(time.Now().UnixNano())
	require.NotEmpty(t, matchResults)
	assert.False(t, matchResults[0].Pipelines[0].StoragePolicies.IsDefault())
}

func newTestRuleName(ruleType string, idx int) string {
	return fmt.Sprintf("rule-%s-%d", ruleType, idx)
}

func newBenchmarkMappingRules(t testing.TB, numRules int, numSnapshotsPerRule int) []*mappingRule {
	rules := make([]*mappingRule, 0, numRules)
	for i := 0; i < numRules; i++ {

		// TODO: offset/interleave this with the rollup rules. Use a globally advancing clock.
		cutoverStart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

		snapshots := make([]*mappingRuleSnapshot, 0, numSnapshotsPerRule)
		for j := 0; j < numSnapshotsPerRule; j++ {
			snapshot := newBenchmarkMappingRuleSnapshot(t)
			snapshot.cutoverNanos = cutoverStart.Add(time.Duration(j) * time.Hour).UnixNano()

			snapshots = append(snapshots, snapshot)
		}

		rule := newBenchmarkMappingRule(newTestRuleName("mapping", i))
		rule.snapshots = snapshots

		rules = append(rules, rule)
	}
	return rules
}

func newBenchmarkMappingRule(name string) *mappingRule {
	return &mappingRule{
		uuid: name,
	}
}

func newBenchmarkMappingRuleSnapshot(t testing.TB) *mappingRuleSnapshot {
	filter, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "val1"},
		},
		filters.Conjunction,
		filters.TagsFilterOptions{
			NameTagKey:          []byte("name"),
			NameAndTagsFn:       m3.NameAndTags,
			SortedTagIteratorFn: m3.NewSortedTagIterator,
		},
	)
	require.NoError(t, err)
	ms := &mappingRuleSnapshot{
		name:               "mappingRule10.snapshot1",
		tombstoned:         false,
		cutoverNanos:       100000,
		filter:             filter,
		lastUpdatedAtNanos: 105000,
		lastUpdatedBy:      "test",
		storagePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
		},
	}
	return ms
}

func newBenchmarkRollupRules(t testing.TB, numRules int, numSnapshotsPerRule int) []*rollupRule {
	cutoverStart := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)

	rules := make([]*rollupRule, 0, numRules)
	for i := 0; i < numRules; i++ {
		ruleName := newTestRuleName("rollup", i)

		snapshots := make([]*rollupRuleSnapshot, 0, numSnapshotsPerRule)
		for j := 0; j < numSnapshotsPerRule; j++ {
			snapshots = append(
				snapshots,
				newTestRollupRuleSnapshot(t, ruleName, func(snapshot *rollupRuleSnapshot) {
					snapshot.cutoverNanos = cutoverStart.Add(time.Duration(j) * time.Hour).
						UnixNano()
				}),
			)
		}

		rule := newTestRollupRule(nil, ruleName, func(rule *rollupRule) {
			rule.snapshots = snapshots
		})

		rules = append(rules, rule)
	}
	return rules
}

func newTestRollupRule(t testing.TB, ruleName string, opts ...func(rr *rollupRule)) *rollupRule {
	rr := &rollupRule{
		uuid:      ruleName,
		snapshots: []*rollupRuleSnapshot{newTestRollupRuleSnapshot(t, ruleName)},
	}

	for _, opt := range opts {
		opt(rr)
	}
	return rr
}

func newTestRollupRuleSnapshot(
	t testing.TB,
	rollupNewName string,
	opts ...func(rr *rollupRuleSnapshot),
) *rollupRuleSnapshot {
	rollupOp, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		rollupNewName,
		[]string{"rtagName1", "rtagName2"},
		aggregation.MustCompressTypes(aggregation.Count),
	)
	require.NoError(t, err)

	filter, err := filters.NewTagsFilter(
		filters.TagFilterValueMap{
			"rtagName1": filters.FilterValue{Pattern: "val1"},
		},
		filters.Conjunction,
		testTagsFilterOptions(),
	)
	require.NoError(t, err)
	rs := &rollupRuleSnapshot{
		name:               "rollupRule10.snapshot1",
		tombstoned:         false,
		cutoverNanos:       100000,
		filter:             filter,
		lastUpdatedAtNanos: 105000,
		lastUpdatedBy:      "test",
		targets: []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type:   pipeline.RollupOpType,
						Rollup: rollupOp,
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Second, 10*time.Hour),
				},
			},
		},
	}
	for _, opt := range opts {
		opt(rs)
	}
	return rs
}
