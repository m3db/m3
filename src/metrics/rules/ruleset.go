// Copyright (c) 2017 Uber Technologies, Inc.
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

package rules

import (
	"bytes"
	"errors"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	metricID "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"
)

const (
	timeNanosMax = int64(math.MaxInt64)
)

var (
	errNilRuleSetSchema = errors.New("nil rule set schema")
)

// MatchMode determines how match is performed.
type MatchMode int

// List of supported match modes.
const (
	// When performing matches in Forward mode, the matcher matches the given id against
	// both the mapping rules and rollup rules to find out the applicable mapping policies
	// and rollup policies.
	Forward MatchMode = iota

	// When performing matches in Reverse mode, the matcher find the applicable mapping
	// policies for the given id.
	Reverse
)

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// MatchAll returns the applicable policies for a metric id between [fromNanos, toNanos).
	MatchAll(id []byte, fromNanos, toNanos int64, matchMode MatchMode) MatchResult
}

type activeRuleSet struct {
	mappingRules    []*mappingRule
	rollupRules     []*rollupRule
	cutoverTimesAsc []int64
	tagFilterOpts   filters.TagsFilterOptions
	newRollupIDFn   metricID.NewIDFn
	isRollupIDFn    metricID.MatchIDFn
}

func newActiveRuleSet(
	mappingRules []*mappingRule,
	rollupRules []*rollupRule,
	tagFilterOpts filters.TagsFilterOptions,
	newRollupIDFn metricID.NewIDFn,
	isRollupIDFn metricID.MatchIDFn,
) *activeRuleSet {
	uniqueCutoverTimes := make(map[int64]struct{})
	for _, mappingRule := range mappingRules {
		for _, snapshot := range mappingRule.snapshots {
			uniqueCutoverTimes[snapshot.cutoverNanos] = struct{}{}
		}
	}
	for _, rollupRule := range rollupRules {
		for _, snapshot := range rollupRule.snapshots {
			uniqueCutoverTimes[snapshot.cutoverNanos] = struct{}{}
		}
	}

	cutoverTimesAsc := make([]int64, 0, len(uniqueCutoverTimes))
	for t := range uniqueCutoverTimes {
		cutoverTimesAsc = append(cutoverTimesAsc, t)
	}
	sort.Sort(int64Asc(cutoverTimesAsc))

	return &activeRuleSet{
		mappingRules:    mappingRules,
		rollupRules:     rollupRules,
		cutoverTimesAsc: cutoverTimesAsc,
		tagFilterOpts:   tagFilterOpts,
		newRollupIDFn:   newRollupIDFn,
		isRollupIDFn:    isRollupIDFn,
	}
}

// NB(xichen): could make this more efficient by keeping track of matched rules
// at previous iteration and incrementally update match results.
func (as *activeRuleSet) MatchAll(
	id []byte,
	fromNanos, toNanos int64,
	matchMode MatchMode,
) MatchResult {
	if matchMode == Forward {
		return as.matchAllForward(id, fromNanos, toNanos)
	}
	return as.matchAllReverse(id, fromNanos, toNanos)
}

func (as *activeRuleSet) matchAllForward(id []byte, fromNanos, toNanos int64) MatchResult {
	var (
		nextIdx            = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos   = as.cutoverNanosAt(nextIdx)
		currMappingResults = policy.PoliciesList{as.mappingsForNonRollupID(id, fromNanos)}
		currRollupResults  = as.rollupResultsFor(id, fromNanos)
	)
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		nextMappingPolicies := as.mappingsForNonRollupID(id, nextCutoverNanos)
		currMappingResults = mergeMappingResults(currMappingResults, nextMappingPolicies)
		nextRollupResults := as.rollupResultsFor(id, nextCutoverNanos)
		currRollupResults = mergeRollupResults(currRollupResults, nextRollupResults, nextCutoverNanos)
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}

	// The result expires when it reaches the first cutover time after toNanos among all
	// active rules because the metric may then be matched against a different set of rules.
	return NewMatchResult(nextCutoverNanos, currMappingResults, currRollupResults)
}

func (as *activeRuleSet) matchAllReverse(id []byte, fromNanos, toNanos int64) MatchResult {
	var (
		nextIdx            = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos   = as.cutoverNanosAt(nextIdx)
		currMappingResults policy.PoliciesList
		isRollupID         bool
	)

	// Determine whether the id is a rollup metric id.
	name, tags, err := as.tagFilterOpts.NameAndTagsFn(id)
	if err == nil {
		isRollupID = as.isRollupIDFn(name, tags)
	}

	if currMappingPolicies, found := as.reverseMappingsFor(id, name, tags, isRollupID, fromNanos); found {
		currMappingResults = append(currMappingResults, currMappingPolicies)
	}
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		if nextMappingPolicies, found := as.reverseMappingsFor(id, name, tags, isRollupID, nextCutoverNanos); found {
			currMappingResults = mergeMappingResults(currMappingResults, nextMappingPolicies)
		}
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}

	// The result expires when it reaches the first cutover time after toNanos among all
	// active rules because the metric may then be matched against a different set of rules.
	return NewMatchResult(nextCutoverNanos, currMappingResults, nil)
}

func (as *activeRuleSet) reverseMappingsFor(
	id, name, tags []byte,
	isRollupID bool,
	timeNanos int64,
) (policy.StagedPolicies, bool) {
	if !isRollupID {
		return as.mappingsForNonRollupID(id, timeNanos), true
	}
	return as.mappingsForRollupID(name, tags, timeNanos)
}

// NB(xichen): in order to determine the applicable policies for a rollup metric, we need to
// match the id against rollup rules to determine which rollup rules are applicable, under the
// assumption that no two rollup targets in the same namespace may have the same rollup metric
// name and the list of rollup tags. Otherwise, a rollup metric could potentially match more
// than one rollup rule with different policies even though only one of the matched rules was
// used to produce the given rollup metric id due to its tag filters, thereby causing the wrong
// staged policies to be returned. This also implies at any given time, at most one rollup target
// may match the given rollup id.
func (as *activeRuleSet) mappingsForRollupID(
	name, tags []byte,
	timeNanos int64,
) (policy.StagedPolicies, bool) {
	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.ActiveSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		for _, target := range snapshot.targets {
			if !bytes.Equal(target.Name, name) {
				continue
			}
			var (
				tagIter      = as.tagFilterOpts.SortedTagIteratorFn(tags)
				hasMoreTags  = tagIter.Next()
				targetTagIdx = 0
			)
			for hasMoreTags && targetTagIdx < len(target.Tags) {
				tagName, _ := tagIter.Current()
				res := bytes.Compare(tagName, target.Tags[targetTagIdx])
				if res == 0 {
					targetTagIdx++
					hasMoreTags = tagIter.Next()
					continue
				}
				// If one of the target tags is not found in the id, this is considered
				// a non-match so bail immediately.
				if res > 0 {
					break
				}
				hasMoreTags = tagIter.Next()
			}
			tagIter.Close()

			// If all of the target tags are matched, this is considered as a match.
			if targetTagIdx == len(target.Tags) {
				policies := make([]policy.Policy, len(target.Policies))
				copy(policies, target.Policies)
				resolved := resolvePolicies(policies)
				return policy.NewStagedPolicies(snapshot.cutoverNanos, false, resolved), true
			}
		}
	}

	return policy.DefaultStagedPolicies, false
}

// NB(xichen): if the given id is not a rollup id, we need to match it against the mapping
// rules to determine the corresponding mapping policies.
func (as *activeRuleSet) mappingsForNonRollupID(id []byte, timeNanos int64) policy.StagedPolicies {
	var (
		cutoverNanos int64
		policies     []policy.Policy
	)
	for _, mappingRule := range as.mappingRules {
		snapshot := mappingRule.ActiveSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		if !snapshot.filter.Matches(id) {
			continue
		}
		if cutoverNanos < snapshot.cutoverNanos {
			cutoverNanos = snapshot.cutoverNanos
		}
		policies = append(policies, snapshot.policies...)
	}
	if cutoverNanos == 0 && len(policies) == 0 {
		return policy.DefaultStagedPolicies
	}
	resolved := resolvePolicies(policies)
	return policy.NewStagedPolicies(cutoverNanos, false, resolved)
}

func (as *activeRuleSet) rollupResultsFor(id []byte, timeNanos int64) []RollupResult {
	// TODO(xichen): pool the rollup targets.
	var (
		cutoverNanos int64
		rollups      []rollupTarget
	)
	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.ActiveSnapshot(timeNanos)
		if snapshot == nil {
			continue
		}
		if !snapshot.filter.Matches(id) {
			continue
		}
		if cutoverNanos < snapshot.cutoverNanos {
			cutoverNanos = snapshot.cutoverNanos
		}
		for _, target := range snapshot.targets {
			found := false
			// If the new target has the same transformation as an existing one,
			// we merge their policies.
			for i := range rollups {
				if rollups[i].sameTransform(target) {
					rollups[i].Policies = append(rollups[i].Policies, target.Policies...)
					found = true
					break
				}
			}
			// Otherwise, we add a new rollup target.
			if !found {
				rollups = append(rollups, target.clone())
			}
		}
	}

	// Resolve the policies for each rollup target.
	if len(rollups) == 0 {
		return nil
	}
	for i := range rollups {
		rollups[i].Policies = resolvePolicies(rollups[i].Policies)
	}

	return as.toRollupResults(id, cutoverNanos, rollups)
}

// toRollupResults encodes rollup target name and values into ids for each rollup target.
func (as *activeRuleSet) toRollupResults(id []byte, cutoverNanos int64, targets []rollupTarget) []RollupResult {
	// NB(r): This is n^2 however this should be quite fast still as
	// long as there is not an absurdly high number of rollup
	// targets for any given ID and that iterfn is alloc free.
	//
	// Even with a very high number of rules its still predicted that
	// any given ID would match a relatively low number of rollups.

	// TODO(xichen): pool tag pairs and rollup results.
	if len(targets) == 0 {
		return nil
	}

	// If we cannot extract tags from the id, this is likely an invalid
	// metric and we bail early.
	_, tags, err := as.tagFilterOpts.NameAndTagsFn(id)
	if err != nil {
		return nil
	}

	var tagPairs []metricID.TagPair
	rollups := make([]RollupResult, 0, len(targets))
	for _, target := range targets {
		tagPairs = tagPairs[:0]

		// NB(xichen): this takes advantage of the fact that the tags in each rollup
		// target is sorted in ascending order.
		var (
			tagIter      = as.tagFilterOpts.SortedTagIteratorFn(tags)
			hasMoreTags  = tagIter.Next()
			targetTagIdx = 0
		)
		for hasMoreTags && targetTagIdx < len(target.Tags) {
			tagName, tagVal := tagIter.Current()
			res := bytes.Compare(tagName, target.Tags[targetTagIdx])
			if res == 0 {
				tagPairs = append(tagPairs, metricID.TagPair{Name: tagName, Value: tagVal})
				targetTagIdx++
				hasMoreTags = tagIter.Next()
				continue
			}
			if res > 0 {
				break
			}
			hasMoreTags = tagIter.Next()
		}
		tagIter.Close()
		// If not all the target tags are found in the id, this is considered
		// an ineligible rollup target. In practice, this should never happen
		// because the UI requires the list of rollup tags should be a subset
		// of the tags in the metric selection filter.
		if targetTagIdx < len(target.Tags) {
			continue
		}

		result := RollupResult{
			ID:           as.newRollupIDFn(target.Name, tagPairs),
			PoliciesList: policy.PoliciesList{policy.NewStagedPolicies(cutoverNanos, false, target.Policies)},
		}
		rollups = append(rollups, result)
	}

	sort.Sort(RollupResultsByIDAsc(rollups))
	return rollups
}

// nextCutoverIdx returns the next snapshot index whose cutover time is after t.
// NB(xichen): not using sort.Search to avoid a lambda capture.
func (as *activeRuleSet) nextCutoverIdx(t int64) int {
	i, j := 0, len(as.cutoverTimesAsc)
	for i < j {
		h := i + (j-i)/2
		if as.cutoverTimesAsc[h] <= t {
			i = h + 1
		} else {
			j = h
		}
	}
	return i
}

// cutoverNanosAt returns the cutover time at given index.
func (as *activeRuleSet) cutoverNanosAt(idx int) int64 {
	if idx < len(as.cutoverTimesAsc) {
		return as.cutoverTimesAsc[idx]
	}
	return timeNanosMax
}

// RuleSet is a set of rules associated with a namespace.
type RuleSet interface {
	// Namespace is the metrics namespace the ruleset applies to.
	Namespace() []byte

	// Version returns the ruleset version.
	Version() int

	// CutoverNanos returns when the ruleset takes effect.
	CutoverNanos() int64

	// TombStoned returns whether the ruleset is tombstoned.
	TombStoned() bool

	// ActiveSet returns the active ruleset at a given time.
	ActiveSet(t time.Time) Matcher
}

type ruleSet struct {
	uuid               string
	version            int
	namespace          []byte
	createdAtNanos     int64
	lastUpdatedAtNanos int64
	tombstoned         bool
	cutoverNanos       int64
	mappingRules       []*mappingRule
	rollupRules        []*rollupRule
	tagsFilterOpts     filters.TagsFilterOptions
	newRollupIDFn      metricID.NewIDFn
	isRollupIDFn       metricID.MatchIDFn
}

// NewRuleSet creates a new ruleset.
func NewRuleSet(version int, rs *schema.RuleSet, opts Options) (RuleSet, error) {
	if rs == nil {
		return nil, errNilRuleSetSchema
	}
	tagsFilterOpts := opts.TagsFilterOptions()
	mappingRules := make([]*mappingRule, 0, len(rs.MappingRules))
	for _, mappingRule := range rs.MappingRules {
		mc, err := newMappingRule(mappingRule, tagsFilterOpts)
		if err != nil {
			return nil, err
		}
		mappingRules = append(mappingRules, mc)
	}
	rollupRules := make([]*rollupRule, 0, len(rs.RollupRules))
	for _, rollupRule := range rs.RollupRules {
		rc, err := newRollupRule(rollupRule, tagsFilterOpts)
		if err != nil {
			return nil, err
		}
		rollupRules = append(rollupRules, rc)
	}
	return &ruleSet{
		uuid:               rs.Uuid,
		version:            version,
		namespace:          []byte(rs.Namespace),
		createdAtNanos:     rs.CreatedAt,
		lastUpdatedAtNanos: rs.LastUpdatedAt,
		tombstoned:         rs.Tombstoned,
		cutoverNanos:       rs.CutoverTime,
		mappingRules:       mappingRules,
		rollupRules:        rollupRules,
		tagsFilterOpts:     tagsFilterOpts,
		newRollupIDFn:      opts.NewRollupIDFn(),
		isRollupIDFn:       opts.IsRollupIDFn(),
	}, nil
}

func (rs *ruleSet) Namespace() []byte   { return rs.namespace }
func (rs *ruleSet) Version() int        { return rs.version }
func (rs *ruleSet) CutoverNanos() int64 { return rs.cutoverNanos }
func (rs *ruleSet) TombStoned() bool    { return rs.tombstoned }

func (rs *ruleSet) ActiveSet(t time.Time) Matcher {
	timeNanos := t.UnixNano()
	mappingRules := make([]*mappingRule, 0, len(rs.mappingRules))
	for _, mappingRule := range rs.mappingRules {
		activeRule := mappingRule.ActiveRule(timeNanos)
		mappingRules = append(mappingRules, activeRule)
	}
	rollupRules := make([]*rollupRule, 0, len(rs.rollupRules))
	for _, rollupRule := range rs.rollupRules {
		activeRule := rollupRule.ActiveRule(timeNanos)
		rollupRules = append(rollupRules, activeRule)
	}
	return newActiveRuleSet(
		mappingRules,
		rollupRules,
		rs.tagsFilterOpts,
		rs.newRollupIDFn,
		rs.isRollupIDFn,
	)
}

// resolvePolicies resolves the conflicts among policies if any, following the rules below:
// * If two policies have the same resolution but different retention, the one with longer
// retention period is chosen.
// * If two policies have the same resolution but different custom aggregation types, the
// aggregation types will be merged.
func resolvePolicies(policies []policy.Policy) []policy.Policy {
	if len(policies) == 0 {
		return policies
	}
	sort.Sort(policy.ByResolutionAsc(policies))
	// curr is the index of the last policy kept so far.
	curr := 0
	for i := 1; i < len(policies); i++ {
		// If the policy has the same resolution, it must have either the same or shorter retention
		// period due to sorting, so we keep the one with longer retention period and ignore this
		// policy.
		if policies[curr].Resolution().Window == policies[i].Resolution().Window {
			if res, merged := policies[curr].AggregationID.Merge(policies[i].AggregationID); merged {
				// Merged custom aggregation functions to the current policy.
				policies[curr] = policy.NewPolicy(policies[curr].StoragePolicy, res)
			}
			continue
		}
		// Now we are guaranteed the policy has lower resolution than the
		// current one, so we want to keep it.
		curr++
		policies[curr] = policies[i]
	}
	return policies[:curr+1]
}

// mergeMappingResults assumes the policies contained in currMappingResults
// are sorted by cutover time in time ascending order.
func mergeMappingResults(
	currMappingResults policy.PoliciesList,
	nextMappingPolicies policy.StagedPolicies,
) policy.PoliciesList {
	currMappingPolicies := currMappingResults[len(currMappingResults)-1]
	if currMappingPolicies.SamePolicies(nextMappingPolicies) {
		return currMappingResults
	}
	currMappingResults = append(currMappingResults, nextMappingPolicies)
	return currMappingResults
}

// mergeRollupResults assumes both currRollupResult and nextRollupResult
// are sorted by the ids of roll up results in ascending order.
func mergeRollupResults(
	currRollupResults []RollupResult,
	nextRollupResults []RollupResult,
	nextCutoverNanos int64,
) []RollupResult {
	var (
		numCurrRollupResults = len(currRollupResults)
		numNextRollupResults = len(nextRollupResults)
		currRollupIdx        int
		nextRollupIdx        int
	)

	for currRollupIdx < numCurrRollupResults && nextRollupIdx < numNextRollupResults {
		currRollupResult := currRollupResults[currRollupIdx]
		nextRollupResult := nextRollupResults[nextRollupIdx]

		// If the current and the next rollup result have the same id, we merge their policies.
		compareResult := bytes.Compare(currRollupResult.ID, nextRollupResult.ID)
		if compareResult == 0 {
			currRollupPolicies := currRollupResult.PoliciesList[len(currRollupResult.PoliciesList)-1]
			nextRollupPolicies := nextRollupResult.PoliciesList[0]
			if !currRollupPolicies.SamePolicies(nextRollupPolicies) {
				currRollupResults[currRollupIdx].PoliciesList = append(currRollupResults[currRollupIdx].PoliciesList, nextRollupPolicies)
			}
			currRollupIdx++
			nextRollupIdx++
			continue
		}

		// If the current id is smaller, it means the id is deleted in the next rollup result.
		if compareResult < 0 {
			currRollupPolicies := currRollupResult.PoliciesList[len(currRollupResult.PoliciesList)-1]
			if !currRollupPolicies.Tombstoned {
				tombstonedPolicies := policy.NewStagedPolicies(nextCutoverNanos, true, nil)
				currRollupResults[currRollupIdx].PoliciesList = append(currRollupResults[currRollupIdx].PoliciesList, tombstonedPolicies)
			}
			currRollupIdx++
			continue
		}

		// Otherwise the current id is larger, meaning a new id is added in the next rollup result.
		currRollupResults = append(currRollupResults, nextRollupResult)
		nextRollupIdx++
	}

	// If there are leftover ids in the current rollup result, these ids must have been deleted
	// in the next rollup result.
	for currRollupIdx < numCurrRollupResults {
		currRollupResult := currRollupResults[currRollupIdx]
		currRollupPolicies := currRollupResult.PoliciesList[len(currRollupResult.PoliciesList)-1]
		if !currRollupPolicies.Tombstoned {
			tombstonedPolicies := policy.NewStagedPolicies(nextCutoverNanos, true, nil)
			currRollupResults[currRollupIdx].PoliciesList = append(currRollupResults[currRollupIdx].PoliciesList, tombstonedPolicies)
		}
		currRollupIdx++
	}

	// If there are additional ids in the next rollup result, these ids must have been added
	// in the next rollup result.
	for nextRollupIdx < numNextRollupResults {
		nextRollupResult := nextRollupResults[nextRollupIdx]
		currRollupResults = append(currRollupResults, nextRollupResult)
		nextRollupIdx++
	}

	sort.Sort(RollupResultsByIDAsc(currRollupResults))
	return currRollupResults
}

type int64Asc []int64

func (a int64Asc) Len() int           { return len(a) }
func (a int64Asc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a int64Asc) Less(i, j int) bool { return a[i] < a[j] }
