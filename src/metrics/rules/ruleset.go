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
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/metric"
	metricID "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"

	"github.com/pborman/uuid"
)

const (
	timeNanosMax = int64(math.MaxInt64)
)

var (
	errNilRuleSetSchema   = errors.New("nil rule set schema")
	errNilValidator       = errors.New("no validator provided")
	errNoSuchRule         = errors.New("no such rule exists")
	errNotTombstoned      = errors.New("not tombstoned")
	errNoRuleSnapshots    = errors.New("no snapshots")
	ruleActionErrorFmt    = "cannot %s rule %s. %v"
	ruleSetActionErrorFmt = "cannot %s ruleset %s. %v"
)

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// ForwardMatch matches the applicable policies for a metric id between [fromNanos, toNanos).
	ForwardMatch(id []byte, fromNanos, toNanos int64) MatchResult

	// ReverseMatch reverse matches the applicable policies for a metric id between [fromNanos, toNanos),
	// with aware of the metric type and aggregation type for the given id.
	ReverseMatch(id []byte, fromNanos, toNanos int64, mt metric.Type, at policy.AggregationType) MatchResult
}

type activeRuleSet struct {
	version         int
	mappingRules    []*mappingRule
	rollupRules     []*rollupRule
	cutoverTimesAsc []int64
	tagFilterOpts   filters.TagsFilterOptions
	newRollupIDFn   metricID.NewIDFn
	isRollupIDFn    metricID.MatchIDFn
	aggTypeOpts     policy.AggregationTypesOptions
}

func newActiveRuleSet(
	version int,
	mappingRules []*mappingRule,
	rollupRules []*rollupRule,
	tagFilterOpts filters.TagsFilterOptions,
	newRollupIDFn metricID.NewIDFn,
	isRollupIDFn metricID.MatchIDFn,
	aggOpts policy.AggregationTypesOptions,
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
		version:         version,
		mappingRules:    mappingRules,
		rollupRules:     rollupRules,
		cutoverTimesAsc: cutoverTimesAsc,
		tagFilterOpts:   tagFilterOpts,
		newRollupIDFn:   newRollupIDFn,
		isRollupIDFn:    isRollupIDFn,
		aggTypeOpts:     aggOpts,
	}
}

// NB(xichen): could make this more efficient by keeping track of matched rules
// at previous iteration and incrementally update match results.
func (as *activeRuleSet) ForwardMatch(
	id []byte,
	fromNanos, toNanos int64,
) MatchResult {
	var (
		nextIdx            = as.nextCutoverIdx(fromNanos)
		nextCutoverNanos   = as.cutoverNanosAt(nextIdx)
		currMappingResults = policy.PoliciesList{as.forwardMappingsForNonRollupID(id, fromNanos)}
		currRollupResults  = as.rollupResultsFor(id, fromNanos)
	)
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		nextMappingPolicies := as.forwardMappingsForNonRollupID(id, nextCutoverNanos)
		currMappingResults = mergeMappingResults(currMappingResults, nextMappingPolicies)
		nextRollupResults := as.rollupResultsFor(id, nextCutoverNanos)
		currRollupResults = mergeRollupResults(currRollupResults, nextRollupResults, nextCutoverNanos)
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}

	// The result expires when it reaches the first cutover time after toNanos among all
	// active rules because the metric may then be matched against a different set of rules.
	return NewMatchResult(as.version, nextCutoverNanos, currMappingResults, currRollupResults)
}

func (as *activeRuleSet) ReverseMatch(
	id []byte,
	fromNanos, toNanos int64,
	mt metric.Type, at policy.AggregationType,
) MatchResult {
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

	if currMappingPolicies, found := as.reverseMappingsFor(id, name, tags, isRollupID, fromNanos, mt, at); found {
		currMappingResults = append(currMappingResults, currMappingPolicies)
	}
	for nextIdx < len(as.cutoverTimesAsc) && nextCutoverNanos < toNanos {
		if nextMappingPolicies, found := as.reverseMappingsFor(id, name, tags, isRollupID, nextCutoverNanos, mt, at); found {
			currMappingResults = mergeMappingResults(currMappingResults, nextMappingPolicies)
		}
		nextIdx++
		nextCutoverNanos = as.cutoverNanosAt(nextIdx)
	}

	// The result expires when it reaches the first cutover time after toNanos among all
	// active rules because the metric may then be matched against a different set of rules.
	return NewMatchResult(as.version, nextCutoverNanos, currMappingResults, nil)
}

func (as *activeRuleSet) reverseMappingsFor(
	id, name, tags []byte,
	isRollupID bool,
	timeNanos int64,
	mt metric.Type,
	at policy.AggregationType,
) (policy.StagedPolicies, bool) {
	if !isRollupID {
		return as.reverseMappingsForNonRollupID(id, timeNanos, mt, at)
	}
	return as.reverseMappingsForRollupID(name, tags, timeNanos, mt, at)
}

// NB(xichen): in order to determine the applicable policies for a rollup metric, we need to
// match the id against rollup rules to determine which rollup rules are applicable, under the
// assumption that no two rollup targets in the same namespace may have the same rollup metric
// name and the list of rollup tags. Otherwise, a rollup metric could potentially match more
// than one rollup rule with different policies even though only one of the matched rules was
// used to produce the given rollup metric id due to its tag filters, thereby causing the wrong
// staged policies to be returned. This also implies at any given time, at most one rollup target
// may match the given rollup id.
// Since we may have policies with different aggregation types defined for a roll up rule,
// and each aggregation type would generate a new id. So when doing reverse mapping, not only do
// we need to match the roll up tags, we also need to check the aggregation type against
// each policy to see if the aggregation type was actually contained in the policy.
func (as *activeRuleSet) reverseMappingsForRollupID(
	name, tags []byte,
	timeNanos int64,
	mt metric.Type, at policy.AggregationType,
) (policy.StagedPolicies, bool) {
	for _, rollupRule := range as.rollupRules {
		snapshot := rollupRule.ActiveSnapshot(timeNanos)
		if snapshot == nil || snapshot.tombstoned {
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
				// Filter the each policy by the aggregation type.
				filtered, _ := filterPoliciesWithAggregationTypes(resolved, mt, at, as.aggTypeOpts)
				if len(filtered) == 0 {
					return policy.DefaultStagedPolicies, false
				}
				return policy.NewStagedPolicies(snapshot.cutoverNanos, false, filtered), true
			}
		}
	}

	return policy.DefaultStagedPolicies, false
}

func (as *activeRuleSet) reverseMappingsForNonRollupID(
	id []byte,
	timeNanos int64,
	mt metric.Type,
	at policy.AggregationType,
) (policy.StagedPolicies, bool) {
	policies, cutoverNanos := as.mappingsForNonRollupID(id, timeNanos)
	// NB(cw) aggregation types filter must be applied after the policy list is resolved.
	// Because policies like [1m:40h|P90,P99; 1m:20h|P50] will be resolved to [1m:40h|P50,P90,P99].
	filtered, isDefault := filterPoliciesWithAggregationTypes(policies, mt, at, as.aggTypeOpts)
	if cutoverNanos == 0 && isDefault {
		return policy.DefaultStagedPolicies, true
	}
	if isDefault || len(filtered) != 0 {
		return policy.NewStagedPolicies(cutoverNanos, false, filtered), true
	}
	return policy.DefaultStagedPolicies, false
}

func (as *activeRuleSet) forwardMappingsForNonRollupID(id []byte, timeNanos int64) policy.StagedPolicies {
	policies, cutoverNanos := as.mappingsForNonRollupID(id, timeNanos)
	if cutoverNanos == 0 && len(policies) == 0 {
		return policy.DefaultStagedPolicies
	}
	return policy.NewStagedPolicies(cutoverNanos, false, policies)
}

// NB(xichen): if the given id is not a rollup id, we need to match it against the mapping
// rules to determine the corresponding mapping policies.
func (as *activeRuleSet) mappingsForNonRollupID(id []byte, timeNanos int64) ([]policy.Policy, int64) {
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
		// If the mapping rule snapshot is a tombstoned snapshot, its cutover time is
		// recorded to indicate a rule change, but its policies are no longer in effect.
		if snapshot.tombstoned {
			continue
		}
		policies = append(policies, snapshot.policies...)
	}
	resolved := resolvePolicies(policies)
	return resolved, cutoverNanos
}

func (as *activeRuleSet) rollupResultsFor(id []byte, timeNanos int64) []RollupResult {
	// TODO(xichen): pool the rollup targets.
	var (
		cutoverNanos int64
		rollups      []RollupTarget
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
		// If the rollup rule snapshot is a tombstoned snapshot, its cutover time is
		// recorded to indicate a rule change, but its rollup targets are no longer in effect.
		if snapshot.tombstoned {
			continue
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
func (as *activeRuleSet) toRollupResults(id []byte, cutoverNanos int64, targets []RollupTarget) []RollupResult {
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

// MappingRules belonging to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type MappingRules map[string][]*MappingRuleView

// RollupRules belonging to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type RollupRules map[string][]*RollupRuleView

// RuleSet is a set of rules associated with a namespace.
type RuleSet interface {
	// Namespace is the metrics namespace the ruleset applies to.
	Namespace() []byte

	// Version returns the ruleset version.
	Version() int

	// CutoverNanos returns when the ruleset takes effect.
	CutoverNanos() int64

	// TombStoned returns whether the ruleset is tombstoned.
	Tombstoned() bool

	// CreatedAtNanos returns the creation time for this ruleset.
	CreatedAtNanos() int64

	// LastUpdatedAtNanos returns the time when this ruleset was last updated.
	LastUpdatedAtNanos() int64

	// ActiveSet returns the active ruleset at a given time.
	ActiveSet(timeNanos int64) Matcher

	// MappingRuleHistory returns a map of mapping rule id to states that rule has been in.
	MappingRules() (MappingRules, error)

	// RollupRuleHistory returns a map of rollup rule id to states that rule has been in.
	RollupRules() (RollupRules, error)

	// Latest returns the latest snapshot of a ruleset containing the latest snapshots
	// of each rule in the ruleset.
	Latest() (*RuleSetSnapshot, error)

	// Validate validates this ruleset.
	Validate(validator Validator) error

	// ToMutableRuleSet returns a mutable version of this ruleset.
	ToMutableRuleSet() MutableRuleSet
}

// MutableRuleSet is an extension of a RuleSet that implements mutation functions.
type MutableRuleSet interface {
	RuleSet

	// Schema returns the schema.Ruleset representation of this ruleset.
	Schema() (*schema.RuleSet, error)

	// Clone returns a copy of this MutableRuleSet.
	Clone() MutableRuleSet

	// AppendMappingRule creates a new mapping rule and adds it to this ruleset.
	// Should return the id of the newly created rule.
	AddMappingRule(MappingRuleView, UpdateMetadata) (string, error)

	// UpdateMappingRule creates a new mapping rule and adds it to this ruleset.
	UpdateMappingRule(MappingRuleView, UpdateMetadata) error

	// DeleteMappingRule deletes a mapping rule
	DeleteMappingRule(string, UpdateMetadata) error

	// AppendRollupRule creates a new rollup rule and adds it to this ruleset.
	// Should return the id of the newly created rule.
	AddRollupRule(RollupRuleView, UpdateMetadata) (string, error)

	// UpdateRollupRule creates a new rollup rule and adds it to this ruleset.
	UpdateRollupRule(RollupRuleView, UpdateMetadata) error

	// DeleteRollupRule deletes a rollup rule
	DeleteRollupRule(string, UpdateMetadata) error

	// Tombstone tombstones this ruleset and all of its rules.
	Delete(UpdateMetadata) error

	// Revive removes the tombstone from this ruleset. It does not revive any rules.
	Revive(UpdateMetadata) error
}

type ruleSet struct {
	uuid               string
	version            int
	namespace          []byte
	createdAtNanos     int64
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
	tombstoned         bool
	cutoverNanos       int64
	mappingRules       []*mappingRule
	rollupRules        []*rollupRule
	tagsFilterOpts     filters.TagsFilterOptions
	newRollupIDFn      metricID.NewIDFn
	isRollupIDFn       metricID.MatchIDFn
	aggOpts            policy.AggregationTypesOptions
}

// NewRuleSetFromSchema creates a new RuleSet from a schema object.
func NewRuleSetFromSchema(version int, rs *schema.RuleSet, opts Options) (RuleSet, error) {
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
		createdAtNanos:     rs.CreatedAtNanos,
		lastUpdatedAtNanos: rs.LastUpdatedAtNanos,
		lastUpdatedBy:      rs.LastUpdatedBy,
		tombstoned:         rs.Tombstoned,
		cutoverNanos:       rs.CutoverNanos,
		mappingRules:       mappingRules,
		rollupRules:        rollupRules,
		tagsFilterOpts:     tagsFilterOpts,
		newRollupIDFn:      opts.NewRollupIDFn(),
		isRollupIDFn:       opts.IsRollupIDFn(),
		aggOpts:            opts.AggregationTypesOptions(),
	}, nil
}

// NewEmptyRuleSet returns an empty ruleset to be used with a new namespace.
func NewEmptyRuleSet(namespaceName string, meta UpdateMetadata) MutableRuleSet {
	rs := &ruleSet{
		uuid:         uuid.NewUUID().String(),
		version:      kv.UninitializedVersion,
		namespace:    []byte(namespaceName),
		tombstoned:   false,
		mappingRules: make([]*mappingRule, 0),
		rollupRules:  make([]*rollupRule, 0),
		aggOpts:      policy.NewAggregationTypesOptions(),
	}
	rs.updateMetadata(meta)
	return rs
}

func (rs *ruleSet) Namespace() []byte         { return rs.namespace }
func (rs *ruleSet) Version() int              { return rs.version }
func (rs *ruleSet) CutoverNanos() int64       { return rs.cutoverNanos }
func (rs *ruleSet) Tombstoned() bool          { return rs.tombstoned }
func (rs *ruleSet) LastUpdatedAtNanos() int64 { return rs.lastUpdatedAtNanos }
func (rs *ruleSet) CreatedAtNanos() int64     { return rs.createdAtNanos }

func (rs *ruleSet) ActiveSet(timeNanos int64) Matcher {
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
		rs.version,
		mappingRules,
		rollupRules,
		rs.tagsFilterOpts,
		rs.newRollupIDFn,
		rs.isRollupIDFn,
		rs.aggOpts,
	)
}

func (rs *ruleSet) ToMutableRuleSet() MutableRuleSet {
	return rs
}

// Schema returns the protobuf representation of a ruleset.
func (rs *ruleSet) Schema() (*schema.RuleSet, error) {
	res := &schema.RuleSet{
		Uuid:               rs.uuid,
		Namespace:          string(rs.namespace),
		CreatedAtNanos:     rs.createdAtNanos,
		LastUpdatedAtNanos: rs.lastUpdatedAtNanos,
		LastUpdatedBy:      rs.lastUpdatedBy,
		Tombstoned:         rs.tombstoned,
		CutoverNanos:       rs.cutoverNanos,
	}

	mappingRules := make([]*schema.MappingRule, len(rs.mappingRules))
	for i, m := range rs.mappingRules {
		mr, err := m.Schema()
		if err != nil {
			return nil, err
		}
		mappingRules[i] = mr
	}
	res.MappingRules = mappingRules

	rollupRules := make([]*schema.RollupRule, len(rs.rollupRules))
	for i, r := range rs.rollupRules {
		rr, err := r.Schema()
		if err != nil {
			return nil, err
		}
		rollupRules[i] = rr
	}
	res.RollupRules = rollupRules

	return res, nil
}

func (rs *ruleSet) MappingRules() (MappingRules, error) {
	mappingRules := make(MappingRules, len(rs.mappingRules))
	for _, m := range rs.mappingRules {
		hist, err := m.history()
		if err != nil {
			return nil, err
		}
		mappingRules[m.uuid] = hist
	}
	return mappingRules, nil
}

func (rs *ruleSet) RollupRules() (RollupRules, error) {
	rollupRules := make(RollupRules, len(rs.rollupRules))
	for _, r := range rs.rollupRules {
		hist, err := r.history()
		if err != nil {
			return nil, err
		}
		rollupRules[r.uuid] = hist
	}
	return rollupRules, nil
}

func (rs *ruleSet) Latest() (*RuleSetSnapshot, error) {
	mrs, err := rs.latestMappingRules()
	if err != nil {
		return nil, err
	}
	rrs, err := rs.latestRollupRules()
	if err != nil {
		return nil, err
	}
	return &RuleSetSnapshot{
		Namespace:    string(rs.Namespace()),
		Version:      rs.Version(),
		CutoverNanos: rs.CutoverNanos(),
		MappingRules: mrs,
		RollupRules:  rrs,
	}, nil
}

func (rs *ruleSet) Validate(validator Validator) error {
	if validator == nil {
		return errNilValidator
	}
	return validator.Validate(rs)
}

func (rs *ruleSet) Clone() MutableRuleSet {
	namespace := make([]byte, len(rs.namespace))
	copy(namespace, rs.namespace)

	mappingRules := make([]*mappingRule, len(rs.mappingRules))
	for i, m := range rs.mappingRules {
		c := m.clone()
		mappingRules[i] = &c
	}

	rollupRules := make([]*rollupRule, len(rs.rollupRules))
	for i, r := range rs.rollupRules {
		c := r.clone()
		rollupRules[i] = &c
	}

	// This clone deliberately ignores tagFliterOpts and rollupIDFn
	// as they are not useful for the MutableRuleSet.
	return MutableRuleSet(&ruleSet{
		uuid:               rs.uuid,
		version:            rs.version,
		createdAtNanos:     rs.createdAtNanos,
		lastUpdatedAtNanos: rs.lastUpdatedAtNanos,
		lastUpdatedBy:      rs.lastUpdatedBy,
		tombstoned:         rs.tombstoned,
		cutoverNanos:       rs.cutoverNanos,
		namespace:          namespace,
		mappingRules:       mappingRules,
		rollupRules:        rollupRules,
		tagsFilterOpts:     rs.tagsFilterOpts,
		newRollupIDFn:      rs.newRollupIDFn,
		isRollupIDFn:       rs.isRollupIDFn,
		aggOpts:            rs.aggOpts,
	})
}

func (rs *ruleSet) AddMappingRule(mrv MappingRuleView, meta UpdateMetadata) (string, error) {
	m, err := rs.getMappingRuleByName(mrv.Name)
	if err != nil && err != errNoSuchRule {
		return "", fmt.Errorf(ruleActionErrorFmt, "add", mrv.Name, err)
	}
	if err == errNoSuchRule {
		if m, err = newMappingRuleFromFields(
			mrv.Name,
			mrv.Filter,
			mrv.Policies,
			meta,
		); err != nil {
			return "", fmt.Errorf(ruleActionErrorFmt, "add", mrv.Name, err)
		}
		rs.mappingRules = append(rs.mappingRules, m)
	} else {
		if err := m.revive(
			mrv.Name,
			mrv.Filter,
			mrv.Policies,
			meta,
		); err != nil {
			return "", fmt.Errorf(ruleActionErrorFmt, "revive", mrv.Name, err)
		}
	}
	rs.updateMetadata(meta)
	return m.uuid, nil
}

func (rs *ruleSet) UpdateMappingRule(mrv MappingRuleView, meta UpdateMetadata) error {
	m, err := rs.getMappingRuleByID(mrv.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mrv.ID, err)
	}
	if err := m.addSnapshot(
		mrv.Name,
		mrv.Filter,
		mrv.Policies,
		meta,
	); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mrv.Name, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteMappingRule(id string, meta UpdateMetadata) error {
	m, err := rs.getMappingRuleByID(id)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", id, err)
	}
	if err := m.markTombstoned(meta.cutoverNanos); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", id, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) AddRollupRule(rrv RollupRuleView, meta UpdateMetadata) (string, error) {
	r, err := rs.getRollupRuleByName(rrv.Name)
	if err != nil && err != errNoSuchRule {
		return "", fmt.Errorf(ruleActionErrorFmt, "add", rrv.Name, err)
	}
	targets := rollupTargetViewsToTargets(rrv.Targets)
	if err == errNoSuchRule {
		if r, err = newRollupRuleFromFields(
			rrv.Name,
			rrv.Filter,
			targets,
			meta,
		); err != nil {
			return "", fmt.Errorf(ruleActionErrorFmt, "add", rrv.Name, err)
		}
		rs.rollupRules = append(rs.rollupRules, r)
	} else {
		if err := r.revive(
			rrv.Name,
			rrv.Filter,
			targets,
			meta,
		); err != nil {
			return "", fmt.Errorf(ruleActionErrorFmt, "revive", rrv.Name, err)
		}
	}
	rs.updateMetadata(meta)
	return r.uuid, nil
}

func (rs *ruleSet) UpdateRollupRule(rrv RollupRuleView, meta UpdateMetadata) error {
	r, err := rs.getRollupRuleByID(rrv.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rrv.ID, err)
	}
	targets := rollupTargetViewsToTargets(rrv.Targets)
	if err = r.addSnapshot(
		rrv.Name,
		rrv.Filter,
		targets,
		meta,
	); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rrv.Name, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteRollupRule(id string, meta UpdateMetadata) error {
	r, err := rs.getRollupRuleByID(id)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", id, err)
	}
	if err := r.markTombstoned(meta.cutoverNanos); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", id, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) Delete(meta UpdateMetadata) error {
	if rs.tombstoned {
		return fmt.Errorf("%s is already tombstoned", string(rs.namespace))
	}

	rs.tombstoned = true
	rs.updateMetadata(meta)

	// Make sure that all of the rules in the ruleset are tombstoned as well.
	for _, m := range rs.mappingRules {
		if t := m.Tombstoned(); !t {
			_ = m.markTombstoned(meta.cutoverNanos)
		}
	}

	for _, r := range rs.rollupRules {
		if t := r.Tombstoned(); !t {
			_ = r.markTombstoned(meta.cutoverNanos)
		}
	}

	return nil
}

func (rs *ruleSet) Revive(meta UpdateMetadata) error {
	if !rs.Tombstoned() {
		return fmt.Errorf(ruleSetActionErrorFmt, "revive", string(rs.namespace), errNotTombstoned)
	}

	rs.tombstoned = false
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) updateMetadata(meta UpdateMetadata) {
	rs.cutoverNanos = meta.cutoverNanos
	rs.lastUpdatedAtNanos = meta.updatedAtNanos
	rs.lastUpdatedBy = meta.updatedBy
}

func (rs *ruleSet) getMappingRuleByName(name string) (*mappingRule, error) {
	for _, m := range rs.mappingRules {
		n, err := m.Name()
		if err != nil {
			continue
		}

		if n == name {
			return m, nil
		}
	}
	return nil, errNoSuchRule
}

func (rs *ruleSet) getMappingRuleByID(id string) (*mappingRule, error) {
	for _, m := range rs.mappingRules {
		if m.uuid == id {
			return m, nil
		}
	}
	return nil, errNoSuchRule
}

func (rs *ruleSet) getRollupRuleByName(name string) (*rollupRule, error) {
	for _, r := range rs.rollupRules {
		n, err := r.Name()
		if err != nil {
			return nil, err
		}

		if n == name {
			return r, nil
		}
	}
	return nil, errNoSuchRule
}

func (rs *ruleSet) getRollupRuleByID(id string) (*rollupRule, error) {
	for _, r := range rs.rollupRules {
		if r.uuid == id {
			return r, nil
		}
	}
	return nil, errNoSuchRule
}

func (rs *ruleSet) latestMappingRules() (map[string]*MappingRuleView, error) {
	mrs, err := rs.MappingRules()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*MappingRuleView, len(mrs))
	for id, m := range mrs {
		if len(m) > 0 && !m[0].Tombstoned {
			// views included in m are sorted latest first.
			result[id] = m[0]
		}
	}
	return result, nil
}

func (rs *ruleSet) latestRollupRules() (map[string]*RollupRuleView, error) {
	rrs, err := rs.RollupRules()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*RollupRuleView, len(rrs))
	for id, r := range rrs {
		if len(r) > 0 && !r[0].Tombstoned {
			// views included in m are sorted latest first.
			result[id] = r[0]
		}
	}
	return result, nil
}

// resolvePolicies resolves the conflicts among policies if any, following the rules below:
// * Duplicate policies are skipped.
func resolvePolicies(policies []policy.Policy) []policy.Policy {
	if len(policies) == 0 {
		return policies
	}
	sort.Sort(policy.ByResolutionAscRetentionDesc(policies))
	curr := 0
	for i := 1; i < len(policies); i++ {
		if policies[curr] == policies[i] {
			continue
		}
		curr++
		policies[curr] = policies[i]
	}
	return policies[:curr+1]
}

// filterPoliciesWithAggregationTypes accepts a policy when:
// * If the policy contains default aggregation types and the given aggregation type
//   is contained in the default aggregation types for the metric type.
// * If the policy contains custom aggregation types and the given aggregation type
//   is contained in the custom aggregation type.
func filterPoliciesWithAggregationTypes(ps []policy.Policy, mt metric.Type, at policy.AggregationType, opts policy.AggregationTypesOptions) ([]policy.Policy, bool) {
	if policy.IsDefaultPolicies(ps) {
		return nil, opts.IsContainedInDefaultAggregationTypes(at, mt)
	}

	var cur int
	for i := 0; i < len(ps); i++ {
		if !containsAggType(ps[i].AggregationID, mt, at, opts) {
			continue
		}
		// NB: The policy does not match the aggregation type and should be removed,
		// but we need to maintain the order of the policy list.
		if cur != i {
			ps[cur] = ps[i]
		}
		cur++
	}
	return ps[:cur], false
}

func containsAggType(aggID policy.AggregationID, mt metric.Type, at policy.AggregationType, opts policy.AggregationTypesOptions) bool {
	if aggID.IsDefault() {
		return opts.IsContainedInDefaultAggregationTypes(at, mt)
	}
	return aggID.Contains(at)
}

// mergeMappingResults assumes the policies contained in currMappingResults
// are sorted by cutover time in time ascending order.
func mergeMappingResults(
	currMappingResults policy.PoliciesList,
	nextMappingPolicies policy.StagedPolicies,
) policy.PoliciesList {
	if len(currMappingResults) == 0 {
		return policy.PoliciesList{nextMappingPolicies}
	}
	currMappingPolicies := currMappingResults[len(currMappingResults)-1]
	if currMappingPolicies.Equals(nextMappingPolicies) {
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
			if !currRollupPolicies.Equals(nextRollupPolicies) {
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

// RuleSetView is a view into the current state of the ruleset.

// RuleSetUpdateHelper stores the necessary details to create an UpdateMetadata.
type RuleSetUpdateHelper struct {
	propagationDelay time.Duration
}

// NewRuleSetUpdateHelper creates a new RuleSetUpdateHelper struct.
func NewRuleSetUpdateHelper(propagationDelay time.Duration) RuleSetUpdateHelper {
	return RuleSetUpdateHelper{propagationDelay: propagationDelay}
}

// UpdateMetadata contains descriptive information that needs to be updated
// with any modification of the ruleset.
type UpdateMetadata struct {
	cutoverNanos   int64
	updatedAtNanos int64
	updatedBy      string
}

// NewUpdateMetadata creates a properly initialized UpdateMetadata object.
func (r RuleSetUpdateHelper) NewUpdateMetadata(updateTime int64, updatedBy string) UpdateMetadata {
	cutoverNanos := updateTime + int64(r.propagationDelay)
	return UpdateMetadata{updatedAtNanos: updateTime, cutoverNanos: cutoverNanos, updatedBy: updatedBy}
}

// RuleSetSnapshot represents a snapshot of a rule set containing snapshots of rules
// in the ruleset.
type RuleSetSnapshot struct {
	Namespace    string
	Version      int
	CutoverNanos int64
	MappingRules map[string]*MappingRuleView
	RollupRules  map[string]*RollupRuleView
}
