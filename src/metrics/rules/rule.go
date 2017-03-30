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
	"sort"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	emptyRollupTarget RollupTarget
	emptyMappingRule  mappingRule
	emptyRollupRule   rollupRule
)

// RollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies
type RollupTarget struct {
	Name     []byte          // name of the rollup metric
	Tags     [][]byte        // a set of sorted tags rollups are performed on
	Policies []policy.Policy // defines how the rollup metric is aggregated and retained
}

func newRollupTarget(target *schema.RollupTarget) (RollupTarget, error) {
	policies, err := policy.NewPoliciesFromSchema(target.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}

	tags := make([]string, len(target.Tags))
	copy(tags, target.Tags)
	sort.Strings(tags)

	return RollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

// sameTransform determines whether two targets have the same transformation
func (t RollupTarget) sameTransform(other RollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if !bytes.Equal(t.Tags[i], other.Tags[i]) {
			return false
		}
	}
	return true
}

// clone clones a rollup target
func (t RollupTarget) clone() RollupTarget {
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return RollupTarget{
		Name:     t.Name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

func bytesArrayFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

func bytesArrayCopy(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, b := range values {
		result[i] = make([]byte, len(b))
		copy(result[i], b)
	}
	return result
}

// TagPair contains a tag name and a tag value
type TagPair struct {
	Name  []byte
	Value []byte
}

// RollupResult contains the rollup metric id and the associated policies
type RollupResult struct {
	ID       []byte
	Policies []policy.Policy
}

// MatchResult represents a match result
type MatchResult struct {
	version  int
	cutover  time.Time
	mappings []policy.Policy
	rollups  []RollupResult
}

// NewMatchResult creates a new match result
func NewMatchResult(version int, cutover time.Time, mappings []policy.Policy, rollups []RollupResult) MatchResult {
	return MatchResult{
		version:  version,
		cutover:  cutover,
		mappings: mappings,
		rollups:  rollups,
	}
}

// Version returns the version of the match result
func (r *MatchResult) Version() int { return r.version }

// Cutover returns the cutover time of the match result
func (r *MatchResult) Cutover() time.Time { return r.cutover }

// NumRollups returns the number of rollup result associated with the given id
func (r *MatchResult) NumRollups() int { return len(r.rollups) }

// Mappings returns the mapping policies for the given id
func (r *MatchResult) Mappings() policy.VersionedPolicies {
	return r.versionedPolicies(r.mappings)
}

// Rollups returns the rollup metric id and corresponding policies at a given index
func (r *MatchResult) Rollups(idx int) ([]byte, policy.VersionedPolicies) {
	rollup := r.rollups[idx]
	return rollup.ID, r.versionedPolicies(rollup.Policies)
}

func (r *MatchResult) versionedPolicies(policies []policy.Policy) policy.VersionedPolicies {
	// NB(xichen): if there are no policies for this id, we fall
	// back to the default mapping policies
	if len(policies) == 0 {
		return policy.DefaultVersionedPolicies(r.version, r.cutover)
	}
	return policy.CustomVersionedPolicies(r.version, r.cutover, policies)
}

// RuleSet is a set of rules associated with a namespace
type RuleSet interface {
	// Namespace is the metrics namespace the ruleset applies to
	Namespace() string

	// Version returns the ruleset version
	Version() int

	// Cutover returns when the ruleset takes effect
	Cutover() time.Time

	// TombStoned returns whether the ruleset is tombstoned
	TombStoned() bool

	// Match matches the set of rules against a metric id, returning
	// the applicable mapping policies and rollup policies
	Match(id []byte) MatchResult
}

// mappingRule defines a rule such that if a metric matches the provided filters,
// it is aggregated and retained under the provided set of policies
type mappingRule struct {
	filter   filters.Filter  // used to select matching metrics
	policies []policy.Policy // defines how the metrics should be aggregated and retained
}

func newMappingRule(r *schema.MappingRule, iterfn filters.NewSortedTagIteratorFn) (mappingRule, error) {
	policies, err := policy.NewPoliciesFromSchema(r.Policies)
	if err != nil {
		return emptyMappingRule, err
	}

	filter, err := filters.NewTagsFilter(r.TagFilters, iterfn, filters.Conjunction)
	if err != nil {
		return emptyMappingRule, err
	}

	return mappingRule{
		filter:   filter,
		policies: policies,
	}, nil
}

// rollupRule defines a rule such that if a metric matches the provided filters,
// it is rolled up using the provided list of rollup targets
type rollupRule struct {
	filter  filters.Filter // used to select matching metrics
	targets []RollupTarget // dictates how metrics should be rolled up
}

func newRollupRule(r *schema.RollupRule, iterfn filters.NewSortedTagIteratorFn) (rollupRule, error) {
	targets := make([]RollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return emptyRollupRule, err
		}
		targets = append(targets, target)
	}

	filter, err := filters.NewTagsFilter(r.TagFilters, iterfn, filters.Conjunction)
	if err != nil {
		return emptyRollupRule, err
	}

	return rollupRule{
		filter:  filter,
		targets: targets,
	}, nil
}

type ruleSet struct {
	namespace     string
	createdAt     time.Time
	lastUpdatedAt time.Time
	tombStoned    bool
	version       int
	cutover       time.Time
	iterFn        filters.NewSortedTagIteratorFn
	newIDFn       NewIDFn
	mappingRules  []mappingRule
	rollupRules   []rollupRule
}

// NewRuleSet creates a new ruleset
func NewRuleSet(rs *schema.RuleSet, opts Options) (RuleSet, error) {
	iterFn := opts.NewSortedTagIteratorFn()
	mappingRules := make([]mappingRule, 0, len(rs.MappingRules))
	for _, rule := range rs.MappingRules {
		mrule, err := newMappingRule(rule, iterFn)
		if err != nil {
			return nil, err
		}
		mappingRules = append(mappingRules, mrule)
	}

	rollupRules := make([]rollupRule, 0, len(rs.RollupRules))
	for _, rule := range rs.RollupRules {
		rrule, err := newRollupRule(rule, iterFn)
		if err != nil {
			return nil, err
		}
		rollupRules = append(rollupRules, rrule)
	}

	return &ruleSet{
		namespace:     rs.Namespace,
		createdAt:     time.Unix(0, rs.CreatedAt),
		lastUpdatedAt: time.Unix(0, rs.LastUpdatedAt),
		tombStoned:    rs.Tombstoned,
		version:       int(rs.Version),
		cutover:       time.Unix(0, rs.CutoverTime),
		iterFn:        iterFn,
		newIDFn:       opts.NewIDFn(),
		mappingRules:  mappingRules,
		rollupRules:   rollupRules,
	}, nil
}

func (rs *ruleSet) Namespace() string  { return rs.namespace }
func (rs *ruleSet) Version() int       { return rs.version }
func (rs *ruleSet) Cutover() time.Time { return rs.cutover }
func (rs *ruleSet) TombStoned() bool   { return rs.tombStoned }

func (rs *ruleSet) Match(id []byte) MatchResult {
	var (
		mappings []policy.Policy
		rollups  []RollupResult
	)
	if !rs.tombStoned {
		mappings = rs.mappingPolicies(id)
		rollups = rs.rollupResults(id)
	}
	return NewMatchResult(rs.version, rs.cutover, mappings, rollups)
}

func (rs *ruleSet) mappingPolicies(id []byte) []policy.Policy {
	// TODO(xichen): pool the policies
	var policies []policy.Policy
	for _, rule := range rs.mappingRules {
		if rule.filter.Matches(id) {
			policies = append(policies, rule.policies...)
		}
	}
	return resolvePolicies(policies)
}

func (rs *ruleSet) rollupResults(id []byte) []RollupResult {
	// TODO(xichen): pool the rollup targets
	var rollups []RollupTarget
	for _, rule := range rs.rollupRules {
		if !rule.filter.Matches(id) {
			continue
		}
		for _, new := range rule.targets {
			found := false
			// If the new target has the same transformation as an existing one,
			// we merge their policies
			for i := range rollups {
				if rollups[i].sameTransform(new) {
					rollups[i].Policies = append(rollups[i].Policies, new.Policies...)
					found = true
					break
				}
			}
			// Otherwise, we add a new rollup target
			if !found {
				rollups = append(rollups, new.clone())
			}
		}
	}

	// Resolve the policies for each rollup target
	for i := range rollups {
		rollups[i].Policies = resolvePolicies(rollups[i].Policies)
	}

	return rs.toRollupResults(id, rollups)
}

// toRollupResults encodes rollup target name and values into ids for each rollup target
func (rs *ruleSet) toRollupResults(id []byte, targets []RollupTarget) []RollupResult {
	// NB(r): This is n^2 however this should be quite fast still as
	// long as there is not an absurdly high number of rollup
	// targets for any given ID and that iterfn is alloc free.
	//
	// Even with a very high number of rules its still predicted that
	// any given ID would match a relatively low number of rollups.

	// TODO(xichen): pool tag pairs and rollup results
	var tagPairs []TagPair
	rollups := make([]RollupResult, 0, len(targets))
	for _, target := range targets {
		tagPairs = tagPairs[:0]
		for _, tag := range target.Tags {
			iter := rs.iterFn(id)
			for iter.Next() {
				name, value := iter.Current()
				if bytes.Equal(name, tag) {
					tagPairs = append(tagPairs, TagPair{Name: tag, Value: value})
					break
				}
			}
			iter.Close()
		}
		result := RollupResult{
			ID:       rs.newIDFn(target.Name, tagPairs),
			Policies: target.Policies,
		}
		rollups = append(rollups, result)
	}

	return rollups
}

// resolvePolicies resolves the conflicts among policies if any, following the rules below:
// * If two policies have the same resolution but different retention, the one with longer
// retention period is chosen
// * If two policies have the same retention but different resolution, the policy with higher
// resolution is chosen
// * If a policy has lower resolution and shorter retention than another policy, the policy
// is superseded by the other policy and therefore ignored
func resolvePolicies(policies []policy.Policy) []policy.Policy {
	if len(policies) == 0 {
		return policies
	}
	sort.Sort(policy.ByResolutionAsc(policies))
	// curr is the index of the last policy kept so far
	curr := 0
	for i := 1; i < len(policies); i++ {
		// If the policy has the same resolution, it must have either the same or shorter retention
		// period due to sorting, so we keep the one with longer retention period and ignore this
		// policy
		if policies[curr].Resolution().Window == policies[i].Resolution().Window {
			continue
		}
		// Otherwise the policy has lower resolution, so if it has shorter or the same retention
		// period, we keep the one with higher resolution and longer retention period and ignore
		// this policy
		if policies[curr].Retention() >= policies[i].Retention() {
			continue
		}
		// Now we are guaranteed the policy has lower resolution and higher retention than the
		// current one, so we want to keep it
		curr++
		policies[curr] = policies[i]
	}
	return policies[:curr+1]
}
