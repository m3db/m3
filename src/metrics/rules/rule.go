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
	Name     string          // name of the rollup metric
	Tags     []string        // a set of sorted tags rollups are performed on
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
		Name:     target.Name,
		Tags:     tags,
		Policies: policies,
	}, nil
}

// sameTransform determines whether two targets have the same transformation
func (t RollupTarget) sameTransform(other RollupTarget) bool {
	if t.Name != other.Name {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if t.Tags[i] != other.Tags[i] {
			return false
		}
	}
	return true
}

// clone clones a rollup target
func (t RollupTarget) clone() RollupTarget {
	Tags := make([]string, len(t.Tags))
	copy(Tags, t.Tags)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return RollupTarget{
		Name:     t.Name,
		Tags:     Tags,
		Policies: policies,
	}
}

var defaultMatchResult MatchResult

// MatchResult contains the list of mapping policies and rollup results applicable to a metric
type MatchResult struct {
	Mappings []policy.Policy
	Rollups  []RollupTarget
}

// HasPolicies returns whether the match result has matching policies
func (r MatchResult) HasPolicies() bool { return len(r.Mappings) > 0 }

// RuleSet is a set of rules associated with a namespace
type RuleSet interface {
	// Namespace is the metrics namespace the ruleset applies to
	Namespace() string

	// Version returns the ruleset version
	Version() int

	// Cutover returns when the ruleset takes effect
	Cutover() time.Time

	// Match matches the set of rules against a metric id, returning
	// the applicable mapping policies and rollup policies
	Match(id string) MatchResult
}

// mappingRule defines a rule such that if a metric matches the provided filters,
// it is aggregated and retained under the provided set of policies
type mappingRule struct {
	filter   filters.IDFilter // used to select matching metrics
	policies []policy.Policy  // defines how the metrics should be aggregated and retained
}

func newMappingRule(r *schema.MappingRule, iterfn filters.NewSortedTagIteratorFn) (mappingRule, error) {
	policies, err := policy.NewPoliciesFromSchema(r.Policies)
	if err != nil {
		return emptyMappingRule, err
	}
	return mappingRule{
		filter:   filters.NewTagsFilter(r.TagFilters, iterfn),
		policies: policies,
	}, nil
}

// rollupRule defines a rule such that if a metric matches the provided filters,
// it is rolled up using the provided list of rollup targets
type rollupRule struct {
	filter  filters.IDFilter // used to select matching metrics
	targets []RollupTarget   // dictates how metrics should be rolled up
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
	return rollupRule{
		filter:  filters.NewTagsFilter(r.TagFilters, iterfn),
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
	mappingRules  []mappingRule
	rollupRules   []rollupRule
}

// NewRuleSet creates a new ruleset
func NewRuleSet(rs *schema.RuleSet, iterFn filters.NewSortedTagIteratorFn) (RuleSet, error) {
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
		cutover:       time.Unix(0, rs.Cutover),
		mappingRules:  mappingRules,
		rollupRules:   rollupRules,
	}, nil
}

func (rs *ruleSet) Namespace() string  { return rs.namespace }
func (rs *ruleSet) Version() int       { return rs.version }
func (rs *ruleSet) Cutover() time.Time { return rs.cutover }

func (rs *ruleSet) Match(id string) MatchResult {
	if rs.tombStoned {
		return defaultMatchResult
	}
	return MatchResult{
		Mappings: rs.mappingPolicies(id),
		Rollups:  rs.rollupTargets(id),
	}
}

func (rs *ruleSet) mappingPolicies(id string) []policy.Policy {
	var policies []policy.Policy
	for _, rule := range rs.mappingRules {
		if rule.filter.Matches(id) {
			policies = append(policies, rule.policies...)
		}
	}
	return resolvePolicies(policies)
}

func (rs *ruleSet) rollupTargets(id string) []RollupTarget {
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
