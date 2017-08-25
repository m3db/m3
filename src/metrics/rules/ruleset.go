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
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	metricID "github.com/m3db/m3metrics/metric/id"
	"github.com/m3db/m3metrics/policy"

	"github.com/pborman/uuid"
)

const (
	timeNanosMax = int64(math.MaxInt64)
)

var (
	errNilRuleSetSchema   = errors.New("nil rule set schema")
	errNoSuchRule         = errors.New("no such rule exists")
	errNotTombstoned      = errors.New("not tombstoned")
	errNoRuleSnapshots    = errors.New("no snapshots")
	ruleActionErrorFmt    = "cannot %s rule %s. %v"
	ruleSetActionErrorFmt = "cannot %s ruleset %s. %v"
)

// MatchMode determines how match is performed.
type MatchMode string

// List of supported match modes.
const (
	// When performing matches in ForwardMatch mode, the matcher matches the given id against
	// both the mapping rules and rollup rules to find out the applicable mapping policies
	// and rollup policies.
	ForwardMatch MatchMode = "forward"

	// When performing matches in ReverseMatch mode, the matcher find the applicable mapping
	// policies for the given id.
	ReverseMatch MatchMode = "reverse"
)

// UnmarshalYAML unmarshals match mode from a string.
func (m *MatchMode) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var str string
	if err := unmarshal(&str); err != nil {
		return err
	}

	parsed, err := parseMatchMode(str)
	if err != nil {
		return err
	}

	*m = parsed
	return nil
}

func parseMatchMode(value string) (MatchMode, error) {
	mode := MatchMode(value)
	switch mode {
	case ForwardMatch, ReverseMatch:
		return mode, nil
	default:
		return mode, fmt.Errorf("unknown match mode: %s", value)
	}
}

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// MatchAll returns the applicable policies for a metric id between [fromNanos, toNanos).
	MatchAll(id []byte, fromNanos, toNanos int64, matchMode MatchMode) MatchResult
}

type activeRuleSet struct {
	version         int
	mappingRules    []*mappingRule
	rollupRules     []*rollupRule
	cutoverTimesAsc []int64
	tagFilterOpts   filters.TagsFilterOptions
	newRollupIDFn   metricID.NewIDFn
	isRollupIDFn    metricID.MatchIDFn
}

func newActiveRuleSet(
	version int,
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
		version:         version,
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
	if matchMode == ForwardMatch {
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
	return NewMatchResult(as.version, nextCutoverNanos, currMappingResults, currRollupResults)
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
	return NewMatchResult(as.version, nextCutoverNanos, currMappingResults, nil)
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

	// ActiveSet returns the active ruleset at a given time.
	ActiveSet(timeNanos int64) Matcher

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

	// MarshalJSON serializes this RuleSet into JSON.
	MarshalJSON() ([]byte, error)

	// UnmarshalJSON deserializes this RuleSet from JSON.
	UnmarshalJSON(data []byte) error

	// AppendMappingRule creates a new mapping rule and adds it to this ruleset.
	AddMappingRule(MappingRuleData) error

	// UpdateMappingRule creates a new mapping rule and adds it to this ruleset.
	UpdateMappingRule(MappingRuleUpdate) error

	// DeleteMappingRule deletes a mapping rule
	DeleteMappingRule(DeleteData) error

	// AppendRollupRule creates a new rollup rule and adds it to this ruleset.
	AddRollupRule(RollupRuleData) error

	// UpdateRollupRule creates a new rollup rule and adds it to this ruleset.
	UpdateRollupRule(RollupRuleUpdate) error

	// DeleteRollupRule deletes a rollup rule
	DeleteRollupRule(DeleteData) error

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
	tombstoned         bool
	cutoverNanos       int64
	mappingRules       []*mappingRule
	rollupRules        []*rollupRule
	tagsFilterOpts     filters.TagsFilterOptions
	newRollupIDFn      metricID.NewIDFn
	isRollupIDFn       metricID.MatchIDFn
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

// NewEmptyRuleSet returns an empty ruleset to be used with a new namespace.
func NewEmptyRuleSet(namespaceName string, meta UpdateMetadata) MutableRuleSet {
	return &ruleSet{
		uuid:               uuid.NewUUID().String(),
		version:            1,
		namespace:          []byte(namespaceName),
		createdAtNanos:     meta.lastUpdatedAtNanos,
		lastUpdatedAtNanos: meta.lastUpdatedAtNanos,
		cutoverNanos:       meta.cutoverNanos,
		tombstoned:         false,
		mappingRules:       make([]*mappingRule, 0),
		rollupRules:        make([]*rollupRule, 0),
	}
}

func (rs *ruleSet) Namespace() []byte   { return rs.namespace }
func (rs *ruleSet) Version() int        { return rs.version }
func (rs *ruleSet) CutoverNanos() int64 { return rs.cutoverNanos }
func (rs *ruleSet) Tombstoned() bool    { return rs.tombstoned }

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
	)
}

func (rs *ruleSet) ToMutableRuleSet() MutableRuleSet {
	return rs
}

// Schema returns the protobuf representation fo a ruleset
func (rs *ruleSet) Schema() (*schema.RuleSet, error) {
	res := &schema.RuleSet{
		Uuid:          rs.uuid,
		Namespace:     string(rs.namespace),
		CreatedAt:     rs.createdAtNanos,
		LastUpdatedAt: rs.lastUpdatedAtNanos,
		Tombstoned:    rs.tombstoned,
		CutoverTime:   rs.cutoverNanos,
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

	// this clone deliberately ignores tagFliterOpts and rollupIDFn
	// as they are not useful for the MutableRuleSet.
	return MutableRuleSet(&ruleSet{
		uuid:               rs.uuid,
		version:            rs.version,
		createdAtNanos:     rs.createdAtNanos,
		lastUpdatedAtNanos: rs.lastUpdatedAtNanos,
		tombstoned:         rs.tombstoned,
		cutoverNanos:       rs.cutoverNanos,
		namespace:          namespace,
		mappingRules:       mappingRules,
		rollupRules:        rollupRules,
		tagsFilterOpts:     rs.tagsFilterOpts,
		newRollupIDFn:      rs.newRollupIDFn,
		isRollupIDFn:       rs.isRollupIDFn,
	})
}

// UpdateMetadata contains fields that should be set with each
// update to the state of a ruleset.
type UpdateMetadata struct {
	cutoverNanos       int64
	lastUpdatedAtNanos int64
}

// MappingRuleData is the parts of a mapping rule that an end user would care about.
// This struct is provided to create or make updates to the mapping rule.
type MappingRuleData struct {
	UpdateMetadata

	Name     string            `json:"name" validate:"nonzero"`
	Filters  map[string]string `json:"filters" validate:"nonzero"`
	Policies []policy.Policy   `json:"policies" validate:"nonzero"`
}

// MappingRuleUpdate contains a MappingRuleData along with an ID for a mapping rule.
// The rule with that ID is meant to updated to the given config.
type MappingRuleUpdate struct {
	Data MappingRuleData `json:"config" validate:"nonzero"`
	ID   string          `json:"id" validate:"nonzero"`
}

// RollupRuleData is the parts of a rollup rule that an end user would care about.
// This struct is provided to create or make updates to the rollup rule.
type RollupRuleData struct {
	UpdateMetadata

	Name    string            `json:"name" validate:"nonzero"`
	Filters map[string]string `json:"filters" validate:"nonzero"`
	Targets []RollupTarget    `json:"targets" validate:"nonzero"`
}

// RollupRuleUpdate is a RollupRuleConfig along with an ID for a rollup rule.
// The rule with that ID is meant to updated with a given config.
type RollupRuleUpdate struct {
	Data RollupRuleData `json:"data" validate:"nonzero"`
	ID   string         `json:"id" validate:"nonzero"`
}

// DeleteData contains data necessary to delete a rule.
type DeleteData struct {
	UpdateMetadata

	ID string `json:"id" validate:"nonzero"`
}

func (rs *ruleSet) AddMappingRule(mr MappingRuleData) error {
	err := rs.validateMappingRuleUpdate(mr)
	if err != nil {
		return err
	}

	m, err := rs.getMappingRuleByName(mr.Name)
	if err != nil && err != errNoSuchRule {
		return fmt.Errorf(ruleActionErrorFmt, "add", mr.Name, err)
	}
	meta := mr.UpdateMetadata
	if err == errNoSuchRule {
		if m, err = newMappingRuleFromFields(
			mr.Name,
			mr.Filters,
			mr.Policies,
			meta.cutoverNanos,
		); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "add", mr.Name, err)
		}
		rs.mappingRules = append(rs.mappingRules, m)
	} else {
		if err := m.revive(
			mr.Name,
			mr.Filters,
			mr.Policies,
			meta.cutoverNanos,
		); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "revive", mr.Name, err)
		}
	}

	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) UpdateMappingRule(mru MappingRuleUpdate) error {
	mrd := mru.Data
	err := rs.validateMappingRuleUpdate(mrd)
	if err != nil {
		return err
	}

	m, err := rs.getMappingRuleByID(mru.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mru.ID, err)
	}
	meta := mrd.UpdateMetadata
	if err := m.addSnapshot(
		mrd.Name,
		mrd.Filters,
		mrd.Policies,
		meta.cutoverNanos,
	); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", mrd.Name, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteMappingRule(d DeleteData) error {
	m, err := rs.getMappingRuleByID(d.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", d.ID, err)
	}
	meta := d.UpdateMetadata
	if err := m.markTombstoned(meta.cutoverNanos); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", d.ID, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) AddRollupRule(rr RollupRuleData) error {
	err := rs.validateRollupRuleUpdate(rr)
	if err != nil {
		return err
	}

	r, err := rs.getRollupRuleByName(rr.Name)
	if err != nil && err != errNoSuchRule {
		return fmt.Errorf(ruleActionErrorFmt, "add", rr.Name, err)
	}

	meta := rr.UpdateMetadata
	if err == errNoSuchRule {
		if r, err = newRollupRuleFromFields(
			rr.Name,
			rr.Filters,
			rr.Targets,
			meta.cutoverNanos,
		); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "add", rr.Name, err)
		}
		rs.rollupRules = append(rs.rollupRules, r)
	} else {
		if err := r.revive(
			rr.Name,
			rr.Filters,
			rr.Targets,
			meta.cutoverNanos,
		); err != nil {
			return fmt.Errorf(ruleActionErrorFmt, "revive", rr.Name, err)
		}
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) UpdateRollupRule(rru RollupRuleUpdate) error {
	rrd := rru.Data
	err := rs.validateRollupRuleUpdate(rrd)
	if err != nil {
		return err
	}

	r, err := rs.getRollupRuleByID(rru.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rru.ID, err)
	}

	meta := rrd.UpdateMetadata
	if err = r.addSnapshot(
		rrd.Name,
		rrd.Filters,
		rrd.Targets,
		meta.cutoverNanos,
	); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "update", rrd.Name, err)
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteRollupRule(d DeleteData) error {
	r, err := rs.getRollupRuleByID(d.ID)
	if err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", d.ID, err)
	}
	meta := d.UpdateMetadata
	if err := r.markTombstoned(meta.cutoverNanos); err != nil {
		return fmt.Errorf(ruleActionErrorFmt, "delete", d.ID, err)
	}
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
	rs.lastUpdatedAtNanos = meta.lastUpdatedAtNanos
}

func (rs ruleSet) getMappingRuleByName(name string) (*mappingRule, error) {
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

func (rs ruleSet) getMappingRuleByID(id string) (*mappingRule, error) {
	for _, m := range rs.mappingRules {
		if m.uuid == id {
			return m, nil
		}
	}
	return nil, errNoSuchRule
}

func (rs ruleSet) getRollupRuleByName(name string) (*rollupRule, error) {
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

func (rs ruleSet) getRollupRuleByID(id string) (*rollupRule, error) {
	for _, r := range rs.rollupRules {
		if r.uuid == id {
			return r, nil
		}
	}
	return nil, errNoSuchRule
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

// MarshalJSON returns the JSON encoding of staged policies.
func (rs ruleSet) MarshalJSON() ([]byte, error) {
	return json.Marshal(newRuleSetJSON(rs))
}

// UnmarshalJSON unmarshals JSON-encoded data into staged policies.
func (rs *ruleSet) UnmarshalJSON(data []byte) error {
	var rsj ruleSetJSON
	err := json.Unmarshal(data, &rsj)
	if err != nil {
		return err
	}
	*rs = *rsj.RuleSet()
	return nil
}

type ruleSetJSON struct {
	UUID               string            `json:"uuid"`
	Version            int               `json:"version"`
	Namespace          string            `json:"namespace"`
	CreatedAtNanos     int64             `json:"createdAt"`
	LastUpdatedAtNanos int64             `json:"lastUpdatedAt"`
	Tombstoned         bool              `json:"tombstoned"`
	CutoverNanos       int64             `json:"cutoverNanos"`
	MappingRules       []mappingRuleJSON `json:"mappingRules"`
	RollupRules        []rollupRuleJSON  `json:"rollupRules"`
}

func newRuleSetJSON(rs ruleSet) ruleSetJSON {
	mappingRuleJSONs := make([]mappingRuleJSON, len(rs.mappingRules))
	for i, m := range rs.mappingRules {
		mappingRuleJSONs[i] = newMappingRuleJSON(*m)
	}
	rollupRuleJSONs := make([]rollupRuleJSON, len(rs.rollupRules))
	for i, r := range rs.rollupRules {
		rollupRuleJSONs[i] = newRollupRuleJSON(*r)
	}

	return ruleSetJSON{
		UUID:               rs.uuid,
		Version:            rs.version,
		Namespace:          string(rs.namespace),
		CreatedAtNanos:     rs.createdAtNanos,
		LastUpdatedAtNanos: rs.lastUpdatedAtNanos,
		Tombstoned:         rs.tombstoned,
		CutoverNanos:       rs.cutoverNanos,
		MappingRules:       mappingRuleJSONs,
		RollupRules:        rollupRuleJSONs,
	}
}

// RuleSet returns a ruleSet representation of a ruleSetJSON
func (rsj ruleSetJSON) RuleSet() *ruleSet {
	mappingRules := make([]*mappingRule, len(rsj.MappingRules))
	for i, m := range rsj.MappingRules {
		rule := m.mappingRule()
		mappingRules[i] = &rule
	}

	rollupRules := make([]*rollupRule, len(rsj.RollupRules))
	for i, r := range rsj.RollupRules {
		rule := r.rollupRule()
		rollupRules[i] = &rule
	}

	return &ruleSet{
		uuid:               rsj.UUID,
		version:            rsj.Version,
		namespace:          []byte(rsj.Namespace),
		createdAtNanos:     rsj.CreatedAtNanos,
		lastUpdatedAtNanos: rsj.LastUpdatedAtNanos,
		tombstoned:         rsj.Tombstoned,
		cutoverNanos:       rsj.CutoverNanos,
		mappingRules:       mappingRules,
		rollupRules:        rollupRules,
	}
}

// RuleSetUpdateHelper stores the necessary details to create an UpdateMetadata.
type RuleSetUpdateHelper struct {
	propagationDelay time.Duration
}

// NewRuleSetUpdateHelper creates a new RuleSetUpdateHelper struct.
func NewRuleSetUpdateHelper(propagationDelay time.Duration) RuleSetUpdateHelper {
	return RuleSetUpdateHelper{propagationDelay: propagationDelay}
}

// NewUpdateMetadata creates a properly initialized UpdateMetadata object.
func (r RuleSetUpdateHelper) NewUpdateMetadata() UpdateMetadata {
	updateTime := time.Now().UnixNano()
	cutoverTime := updateTime + int64(r.propagationDelay)
	return UpdateMetadata{lastUpdatedAtNanos: updateTime, cutoverNanos: cutoverTime}
}

// RuleConflictError is returned when a rule modification is made that would conflict with the current state.
type RuleConflictError struct {
	ConflictRuleUUID string
	msg              string
}

func (e RuleConflictError) Error() string { return e.msg }

func (rs ruleSet) validateMappingRuleUpdate(mrd MappingRuleData) error {
	for _, m := range rs.mappingRules {
		if m.Tombstoned() {
			continue
		}
		if n, err := m.Name(); err != nil {
			continue
		} else if n == mrd.Name {
			return RuleConflictError{msg: fmt.Sprintf("Rule with name: %s already exists", n), ConflictRuleUUID: m.uuid}
		}
	}

	return nil
}

func (rs ruleSet) validateRollupRuleUpdate(rrd RollupRuleData) error {
	for _, r := range rs.rollupRules {
		if r.Tombstoned() {
			continue
		}

		if n, err := r.Name(); err != nil {
			continue
		} else if n == rrd.Name {
			return RuleConflictError{msg: fmt.Sprintf("Rule with name: %s already exists", n), ConflictRuleUUID: r.uuid}
		}

		if len(r.snapshots) == 0 {
			continue
		}
		latestSnapshot := r.snapshots[len(r.snapshots)-1]
		for _, t1 := range latestSnapshot.targets {
			for _, t2 := range rrd.Targets {
				if t1.sameTransform(t2) {
					return RuleConflictError{msg: fmt.Sprintf("Same rollup transformation: %s: %v already exists", t1.Name, t1.Tags), ConflictRuleUUID: r.uuid}
				}
			}
		}
	}

	return nil
}
