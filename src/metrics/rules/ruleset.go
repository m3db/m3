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
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	metricid "github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/rules/view/changes"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/pborman/uuid"
)

const (
	timeNanosMax = int64(math.MaxInt64)
)

var (
	errNilRuleSetProto      = errors.New("nil rule set proto")
	errRuleSetNotTombstoned = errors.New("ruleset is not tombstoned")
	errRuleNotFound         = errors.New("rule not found")
	errNoRuleSnapshots      = errors.New("rule has no snapshots")
	ruleIDNotFoundErrorFmt  = "no rule with id %v"
	ruleActionErrorFmt      = "cannot %s rule %s"
	ruleSetActionErrorFmt   = "cannot %s ruleset %s"
	unknownOpTypeFmt        = "unknown op type %v"
)

// Matcher matches metrics against rules to determine applicable policies.
type Matcher interface {
	// ForwardMatch matches the applicable policies for a metric id between [fromNanos, toNanos).
	ForwardMatch(id metricid.ID, fromNanos, toNanos int64, opts MatchOptions) (MatchResult, error)
}

// Fetcher fetches rules.
type Fetcher interface {
	// LatestRollupRules returns the latest rollup rules for a given time.
	LatestRollupRules(namespace []byte, timeNanos int64) ([]view.RollupRule, error)
}

// ActiveSet is the currently active RuleSet.
type ActiveSet interface {
	Matcher
	Fetcher
}

// RuleSet is a read-only set of rules associated with a namespace.
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

	// Proto returns the rulepb.Ruleset representation of this ruleset.
	Proto() (*rulepb.RuleSet, error)

	// MappingRuleHistory returns a map of mapping rule id to states that rule has been in.
	MappingRules() (view.MappingRules, error)

	// RollupRuleHistory returns a map of rollup rule id to states that rule has been in.
	RollupRules() (view.RollupRules, error)

	// Latest returns the latest snapshot of a ruleset containing the latest snapshots
	// of each rule in the ruleset.
	Latest() (view.RuleSet, error)

	// ActiveSet returns the active ruleset at a given time.
	ActiveSet(timeNanos int64) ActiveSet

	// ToMutableRuleSet returns a mutable version of this ruleset.
	ToMutableRuleSet() MutableRuleSet
}

// MutableRuleSet is mutable ruleset.
type MutableRuleSet interface {
	RuleSet

	// Clone returns a copy of this MutableRuleSet.
	Clone() MutableRuleSet

	// AppendMappingRule creates a new mapping rule and adds it to this ruleset.
	// Should return the id of the newly created rule.
	AddMappingRule(view.MappingRule, UpdateMetadata) (string, error)

	// UpdateMappingRule creates a new mapping rule and adds it to this ruleset.
	UpdateMappingRule(view.MappingRule, UpdateMetadata) error

	// DeleteMappingRule deletes a mapping rule
	DeleteMappingRule(string, UpdateMetadata) error

	// AppendRollupRule creates a new rollup rule and adds it to this ruleset.
	// Should return the id of the newly created rule.
	AddRollupRule(view.RollupRule, UpdateMetadata) (string, error)

	// UpdateRollupRule creates a new rollup rule and adds it to this ruleset.
	UpdateRollupRule(view.RollupRule, UpdateMetadata) error

	// DeleteRollupRule deletes a rollup rule
	DeleteRollupRule(string, UpdateMetadata) error

	// Tombstone tombstones this ruleset and all of its rules.
	Delete(UpdateMetadata) error

	// Revive removes the tombstone from this ruleset. It does not revive any rules.
	Revive(UpdateMetadata) error

	// ApplyRuleSetChanges takes set of rule set changes and applies them to a ruleset.
	ApplyRuleSetChanges(changes.RuleSetChanges, UpdateMetadata) error
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
	newRollupIDFn      metricid.NewIDFn
}

// NewRuleSetFromProto creates a new RuleSet from a proto object.
func NewRuleSetFromProto(version int, rs *rulepb.RuleSet, opts Options) (RuleSet, error) {
	if rs == nil {
		return nil, errNilRuleSetProto
	}
	tagsFilterOpts := opts.TagsFilterOptions()
	mappingRules := make([]*mappingRule, 0, len(rs.MappingRules))
	for _, mappingRule := range rs.MappingRules {
		mc, err := newMappingRuleFromProto(mappingRule, tagsFilterOpts)
		if err != nil {
			return nil, err
		}
		mappingRules = append(mappingRules, mc)
	}
	rollupRules := make([]*rollupRule, 0, len(rs.RollupRules))
	for _, rollupRule := range rs.RollupRules {
		rc, err := newRollupRuleFromProto(rollupRule, tagsFilterOpts)
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
	}
	rs.updateMetadata(meta)
	return rs
}

func (rs *ruleSet) Namespace() []byte                { return rs.namespace }
func (rs *ruleSet) Version() int                     { return rs.version }
func (rs *ruleSet) CutoverNanos() int64              { return rs.cutoverNanos }
func (rs *ruleSet) Tombstoned() bool                 { return rs.tombstoned }
func (rs *ruleSet) LastUpdatedAtNanos() int64        { return rs.lastUpdatedAtNanos }
func (rs *ruleSet) CreatedAtNanos() int64            { return rs.createdAtNanos }
func (rs *ruleSet) ToMutableRuleSet() MutableRuleSet { return rs }

func (rs *ruleSet) ActiveSet(timeNanos int64) ActiveSet {
	mappingRules := make([]*mappingRule, 0, len(rs.mappingRules))
	for _, mappingRule := range rs.mappingRules {
		activeRule := mappingRule.activeRule(timeNanos)
		mappingRules = append(mappingRules, activeRule)
	}
	rollupRules := make([]*rollupRule, 0, len(rs.rollupRules))
	for _, rollupRule := range rs.rollupRules {
		activeRule := rollupRule.activeRule(timeNanos)
		rollupRules = append(rollupRules, activeRule)
	}
	return newActiveRuleSet(
		rs.version,
		mappingRules,
		rollupRules,
		rs.tagsFilterOpts,
		rs.newRollupIDFn,
	)
}

// Proto returns the protobuf representation of a ruleset.
func (rs *ruleSet) Proto() (*rulepb.RuleSet, error) {
	res := &rulepb.RuleSet{
		Uuid:               rs.uuid,
		Namespace:          string(rs.namespace),
		CreatedAtNanos:     rs.createdAtNanos,
		LastUpdatedAtNanos: rs.lastUpdatedAtNanos,
		LastUpdatedBy:      rs.lastUpdatedBy,
		Tombstoned:         rs.tombstoned,
		CutoverNanos:       rs.cutoverNanos,
	}

	mappingRules := make([]*rulepb.MappingRule, len(rs.mappingRules))
	for i, m := range rs.mappingRules {
		mr, err := m.proto()
		if err != nil {
			return nil, err
		}
		mappingRules[i] = mr
	}
	res.MappingRules = mappingRules

	rollupRules := make([]*rulepb.RollupRule, len(rs.rollupRules))
	for i, r := range rs.rollupRules {
		rr, err := r.proto()
		if err != nil {
			return nil, err
		}
		rollupRules[i] = rr
	}
	res.RollupRules = rollupRules

	return res, nil
}

func (rs *ruleSet) MappingRules() (view.MappingRules, error) {
	mappingRules := make(view.MappingRules, len(rs.mappingRules))
	for _, m := range rs.mappingRules {
		hist, err := m.history()
		if err != nil {
			return nil, err
		}
		mappingRules[m.uuid] = hist
	}
	return mappingRules, nil
}

func (rs *ruleSet) RollupRules() (view.RollupRules, error) {
	rollupRules := make(view.RollupRules, len(rs.rollupRules))
	for _, r := range rs.rollupRules {
		hist, err := r.history()
		if err != nil {
			return nil, err
		}
		rollupRules[r.uuid] = hist
	}
	return rollupRules, nil
}

func (rs *ruleSet) Latest() (view.RuleSet, error) {
	mrs, err := rs.latestMappingRules()
	if err != nil {
		return view.RuleSet{}, err
	}
	rrs, err := rs.latestRollupRules()
	if err != nil {
		return view.RuleSet{}, err
	}
	return view.RuleSet{
		Namespace:     string(rs.Namespace()),
		Version:       rs.Version(),
		CutoverMillis: rs.CutoverNanos() / nanosPerMilli,
		MappingRules:  mrs,
		RollupRules:   rrs,
	}, nil
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
	return &ruleSet{
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
	}
}

func (rs *ruleSet) AddMappingRule(mrv view.MappingRule, meta UpdateMetadata) (string, error) {
	m, err := rs.getMappingRuleByName(mrv.Name)
	if err != nil && err != errRuleNotFound {
		return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "add", mrv.Name))
	}
	if err == errRuleNotFound {
		m = newEmptyMappingRule()
		if err = m.addSnapshot(
			mrv.Name,
			mrv.Filter,
			mrv.AggregationID,
			mrv.StoragePolicies,
			mrv.DropPolicy,
			mrv.Tags,
			meta,
		); err != nil {
			return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "add", mrv.Name))
		}
		rs.mappingRules = append(rs.mappingRules, m)
	} else {
		if err := m.revive(
			mrv.Name,
			mrv.Filter,
			mrv.AggregationID,
			mrv.StoragePolicies,
			mrv.DropPolicy,
			mrv.Tags,
			meta,
		); err != nil {
			return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "revive", mrv.Name))
		}
	}
	rs.updateMetadata(meta)
	return m.uuid, nil
}

func (rs *ruleSet) UpdateMappingRule(mrv view.MappingRule, meta UpdateMetadata) error {
	m, err := rs.getMappingRuleByID(mrv.ID)
	if err != nil {
		return merrors.NewInvalidInputError(fmt.Sprintf(ruleIDNotFoundErrorFmt, mrv.ID))
	}
	if err := m.addSnapshot(
		mrv.Name,
		mrv.Filter,
		mrv.AggregationID,
		mrv.StoragePolicies,
		mrv.DropPolicy,
		mrv.Tags,
		meta,
	); err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "update", mrv.Name))
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteMappingRule(id string, meta UpdateMetadata) error {
	m, err := rs.getMappingRuleByID(id)
	if err != nil {
		return merrors.NewInvalidInputError(fmt.Sprintf(ruleIDNotFoundErrorFmt, id))
	}

	if err := m.markTombstoned(meta); err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "delete", id))
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) AddRollupRule(rrv view.RollupRule, meta UpdateMetadata) (string, error) {
	r, err := rs.getRollupRuleByName(rrv.Name)
	if err != nil && err != errRuleNotFound {
		return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "add", rrv.Name))
	}
	targets := newRollupTargetsFromView(rrv.Targets)
	if err == errRuleNotFound {
		r = newEmptyRollupRule()
		if err = r.addSnapshot(
			rrv.Name,
			rrv.Filter,
			targets,
			meta,
			rrv.KeepOriginal,
			rrv.Tags,
		); err != nil {
			return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "add", rrv.Name))
		}
		rs.rollupRules = append(rs.rollupRules, r)
	} else {
		if err := r.revive(
			rrv.Name,
			rrv.Filter,
			targets,
			meta,
			rrv.KeepOriginal,
			rrv.Tags,
		); err != nil {
			return "", xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "revive", rrv.Name))
		}
	}
	rs.updateMetadata(meta)
	return r.uuid, nil
}

func (rs *ruleSet) UpdateRollupRule(rrv view.RollupRule, meta UpdateMetadata) error {
	r, err := rs.getRollupRuleByID(rrv.ID)
	if err != nil {
		return merrors.NewInvalidInputError(fmt.Sprintf(ruleIDNotFoundErrorFmt, rrv.ID))
	}
	targets := newRollupTargetsFromView(rrv.Targets)
	if err = r.addSnapshot(
		rrv.Name,
		rrv.Filter,
		targets,
		meta,
		rrv.KeepOriginal,
		rrv.Tags,
	); err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "update", rrv.Name))
	}
	rs.updateMetadata(meta)
	return nil
}

func (rs *ruleSet) DeleteRollupRule(id string, meta UpdateMetadata) error {
	r, err := rs.getRollupRuleByID(id)
	if err != nil {
		return merrors.NewInvalidInputError(fmt.Sprintf(ruleIDNotFoundErrorFmt, id))
	}

	if err := r.markTombstoned(meta); err != nil {
		return xerrors.Wrap(err, fmt.Sprintf(ruleActionErrorFmt, "delete", id))
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
		if t := m.tombstoned(); !t {
			_ = m.markTombstoned(meta)
		}
	}

	for _, r := range rs.rollupRules {
		if t := r.tombstoned(); !t {
			_ = r.markTombstoned(meta)
		}
	}

	return nil
}

func (rs *ruleSet) ApplyRuleSetChanges(rsc changes.RuleSetChanges, meta UpdateMetadata) error {
	if err := rs.applyMappingRuleChanges(rsc.MappingRuleChanges, meta); err != nil {
		return err
	}
	return rs.applyRollupRuleChanges(rsc.RollupRuleChanges, meta)
}

func (rs *ruleSet) Revive(meta UpdateMetadata) error {
	if !rs.Tombstoned() {
		return xerrors.Wrap(errRuleSetNotTombstoned, fmt.Sprintf(ruleSetActionErrorFmt, "revive", string(rs.namespace)))
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
		n, err := m.name()
		if err != nil {
			continue
		}

		if n == name {
			return m, nil
		}
	}
	return nil, errRuleNotFound
}

func (rs *ruleSet) getMappingRuleByID(id string) (*mappingRule, error) {
	for _, m := range rs.mappingRules {
		if m.uuid == id {
			return m, nil
		}
	}
	return nil, errRuleNotFound
}

func (rs *ruleSet) getRollupRuleByName(name string) (*rollupRule, error) {
	for _, r := range rs.rollupRules {
		n, err := r.name()
		if err != nil {
			return nil, err
		}

		if n == name {
			return r, nil
		}
	}
	return nil, errRuleNotFound
}

func (rs *ruleSet) getRollupRuleByID(id string) (*rollupRule, error) {
	for _, r := range rs.rollupRules {
		if r.uuid == id {
			return r, nil
		}
	}
	return nil, errRuleNotFound
}

func (rs *ruleSet) latestMappingRules() ([]view.MappingRule, error) {
	mrs, err := rs.MappingRules()
	if err != nil {
		return nil, err
	}
	filtered := make([]view.MappingRule, 0, len(mrs))
	for _, m := range mrs {
		if len(m) > 0 && !m[0].Tombstoned {
			// Rule snapshots are sorted by cutover time in descending order.
			filtered = append(filtered, m[0])
		}
	}
	sort.Sort(view.MappingRulesByNameAsc(filtered))
	return filtered, nil
}

func (rs *ruleSet) latestRollupRules() ([]view.RollupRule, error) {
	rrs, err := rs.RollupRules()
	if err != nil {
		return nil, err
	}
	filtered := make([]view.RollupRule, 0, len(rrs))
	for _, r := range rrs {
		if len(r) > 0 && !r[0].Tombstoned {
			// Rule snapshots are sorted by cutover time in descending order.
			filtered = append(filtered, r[0])
		}
	}
	sort.Sort(view.RollupRulesByNameAsc(filtered))
	return filtered, nil
}

func (rs *ruleSet) applyMappingRuleChanges(mrChanges []changes.MappingRuleChange, meta UpdateMetadata) error {
	for _, mrChange := range mrChanges {
		switch mrChange.Op {
		case changes.AddOp:
			if _, err := rs.AddMappingRule(*mrChange.RuleData, meta); err != nil {
				return err
			}
		case changes.ChangeOp:
			if err := rs.UpdateMappingRule(*mrChange.RuleData, meta); err != nil {
				return err
			}
		case changes.DeleteOp:
			if err := rs.DeleteMappingRule(*mrChange.RuleID, meta); err != nil {
				return err
			}
		default:
			return merrors.NewInvalidInputError(fmt.Sprintf(unknownOpTypeFmt, mrChange.Op))
		}
	}

	return nil
}

func (rs *ruleSet) applyRollupRuleChanges(rrChanges []changes.RollupRuleChange, meta UpdateMetadata) error {
	for _, rrChange := range rrChanges {
		switch rrChange.Op {
		case changes.AddOp:
			if _, err := rs.AddRollupRule(*rrChange.RuleData, meta); err != nil {
				return err
			}
		case changes.ChangeOp:
			if err := rs.UpdateRollupRule(*rrChange.RuleData, meta); err != nil {
				return err
			}
		case changes.DeleteOp:
			if err := rs.DeleteRollupRule(*rrChange.RuleID, meta); err != nil {
				return err
			}
		default:
			return merrors.NewInvalidInputError(fmt.Sprintf(unknownOpTypeFmt, rrChange.Op))
		}
	}

	return nil
}

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
