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
	"sort"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"

	"github.com/pborman/uuid"
)

var (
	emptyRollupTarget RollupTarget

	errNilRollupTargetSchema       = errors.New("nil rollup target schema")
	errNilRollupRuleSnapshotSchema = errors.New("nil rollup rule snapshot schema")
	errNilRollupRuleSchema         = errors.New("nil rollup rule schema")
)

// RollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type RollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTarget(target *schema.RollupTarget) (RollupTarget, error) {
	if target == nil {
		return emptyRollupTarget, errNilRollupTargetSchema
	}
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

// NewRollupTargetFromFields creates a new rollupTarget from a list of its component fields.
func NewRollupTargetFromFields(name string, tags []string, policies []policy.Policy) RollupTarget {
	return RollupTarget{Name: []byte(name), Tags: bytesArrayFromStringArray(tags), Policies: policies}
}

// RollupTargetView is a human friendly representation of a rollup rule target at a given point in time.
type RollupTargetView struct {
	Name     string
	Tags     []string
	Policies []policy.Policy
}

func newRollupTargetView(target RollupTarget) RollupTargetView {
	return RollupTargetView{
		Name:     string(target.Name),
		Tags:     stringArrayFromBytesArray(target.Tags),
		Policies: target.Policies,
	}
}

func (rtv RollupTargetView) rollupTarget() RollupTarget {
	return RollupTarget{
		Name:     []byte(rtv.Name),
		Tags:     bytesArrayFromStringArray(rtv.Tags),
		Policies: rtv.Policies,
	}
}

func rollupTargetViewsToTargets(views []RollupTargetView) []RollupTarget {
	targets := make([]RollupTarget, len(views))
	for i, t := range views {
		targets[i] = t.rollupTarget()
	}
	return targets
}

func (t *RollupTarget) sameTransform(other RollupTarget) bool {
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

// clone clones a rollup target.
func (t *RollupTarget) clone() RollupTarget {
	name := make([]byte, len(t.Name))
	copy(name, t.Name)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return RollupTarget{
		Name:     name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

// Schema returns the schema representation of a rollup target.
func (t *RollupTarget) Schema() (*schema.RollupTarget, error) {
	res := &schema.RollupTarget{
		Name: string(t.Name),
	}

	policies := make([]*schema.Policy, len(t.Policies))
	for i, p := range t.Policies {
		policy, err := p.Schema()
		if err != nil {
			return nil, err
		}
		policies[i] = policy
	}
	res.Policies = policies
	res.Tags = stringArrayFromBytesArray(t.Tags)

	return res, nil
}

// rollupRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is rolled up using the provided list of rollup targets.
type rollupRuleSnapshot struct {
	name         string
	tombstoned   bool
	cutoverNanos int64
	filter       filters.Filter
	targets      []RollupTarget
	rawFilters   map[string]string
}

func newRollupRuleSnapshot(
	r *schema.RollupRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*rollupRuleSnapshot, error) {
	if r == nil {
		return nil, errNilRollupRuleSnapshotSchema
	}
	targets := make([]RollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}

	return newRollupRuleSnapshotFromFields(
		r.Name,
		r.Tombstoned,
		r.CutoverTime,
		r.TagFilters,
		targets,
		filter,
	), nil
}

func newRollupRuleSnapshotFromFields(
	name string,
	tombstoned bool,
	cutoverNanos int64,
	tagFilters map[string]string,
	targets []RollupTarget,
	filter filters.Filter,
) *rollupRuleSnapshot {
	return &rollupRuleSnapshot{
		name:         name,
		tombstoned:   tombstoned,
		cutoverNanos: cutoverNanos,
		filter:       filter,
		targets:      targets,
		rawFilters:   tagFilters,
	}
}

func (rrs *rollupRuleSnapshot) clone() rollupRuleSnapshot {
	rawFilters := make(map[string]string, len(rrs.rawFilters))
	for k, v := range rrs.rawFilters {
		rawFilters[k] = v
	}
	targets := make([]RollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.clone()
	}
	filter := rrs.filter.Clone()
	return rollupRuleSnapshot{
		name:         rrs.name,
		tombstoned:   rrs.tombstoned,
		cutoverNanos: rrs.cutoverNanos,
		filter:       filter,
		targets:      targets,
		rawFilters:   rawFilters,
	}
}

// Schema returns the given MappingRuleSnapshot in protobuf form.
func (rrs *rollupRuleSnapshot) Schema() (*schema.RollupRuleSnapshot, error) {
	res := &schema.RollupRuleSnapshot{
		Name:        rrs.name,
		Tombstoned:  rrs.tombstoned,
		CutoverTime: rrs.cutoverNanos,
		TagFilters:  rrs.rawFilters,
	}

	targets := make([]*schema.RollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		target, err := t.Schema()
		if err != nil {
			return nil, err
		}
		targets[i] = target
	}
	res.Targets = targets

	return res, nil
}

// RollupRuleView is a human friendly representation of a rollup rule at a given point in time.
type RollupRuleView struct {
	ID           string
	Name         string
	CutoverNanos int64
	Filters      map[string]string
	Targets      []RollupTargetView
}

func newRollupRuleView(uuid string, rrs rollupRuleSnapshot) RollupRuleView {
	rrs = rrs.clone()
	targets := make([]RollupTargetView, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = newRollupTargetView(t)
	}
	return RollupRuleView{
		ID:           uuid,
		Name:         rrs.name,
		CutoverNanos: rrs.cutoverNanos,
		Filters:      rrs.rawFilters,
		Targets:      targets,
	}
}

// rollupRule stores rollup rule snapshots.
type rollupRule struct {
	uuid      string
	snapshots []*rollupRuleSnapshot
}

func newRollupRule(
	mc *schema.RollupRule,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	if mc == nil {
		return nil, errNilRollupRuleSchema
	}
	snapshots := make([]*rollupRuleSnapshot, 0, len(mc.Snapshots))
	for i := 0; i < len(mc.Snapshots); i++ {
		mr, err := newRollupRuleSnapshot(mc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, mr)
	}
	return &rollupRule{
		uuid:      mc.Uuid,
		snapshots: snapshots,
	}, nil
}

func newRollupRuleFromFields(
	name string,
	rawFilters map[string]string,
	targets []RollupTarget,
	cutoverTime int64,
) (*rollupRule, error) {
	rr := rollupRule{uuid: uuid.New()}
	if err := rr.addSnapshot(name, rawFilters, targets, cutoverTime); err != nil {
		return nil, err
	}
	return &rr, nil
}

func (rc rollupRule) clone() rollupRule {
	snapshots := make([]*rollupRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		c := s.clone()
		snapshots[i] = &c
	}
	return rollupRule{
		uuid:      rc.uuid,
		snapshots: snapshots,
	}
}

// ActiveSnapshot returns the latest rule snapshot whose cutover time is earlier
// than or equal to timeNanos, or nil if not found.
func (rc *rollupRule) ActiveSnapshot(timeNanos int64) *rollupRuleSnapshot {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return rc.snapshots[idx]
}

// ActiveRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future rules after time timeNanos.
func (rc *rollupRule) ActiveRule(timeNanos int64) *rollupRule {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return rc
	}
	return &rollupRule{uuid: rc.uuid, snapshots: rc.snapshots[idx:]}
}

func (rc *rollupRule) activeIndex(timeNanos int64) int {
	idx := len(rc.snapshots) - 1
	for idx >= 0 && rc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func (rc *rollupRule) Name() (string, error) {
	if len(rc.snapshots) == 0 {
		return "", errNoRuleSnapshots
	}
	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.name, nil
}

func (rc *rollupRule) Tombstoned() bool {
	if len(rc.snapshots) == 0 {
		return true
	}

	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.tombstoned
}

func (rc *rollupRule) addSnapshot(
	name string,
	rawFilters map[string]string,
	rollupTargets []RollupTarget,
	cutoverTime int64,
) error {
	snapshot := newRollupRuleSnapshotFromFields(
		name,
		false,
		cutoverTime,
		rawFilters,
		rollupTargets,
		nil,
	)

	rc.snapshots = append(rc.snapshots, snapshot)
	return nil
}

func (rc *rollupRule) markTombstoned(cutoverTime int64) error {
	n, err := rc.Name()
	if err != nil {
		return err
	}

	if rc.Tombstoned() {
		return fmt.Errorf("%s is already tombstoned", n)
	}

	if len(rc.snapshots) == 0 {
		return errNoRuleSnapshots
	}

	snapshot := *rc.snapshots[len(rc.snapshots)-1]
	snapshot.tombstoned = true
	snapshot.cutoverNanos = cutoverTime
	rc.snapshots = append(rc.snapshots, &snapshot)
	return nil
}

func (rc *rollupRule) revive(
	name string,
	rawFilters map[string]string,
	targets []RollupTarget,
	cutoverTime int64,
) error {
	n, err := rc.Name()
	if err != nil {
		return err
	}
	if !rc.Tombstoned() {
		return fmt.Errorf("%s is not tombstoned", n)
	}
	return rc.addSnapshot(name, rawFilters, targets, cutoverTime)
}

func (rc *rollupRule) history() []RollupRuleView {
	views := make([]RollupRuleView, len(rc.snapshots))
	for i, s := range rc.snapshots {
		views[i] = newRollupRuleView(rc.uuid, *s)
	}

	return views
}

// Schema returns the given RollupRule in protobuf form.
func (rc *rollupRule) Schema() (*schema.RollupRule, error) {
	res := &schema.RollupRule{
		Uuid: rc.uuid,
	}

	snapshots := make([]*schema.RollupRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshot, err := s.Schema()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}
	res.Snapshots = snapshots

	return res, nil
}

func bytesArrayFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

func stringArrayFromBytesArray(values [][]byte) []string {
	result := make([]string, len(values))
	for i, bytes := range values {
		result[i] = string(bytes)
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
