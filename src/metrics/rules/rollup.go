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

	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/pborman/uuid"
)

var (
	emptyRollupTarget rollupTarget

	errRollupRuleSnapshotIndexOutOfRange = errors.New("rollup rule snapshot index out of range")
	errNilRollupTargetSchema             = errors.New("nil rollup target schema")
	errNilRollupRuleSnapshotSchema       = errors.New("nil rollup rule snapshot schema")
	errNilRollupRuleSchema               = errors.New("nil rollup rule schema")
)

// RollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type rollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTarget(target *schema.RollupTarget) (rollupTarget, error) {
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
	return rollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

func newRollupTargetsFromView(views []models.RollupTargetView) []rollupTarget {
	targets := make([]rollupTarget, len(views))
	for i, t := range views {
		targets[i] = newRollupTargetFromView(t)
	}
	return targets
}

func newRollupTargetFromView(rtv models.RollupTargetView) rollupTarget {
	return rollupTarget{
		Name:     []byte(rtv.Name),
		Tags:     bytesArrayFromStringArray(rtv.Tags),
		Policies: rtv.Policies,
	}
}

func (t rollupTarget) rollupTargetView() models.RollupTargetView {
	return models.RollupTargetView{
		Name:     string(t.Name),
		Tags:     stringArrayFromBytesArray(t.Tags),
		Policies: t.Policies,
	}
}

// TODO: Evaluate if this function is needed for rule matching. If not remove it.
// SameTransform returns whether two rollup targets have the same transformation.
func (t *rollupTarget) SameTransform(other rollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	clonedTags := stringArrayFromBytesArray(t.Tags)
	sort.Strings(clonedTags)
	otherClonedTags := stringArrayFromBytesArray(other.Tags)
	sort.Strings(otherClonedTags)
	for i := 0; i < len(clonedTags); i++ {
		if clonedTags[i] != otherClonedTags[i] {
			return false
		}
	}
	return true
}

// clone clones a rollup target.
func (t *rollupTarget) clone() rollupTarget {
	name := make([]byte, len(t.Name))
	copy(name, t.Name)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return rollupTarget{
		Name:     name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

// Schema returns the schema representation of a rollup target.
func (t *rollupTarget) Schema() (*schema.RollupTarget, error) {
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
	name               string
	tombstoned         bool
	cutoverNanos       int64
	filter             filters.Filter
	targets            []rollupTarget
	rawFilter          string
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
}

func newRollupRuleSnapshot(
	r *schema.RollupRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*rollupRuleSnapshot, error) {
	if r == nil {
		return nil, errNilRollupRuleSnapshotSchema
	}
	targets := make([]rollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	filterValues, err := filters.ParseTagFilterValueMap(r.Filter)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(filterValues, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}

	return newRollupRuleSnapshotFromFieldsInternal(
		r.Name,
		r.Tombstoned,
		r.CutoverNanos,
		r.Filter,
		targets,
		filter,
		r.LastUpdatedAtNanos,
		r.LastUpdatedBy,
	), nil
}

func newRollupRuleSnapshotFromFields(
	name string,
	cutoverNanos int64,
	rawFilter string,
	targets []rollupTarget,
	filter filters.Filter,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) (*rollupRuleSnapshot, error) {
	if _, err := filters.ValidateTagsFilter(rawFilter); err != nil {
		return nil, err
	}
	return newRollupRuleSnapshotFromFieldsInternal(
		name,
		false,
		cutoverNanos,
		rawFilter,
		targets,
		filter,
		lastUpdatedAtNanos,
		lastUpdatedBy,
	), nil
}

// newRollupRuleSnapshotFromFieldsInternal creates a new rollup rule snapshot
// from various given fields assuming the filter has already been validated.
func newRollupRuleSnapshotFromFieldsInternal(
	name string,
	tombstoned bool,
	cutoverNanos int64,
	rawFilter string,
	targets []rollupTarget,
	filter filters.Filter,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) *rollupRuleSnapshot {
	return &rollupRuleSnapshot{
		name:               name,
		tombstoned:         tombstoned,
		cutoverNanos:       cutoverNanos,
		filter:             filter,
		targets:            targets,
		rawFilter:          rawFilter,
		lastUpdatedAtNanos: lastUpdatedAtNanos,
		lastUpdatedBy:      lastUpdatedBy,
	}
}

func (rrs *rollupRuleSnapshot) clone() rollupRuleSnapshot {
	targets := make([]rollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.clone()
	}
	var filter filters.Filter
	if rrs.filter != nil {
		filter = rrs.filter.Clone()
	}
	return rollupRuleSnapshot{
		name:               rrs.name,
		tombstoned:         rrs.tombstoned,
		cutoverNanos:       rrs.cutoverNanos,
		filter:             filter,
		targets:            targets,
		rawFilter:          rrs.rawFilter,
		lastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		lastUpdatedBy:      rrs.lastUpdatedBy,
	}
}

// Schema returns the given MappingRuleSnapshot in protobuf form.
func (rrs *rollupRuleSnapshot) Schema() (*schema.RollupRuleSnapshot, error) {
	res := &schema.RollupRuleSnapshot{
		Name:               rrs.name,
		Tombstoned:         rrs.tombstoned,
		CutoverNanos:       rrs.cutoverNanos,
		Filter:             rrs.rawFilter,
		LastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		LastUpdatedBy:      rrs.lastUpdatedBy,
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

func (rc *rollupRule) rollupRuleView(snapshotIdx int) (*models.RollupRuleView, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(rc.snapshots) {
		return nil, errRollupRuleSnapshotIndexOutOfRange
	}

	rrs := rc.snapshots[snapshotIdx].clone()
	targets := make([]models.RollupTargetView, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.rollupTargetView()
	}

	return &models.RollupRuleView{
		ID:                 rc.uuid,
		Name:               rrs.name,
		Tombstoned:         rrs.tombstoned,
		CutoverNanos:       rrs.cutoverNanos,
		Filter:             rrs.rawFilter,
		Targets:            targets,
		LastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		LastUpdatedBy:      rrs.lastUpdatedBy,
	}, nil
}

// rollupRule stores rollup rule snapshots.
type rollupRule struct {
	uuid      string
	snapshots []*rollupRuleSnapshot
}

func newRollupRule(
	rc *schema.RollupRule,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	if rc == nil {
		return nil, errNilRollupRuleSchema
	}
	snapshots := make([]*rollupRuleSnapshot, 0, len(rc.Snapshots))
	for i := 0; i < len(rc.Snapshots); i++ {
		rr, err := newRollupRuleSnapshot(rc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, rr)
	}
	return &rollupRule{
		uuid:      rc.Uuid,
		snapshots: snapshots,
	}, nil
}

func newRollupRuleFromFields(
	name string,
	rawFilter string,
	targets []rollupTarget,
	meta UpdateMetadata,
) (*rollupRule, error) {
	rr := rollupRule{uuid: uuid.New()}
	if err := rr.addSnapshot(name, rawFilter, targets, meta); err != nil {
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
	return &rollupRule{
		uuid:      rc.uuid,
		snapshots: rc.snapshots[idx:]}
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
	rawFilter string,
	rollupTargets []rollupTarget,
	meta UpdateMetadata,
) error {
	snapshot, err := newRollupRuleSnapshotFromFields(
		name,
		meta.cutoverNanos,
		rawFilter,
		rollupTargets,
		nil,
		meta.updatedAtNanos,
		meta.updatedBy,
	)
	if err != nil {
		return err
	}
	rc.snapshots = append(rc.snapshots, snapshot)
	return nil
}

func (rc *rollupRule) markTombstoned(meta UpdateMetadata) error {
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
	snapshot.cutoverNanos = meta.cutoverNanos
	snapshot.targets = nil
	snapshot.lastUpdatedAtNanos = meta.updatedAtNanos
	snapshot.lastUpdatedBy = meta.updatedBy
	rc.snapshots = append(rc.snapshots, &snapshot)
	return nil
}

func (rc *rollupRule) revive(
	name string,
	rawFilter string,
	targets []rollupTarget,
	meta UpdateMetadata,
) error {
	n, err := rc.Name()
	if err != nil {
		return err
	}
	if !rc.Tombstoned() {
		return merrors.NewRuleConflictError(fmt.Sprintf("%s is not tombstoned", n))
	}
	return rc.addSnapshot(name, rawFilter, targets, meta)
}

func (rc *rollupRule) history() ([]*models.RollupRuleView, error) {
	lastIdx := len(rc.snapshots) - 1
	views := make([]*models.RollupRuleView, len(rc.snapshots))
	// Snapshots are stored oldest -> newest. History should start with newest.
	for i := 0; i < len(rc.snapshots); i++ {
		rrs, err := rc.rollupRuleView(lastIdx - i)
		if err != nil {
			return nil, err
		}
		views[i] = rrs
	}
	return views, nil
}

// Schema returns the given RollupRule in protobuf form.
func (rc *rollupRule) Schema() (*schema.RollupRule, error) {
	snapshots := make([]*schema.RollupRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshot, err := s.Schema()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}

	return &schema.RollupRule{
		Uuid:      rc.uuid,
		Snapshots: snapshots,
	}, nil
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
