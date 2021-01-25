// Copyright (c) 2020 Uber Technologies, Inc.
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

	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/rules/view"

	"github.com/pborman/uuid"
)

var (
	errNoRollupTargetsInUtilizationRuleSnapshot = errors.New("no rollup targets in utilization rule snapshot")
	errUtilizationRuleSnapshotIndexOutOfRange   = errors.New("utilization rule snapshot index out of range")
	errNilUtilizationRuleSnapshotProto          = errors.New("nil utilization rule snapshot proto")
	errNilUtilizationRuleProto                  = errors.New("nil utilization rule proto")
)

// rollupRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is rolled up using the provided list of rollup targets.
type utilizationRuleSnapshot struct {
	name               string
	tombstoned         bool
	cutoverNanos       int64
	value              string
	filter             filters.Filter
	rawFilter          string
	targets            []rollupTarget
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
}

func newUtilizationRuleSnapshotFromProto(
	r *rulepb.UtilizationRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*utilizationRuleSnapshot, error) {
	if r == nil {
		return nil, errNilUtilizationRuleSnapshotProto
	}
	var targets []rollupTarget
	if len(r.Targets) > 0 {
		targets = make([]rollupTarget, 0, len(r.Targets))
		for _, t := range r.Targets {
			target, err := newRollupTargetFromV2Proto(t)
			if err != nil {
				return nil, err
			}
			targets = append(targets, target)
		}
	} else if !r.Tombstoned {
		return nil, errNoRollupTargetsInUtilizationRuleSnapshot
	}

	filterValues, err := filters.ParseTagFilterValueMap(r.Filter)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(filterValues, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}

	return newUtilizationRuleSnapshotFromFieldsInternal(
		r.Name,
		r.Tombstoned,
		r.CutoverNanos,
		r.Filter,
		filter,
		r.Value,
		targets,
		r.LastUpdatedAtNanos,
		r.LastUpdatedBy,
	), nil
}

func newUtilizationRuleSnapshotFromFields(
	name string,
	cutoverNanos int64,
	rawFilter string,
	targets []rollupTarget,
	filter filters.Filter,
	value string,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) (*utilizationRuleSnapshot, error) {
	if _, err := filters.ValidateTagsFilter(rawFilter); err != nil {
		return nil, err
	}
	return newUtilizationRuleSnapshotFromFieldsInternal(
		name,
		false,
		cutoverNanos,
		rawFilter,
		filter,
		value,
		targets,
		lastUpdatedAtNanos,
		lastUpdatedBy,
	), nil
}

// newRollupRuleSnapshotFromFieldsInternal creates a new rollup rule snapshot
// from various given fields assuming the filter has already been validated.
func newUtilizationRuleSnapshotFromFieldsInternal(
	name string,
	tombstoned bool,
	cutoverNanos int64,
	rawFilter string,
	filter filters.Filter,
	value string,
	targets []rollupTarget,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) *utilizationRuleSnapshot {
	return &utilizationRuleSnapshot{
		name:               name,
		tombstoned:         tombstoned,
		cutoverNanos:       cutoverNanos,
		filter:             filter,
		value:              value,
		targets:            targets,
		rawFilter:          rawFilter,
		lastUpdatedAtNanos: lastUpdatedAtNanos,
		lastUpdatedBy:      lastUpdatedBy,
	}
}

func (rrs *utilizationRuleSnapshot) clone() utilizationRuleSnapshot {
	targets := make([]rollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.clone()
	}
	var filter filters.Filter
	if rrs.filter != nil {
		filter = rrs.filter.Clone()
	}
	return utilizationRuleSnapshot{
		name:               rrs.name,
		tombstoned:         rrs.tombstoned,
		cutoverNanos:       rrs.cutoverNanos,
		filter:             filter,
		value:              rrs.value,
		targets:            targets,
		rawFilter:          rrs.rawFilter,
		lastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		lastUpdatedBy:      rrs.lastUpdatedBy,
	}
}

// proto returns the given MappingRuleSnapshot in protobuf form.
func (rrs *utilizationRuleSnapshot) proto() (*rulepb.UtilizationRuleSnapshot, error) {
	res := &rulepb.UtilizationRuleSnapshot{
		Name:               rrs.name,
		Tombstoned:         rrs.tombstoned,
		CutoverNanos:       rrs.cutoverNanos,
		Value:              rrs.value,
		Filter:             rrs.rawFilter,
		LastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		LastUpdatedBy:      rrs.lastUpdatedBy,
	}

	targets := make([]*rulepb.RollupTargetV2, len(rrs.targets))
	for i, t := range rrs.targets {
		target, err := t.proto()
		if err != nil {
			return nil, err
		}
		targets[i] = target
	}
	res.Targets = targets

	return res, nil
}

// rollupRule stores rollup rule snapshots.
type utilizationRule struct {
	uuid      string
	snapshots []*utilizationRuleSnapshot
}

func newEmptyUtilizationRule() *utilizationRule {
	return &utilizationRule{uuid: uuid.New()}
}

func newUtilizationRuleFromProto(
	rc *rulepb.UtilizationRule,
	opts filters.TagsFilterOptions,
) (*utilizationRule, error) {
	if rc == nil {
		return nil, errNilUtilizationRuleProto
	}
	snapshots := make([]*utilizationRuleSnapshot, 0, len(rc.Snapshots))
	for i := 0; i < len(rc.Snapshots); i++ {
		rr, err := newUtilizationRuleSnapshotFromProto(rc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, rr)
	}
	return &utilizationRule{
		uuid:      rc.Uuid,
		snapshots: snapshots,
	}, nil
}

func (rc utilizationRule) clone() utilizationRule {
	snapshots := make([]*utilizationRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		c := s.clone()
		snapshots[i] = &c
	}
	return utilizationRule{
		uuid:      rc.uuid,
		snapshots: snapshots,
	}
}

// proto returns the proto message for the given rollup rule.
func (rc *utilizationRule) proto() (*rulepb.UtilizationRule, error) {
	snapshots := make([]*rulepb.UtilizationRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshot, err := s.proto()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}

	return &rulepb.UtilizationRule{
		Uuid:      rc.uuid,
		Snapshots: snapshots,
	}, nil
}

// activeSnapshot returns the latest rule snapshot whose cutover time is earlier
// than or equal to timeNanos, or nil if not found.
func (rc *utilizationRule) activeSnapshot(timeNanos int64) *utilizationRuleSnapshot {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return rc.snapshots[idx]
}

// activeRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future rules after time timeNanos.
func (rc *utilizationRule) activeRule(timeNanos int64) *utilizationRule {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return rc
	}
	return &utilizationRule{
		uuid:      rc.uuid,
		snapshots: rc.snapshots[idx:]}
}

func (rc *utilizationRule) activeIndex(timeNanos int64) int {
	idx := len(rc.snapshots) - 1
	for idx >= 0 && rc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func (rc *utilizationRule) name() (string, error) {
	if len(rc.snapshots) == 0 {
		return "", errNoRuleSnapshots
	}
	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.name, nil
}

func (rc *utilizationRule) tombstoned() bool {
	if len(rc.snapshots) == 0 {
		return true
	}

	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.tombstoned
}

func (rc *utilizationRule) addSnapshot(
	name string,
	rawFilter string,
	rollupTargets []rollupTarget,
	meta UpdateMetadata,
	value string,
) error {
	snapshot, err := newUtilizationRuleSnapshotFromFields(
		name,
		meta.cutoverNanos,
		rawFilter,
		rollupTargets,
		nil,
		value,
		meta.updatedAtNanos,
		meta.updatedBy,
	)
	if err != nil {
		return err
	}
	rc.snapshots = append(rc.snapshots, snapshot)
	return nil
}

func (rc *utilizationRule) markTombstoned(meta UpdateMetadata) error {
	n, err := rc.name()
	if err != nil {
		return err
	}

	if rc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is already tombstoned", n))
	}

	if len(rc.snapshots) == 0 {
		return errNoRuleSnapshots
	}

	snapshot := rc.snapshots[len(rc.snapshots)-1].clone()
	snapshot.tombstoned = true
	snapshot.cutoverNanos = meta.cutoverNanos
	snapshot.lastUpdatedAtNanos = meta.updatedAtNanos
	snapshot.lastUpdatedBy = meta.updatedBy
	snapshot.targets = nil
	rc.snapshots = append(rc.snapshots, &snapshot)
	return nil
}

func (rc *utilizationRule) revive(
	name string,
	rawFilter string,
	targets []rollupTarget,
	meta UpdateMetadata,
	value string,
) error {
	n, err := rc.name()
	if err != nil {
		return err
	}
	if !rc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is not tombstoned", n))
	}
	return rc.addSnapshot(name, rawFilter, targets, meta, value)
}

func (rc *utilizationRule) history() ([]view.UtilizationRule, error) {
	lastIdx := len(rc.snapshots) - 1
	views := make([]view.UtilizationRule, len(rc.snapshots))
	// Snapshots are stored oldest -> newest. History should start with newest.
	for i := 0; i < len(rc.snapshots); i++ {
		rrs, err := rc.utilizationRuleView(lastIdx - i)
		if err != nil {
			return nil, err
		}
		views[i] = rrs
	}
	return views, nil
}

func (rc *utilizationRule) utilizationRuleView(snapshotIdx int) (view.UtilizationRule, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(rc.snapshots) {
		return view.UtilizationRule{}, errUtilizationRuleSnapshotIndexOutOfRange
	}

	rrs := rc.snapshots[snapshotIdx].clone()
	targets := make([]view.RollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.rollupTargetView()
	}

	return view.UtilizationRule{
		ID:                  rc.uuid,
		Name:                rrs.name,
		Tombstoned:          rrs.tombstoned,
		CutoverMillis:       rrs.cutoverNanos / nanosPerMilli,
		Filter:              rrs.rawFilter,
		Targets:             targets,
		LastUpdatedBy:       rrs.lastUpdatedBy,
		LastUpdatedAtMillis: rrs.lastUpdatedAtNanos / nanosPerMilli,
		Value:               rrs.value,
	}, nil
}
