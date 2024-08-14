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

	"github.com/pborman/uuid"

	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
)

var (
	errNoRollupTargetsInRollupRuleSnapshot = errors.New("no rollup targets in rollup rule snapshot")
	errRollupRuleSnapshotIndexOutOfRange   = errors.New("rollup rule snapshot index out of range")
	errNilRollupRuleSnapshotProto          = errors.New("nil rollup rule snapshot proto")
	errNilRollupRuleProto                  = errors.New("nil rollup rule proto")
)

// rollupRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is rolled up using the provided list of rollup targets.
type rollupRuleSnapshot struct {
	name               string
	tombstoned         bool
	cutoverNanos       int64
	filter             filters.TagsFilter
	targets            []rollupTarget
	rawFilter          string
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
	keepOriginal       bool
	tags               []models.Tag
}

func newRollupRuleSnapshotFromProto(
	r *rulepb.RollupRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*rollupRuleSnapshot, error) {
	if r == nil {
		return nil, errNilRollupRuleSnapshotProto
	}
	var targets []rollupTarget
	if len(r.Targets) > 0 {
		// Convert v1 (i.e., legacy) rollup targets proto to rollup targets v2.
		targets = make([]rollupTarget, 0, len(r.Targets))
		for _, t := range r.Targets {
			target, err := newRollupTargetFromV1Proto(t)
			if err != nil {
				return nil, err
			}
			targets = append(targets, target)
		}
	} else if len(r.TargetsV2) > 0 {
		// Convert v2 rollup targets proto to rollup targest v2.
		targets = make([]rollupTarget, 0, len(r.TargetsV2))
		for _, t := range r.TargetsV2 {
			target, err := newRollupTargetFromV2Proto(t)
			if err != nil {
				return nil, err
			}
			targets = append(targets, target)
		}
	} else if !r.Tombstoned {
		return nil, errNoRollupTargetsInRollupRuleSnapshot
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
		r.KeepOriginal,
		models.TagsFromProto(r.Tags),
	), nil
}

func newRollupRuleSnapshotFromFields(
	name string,
	cutoverNanos int64,
	rawFilter string,
	targets []rollupTarget,
	filter filters.TagsFilter,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
	keepOriginal bool,
	tags []models.Tag,
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
		keepOriginal,
		tags,
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
	filter filters.TagsFilter,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
	keepOriginal bool,
	tags []models.Tag,
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
		keepOriginal:       keepOriginal,
		tags:               tags,
	}
}

func (rrs *rollupRuleSnapshot) clone() rollupRuleSnapshot {
	targets := make([]rollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.clone()
	}
	tags := make([]models.Tag, len(rrs.tags))
	copy(tags, rrs.tags)
	return rollupRuleSnapshot{
		name:               rrs.name,
		tombstoned:         rrs.tombstoned,
		cutoverNanos:       rrs.cutoverNanos,
		filter:             rrs.filter,
		targets:            targets,
		rawFilter:          rrs.rawFilter,
		lastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		lastUpdatedBy:      rrs.lastUpdatedBy,
		keepOriginal:       rrs.keepOriginal,
		tags:               tags,
	}
}

// proto returns the given MappingRuleSnapshot in protobuf form.
func (rrs *rollupRuleSnapshot) proto() (*rulepb.RollupRuleSnapshot, error) {
	tags := make([]*metricpb.Tag, 0, len(rrs.tags))
	for _, tag := range rrs.tags {
		tags = append(tags, tag.ToProto())
	}
	res := &rulepb.RollupRuleSnapshot{
		Name:               rrs.name,
		Tombstoned:         rrs.tombstoned,
		CutoverNanos:       rrs.cutoverNanos,
		Filter:             rrs.rawFilter,
		LastUpdatedAtNanos: rrs.lastUpdatedAtNanos,
		LastUpdatedBy:      rrs.lastUpdatedBy,
		KeepOriginal:       rrs.keepOriginal,
		Tags:               tags,
	}

	targets := make([]*rulepb.RollupTargetV2, len(rrs.targets))
	for i, t := range rrs.targets {
		target, err := t.proto()
		if err != nil {
			return nil, err
		}
		targets[i] = target
	}
	res.TargetsV2 = targets

	return res, nil
}

// rollupRule stores rollup rule snapshots.
type rollupRule struct {
	uuid      string
	snapshots []*rollupRuleSnapshot
}

func newEmptyRollupRule() *rollupRule {
	return &rollupRule{uuid: uuid.New()}
}

func newRollupRuleFromProto(
	rc *rulepb.RollupRule,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	if rc == nil {
		return nil, errNilRollupRuleProto
	}
	snapshots := make([]*rollupRuleSnapshot, 0, len(rc.Snapshots))
	for i := 0; i < len(rc.Snapshots); i++ {
		rr, err := newRollupRuleSnapshotFromProto(rc.Snapshots[i], opts)
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

// proto returns the proto message for the given rollup rule.
func (rc *rollupRule) proto() (*rulepb.RollupRule, error) {
	snapshots := make([]*rulepb.RollupRuleSnapshot, len(rc.snapshots))
	for i, s := range rc.snapshots {
		snapshot, err := s.proto()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}

	return &rulepb.RollupRule{
		Uuid:      rc.uuid,
		Snapshots: snapshots,
	}, nil
}

// activeSnapshot returns the latest rule snapshot whose cutover time is earlier
// than or equal to timeNanos, or nil if not found.
func (rc *rollupRule) activeSnapshot(timeNanos int64) *rollupRuleSnapshot {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return rc.snapshots[idx]
}

// activeRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future rules after time timeNanos.
func (rc *rollupRule) activeRule(timeNanos int64) *rollupRule {
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

func (rc *rollupRule) name() (string, error) {
	if len(rc.snapshots) == 0 {
		return "", errNoRuleSnapshots
	}
	latest := rc.snapshots[len(rc.snapshots)-1]
	return latest.name, nil
}

func (rc *rollupRule) tombstoned() bool {
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
	keepOriginal bool,
	tags []models.Tag,
) error {
	snapshot, err := newRollupRuleSnapshotFromFields(
		name,
		meta.cutoverNanos,
		rawFilter,
		rollupTargets,
		nil,
		meta.updatedAtNanos,
		meta.updatedBy,
		keepOriginal,
		tags,
	)
	if err != nil {
		return err
	}
	rc.snapshots = append(rc.snapshots, snapshot)
	return nil
}

func (rc *rollupRule) markTombstoned(meta UpdateMetadata) error {
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
	snapshot.keepOriginal = false
	rc.snapshots = append(rc.snapshots, &snapshot)
	return nil
}

func (rc *rollupRule) revive(
	name string,
	rawFilter string,
	targets []rollupTarget,
	meta UpdateMetadata,
	keepOriginal bool,
	tags []models.Tag,
) error {
	n, err := rc.name()
	if err != nil {
		return err
	}
	if !rc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is not tombstoned", n))
	}
	return rc.addSnapshot(name, rawFilter, targets, meta, keepOriginal, tags)
}

func (rc *rollupRule) history() ([]view.RollupRule, error) {
	lastIdx := len(rc.snapshots) - 1
	views := make([]view.RollupRule, len(rc.snapshots))
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

func (rc *rollupRule) rollupRuleView(snapshotIdx int) (view.RollupRule, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(rc.snapshots) {
		return view.RollupRule{}, errRollupRuleSnapshotIndexOutOfRange
	}

	rrs := rc.snapshots[snapshotIdx].clone()
	targets := make([]view.RollupTarget, len(rrs.targets))
	for i, t := range rrs.targets {
		targets[i] = t.rollupTargetView()
	}

	return view.RollupRule{
		ID:                  rc.uuid,
		Name:                rrs.name,
		Tombstoned:          rrs.tombstoned,
		CutoverMillis:       rrs.cutoverNanos / nanosPerMilli,
		Filter:              rrs.rawFilter,
		Targets:             targets,
		LastUpdatedBy:       rrs.lastUpdatedBy,
		LastUpdatedAtMillis: rrs.lastUpdatedAtNanos / nanosPerMilli,
		KeepOriginal:        rrs.keepOriginal,
		Tags:                rrs.tags,
	}, nil
}
