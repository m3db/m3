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

	"github.com/m3db/m3metrics/aggregation"
	merrors "github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/pborman/uuid"
)

var (
	errNoStoragePoliciesInMappingRuleSnapshot = errors.New("no storage policies in mapping rule snapshot")
	errMappingRuleSnapshotIndexOutOfRange     = errors.New("mapping rule snapshot index out of range")
	errNilMappingRuleSnapshotProto            = errors.New("nil mapping rule snapshot proto")
	errNilMappingRuleProto                    = errors.New("nil mapping rule proto")
)

// mappingRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is aggregated and retained under the provided set of policies.
type mappingRuleSnapshot struct {
	name               string
	tombstoned         bool
	cutoverNanos       int64
	filter             filters.Filter
	rawFilter          string
	aggregationID      aggregation.ID
	storagePolicies    policy.StoragePolicies
	lastUpdatedAtNanos int64
	lastUpdatedBy      string
}

func newMappingRuleSnapshotFromProto(
	r *rulepb.MappingRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*mappingRuleSnapshot, error) {
	if r == nil {
		return nil, errNilMappingRuleSnapshotProto
	}
	var (
		aggregationID   aggregation.ID
		storagePolicies policy.StoragePolicies
		err             error
	)
	if len(r.Policies) > 0 {
		// Extract the aggregation ID and storage policies from v1 proto (i.e., policies list).
		aggregationID, storagePolicies, err = toAggregationIDAndStoragePolicies(r.Policies)
		if err != nil {
			return nil, err
		}
	} else if len(r.StoragePolicies) > 0 {
		// Unmarshal aggregation ID and storage policies directly from v2 proto.
		aggregationID, err = aggregation.NewIDFromProto(r.AggregationTypes)
		if err != nil {
			return nil, err
		}
		storagePolicies, err = policy.NewStoragePoliciesFromProto(r.StoragePolicies)
		if err != nil {
			return nil, err
		}
	} else {
		return nil, errNoStoragePoliciesInMappingRuleSnapshot
	}
	filterValues, err := filters.ParseTagFilterValueMap(r.Filter)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(filterValues, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}

	return newMappingRuleSnapshotFromFieldsInternal(
		r.Name,
		r.Tombstoned,
		r.CutoverNanos,
		filter,
		r.Filter,
		aggregationID,
		storagePolicies,
		r.LastUpdatedAtNanos,
		r.LastUpdatedBy,
	), nil
}

func newMappingRuleSnapshotFromFields(
	name string,
	cutoverNanos int64,
	filter filters.Filter,
	rawFilter string,
	aggregationID aggregation.ID,
	storagePolicies policy.StoragePolicies,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) (*mappingRuleSnapshot, error) {
	if _, err := filters.ValidateTagsFilter(rawFilter); err != nil {
		return nil, err
	}
	return newMappingRuleSnapshotFromFieldsInternal(
		name,
		false,
		cutoverNanos,
		filter,
		rawFilter,
		aggregationID,
		storagePolicies,
		lastUpdatedAtNanos,
		lastUpdatedBy,
	), nil
}

// newMappingRuleSnapshotFromFieldsInternal creates a new mapping rule snapshot
// from various given fields assuming the filter has already been validated.
func newMappingRuleSnapshotFromFieldsInternal(
	name string,
	tombstoned bool,
	cutoverNanos int64,
	filter filters.Filter,
	rawFilter string,
	aggregationID aggregation.ID,
	storagePolicies policy.StoragePolicies,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) *mappingRuleSnapshot {
	return &mappingRuleSnapshot{
		name:               name,
		tombstoned:         tombstoned,
		cutoverNanos:       cutoverNanos,
		filter:             filter,
		rawFilter:          rawFilter,
		aggregationID:      aggregationID,
		storagePolicies:    storagePolicies,
		lastUpdatedAtNanos: lastUpdatedAtNanos,
		lastUpdatedBy:      lastUpdatedBy,
	}
}

func (mrs *mappingRuleSnapshot) clone() mappingRuleSnapshot {
	var filter filters.Filter
	if mrs.filter != nil {
		filter = mrs.filter.Clone()
	}
	return mappingRuleSnapshot{
		name:               mrs.name,
		tombstoned:         mrs.tombstoned,
		cutoverNanos:       mrs.cutoverNanos,
		filter:             filter,
		rawFilter:          mrs.rawFilter,
		aggregationID:      mrs.aggregationID,
		storagePolicies:    mrs.storagePolicies.Clone(),
		lastUpdatedAtNanos: mrs.lastUpdatedAtNanos,
		lastUpdatedBy:      mrs.lastUpdatedBy,
	}
}

// proto returns the given MappingRuleSnapshot in protobuf form.
func (mrs *mappingRuleSnapshot) proto() (*rulepb.MappingRuleSnapshot, error) {
	aggTypes, err := mrs.aggregationID.Types()
	if err != nil {
		return nil, err
	}
	pbAggTypes, err := aggTypes.Proto()
	if err != nil {
		return nil, err
	}
	storagePolicies, err := mrs.storagePolicies.Proto()
	if err != nil {
		return nil, err
	}

	return &rulepb.MappingRuleSnapshot{
		Name:               mrs.name,
		Tombstoned:         mrs.tombstoned,
		CutoverNanos:       mrs.cutoverNanos,
		Filter:             mrs.rawFilter,
		LastUpdatedAtNanos: mrs.lastUpdatedAtNanos,
		LastUpdatedBy:      mrs.lastUpdatedBy,
		AggregationTypes:   pbAggTypes,
		StoragePolicies:    storagePolicies,
	}, nil
}

// mappingRule stores mapping rule snapshots.
type mappingRule struct {
	uuid      string
	snapshots []*mappingRuleSnapshot
}

func newEmptyMappingRule() *mappingRule {
	return &mappingRule{uuid: uuid.New()}
}

func newMappingRuleFromProto(
	mc *rulepb.MappingRule,
	opts filters.TagsFilterOptions,
) (*mappingRule, error) {
	if mc == nil {
		return nil, errNilMappingRuleProto
	}
	snapshots := make([]*mappingRuleSnapshot, 0, len(mc.Snapshots))
	for i := 0; i < len(mc.Snapshots); i++ {
		mr, err := newMappingRuleSnapshotFromProto(mc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, mr)
	}
	return &mappingRule{
		uuid:      mc.Uuid,
		snapshots: snapshots,
	}, nil
}

func (mc *mappingRule) clone() mappingRule {
	snapshots := make([]*mappingRuleSnapshot, len(mc.snapshots))
	for i, s := range mc.snapshots {
		c := s.clone()
		snapshots[i] = &c
	}
	return mappingRule{
		uuid:      mc.uuid,
		snapshots: snapshots,
	}
}

// proto returns the given MappingRule in protobuf form.
func (mc *mappingRule) proto() (*rulepb.MappingRule, error) {
	snapshots := make([]*rulepb.MappingRuleSnapshot, len(mc.snapshots))
	for i, s := range mc.snapshots {
		snapshot, err := s.proto()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}

	return &rulepb.MappingRule{
		Uuid:      mc.uuid,
		Snapshots: snapshots,
	}, nil
}

// activeSnapshot returns the active rule snapshot whose cutover time is no later than
// the time passed in, or nil if no such rule snapshot exists.
func (mc *mappingRule) activeSnapshot(timeNanos int64) *mappingRuleSnapshot {
	idx := mc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return mc.snapshots[idx]
}

// activeRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future snapshots after time timeNanos.
func (mc *mappingRule) activeRule(timeNanos int64) *mappingRule {
	idx := mc.activeIndex(timeNanos)
	// If there are no snapshots that are currently in effect, it means either all
	// snapshots are in the future, or there are no snapshots.
	if idx < 0 {
		return mc
	}
	return &mappingRule{
		uuid:      mc.uuid,
		snapshots: mc.snapshots[idx:],
	}
}

func (mc *mappingRule) name() (string, error) {
	if len(mc.snapshots) == 0 {
		return "", errNoRuleSnapshots
	}
	latest := mc.snapshots[len(mc.snapshots)-1]
	return latest.name, nil
}

func (mc *mappingRule) tombstoned() bool {
	if len(mc.snapshots) == 0 {
		return true
	}
	latest := mc.snapshots[len(mc.snapshots)-1]
	return latest.tombstoned
}

func (mc *mappingRule) addSnapshot(
	name string,
	rawFilter string,
	aggregationID aggregation.ID,
	storagePolicies policy.StoragePolicies,
	meta UpdateMetadata,
) error {
	snapshot, err := newMappingRuleSnapshotFromFields(
		name,
		meta.cutoverNanos,
		nil,
		rawFilter,
		aggregationID,
		storagePolicies,
		meta.updatedAtNanos,
		meta.updatedBy,
	)
	if err != nil {
		return err
	}
	mc.snapshots = append(mc.snapshots, snapshot)
	return nil
}

func (mc *mappingRule) markTombstoned(meta UpdateMetadata) error {
	n, err := mc.name()
	if err != nil {
		return err
	}

	if mc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is already tombstoned", n))
	}
	if len(mc.snapshots) == 0 {
		return errNoRuleSnapshots
	}
	snapshot := *mc.snapshots[len(mc.snapshots)-1]
	snapshot.tombstoned = true
	snapshot.cutoverNanos = meta.cutoverNanos
	snapshot.aggregationID = aggregation.DefaultID
	snapshot.storagePolicies = nil
	snapshot.lastUpdatedAtNanos = meta.updatedAtNanos
	snapshot.lastUpdatedBy = meta.updatedBy
	mc.snapshots = append(mc.snapshots, &snapshot)
	return nil
}

func (mc *mappingRule) revive(
	name string,
	rawFilter string,
	aggregationID aggregation.ID,
	storagePolicies policy.StoragePolicies,
	meta UpdateMetadata,
) error {
	n, err := mc.name()
	if err != nil {
		return err
	}
	if !mc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is not tombstoned", n))
	}
	return mc.addSnapshot(name, rawFilter, aggregationID, storagePolicies, meta)
}

func (mc *mappingRule) activeIndex(timeNanos int64) int {
	idx := len(mc.snapshots) - 1
	for idx >= 0 && mc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func (mc *mappingRule) history() ([]*models.MappingRuleView, error) {
	lastIdx := len(mc.snapshots) - 1
	views := make([]*models.MappingRuleView, len(mc.snapshots))
	// Snapshots are stored oldest -> newest. History should start with newest.
	for i := 0; i < len(mc.snapshots); i++ {
		mrs, err := mc.mappingRuleView(lastIdx - i)
		if err != nil {
			return nil, err
		}
		views[i] = mrs
	}
	return views, nil
}

func (mc *mappingRule) mappingRuleView(snapshotIdx int) (*models.MappingRuleView, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(mc.snapshots) {
		return nil, errMappingRuleSnapshotIndexOutOfRange
	}

	mrs := mc.snapshots[snapshotIdx].clone()
	return &models.MappingRuleView{
		ID:                 mc.uuid,
		Name:               mrs.name,
		Tombstoned:         mrs.tombstoned,
		CutoverNanos:       mrs.cutoverNanos,
		Filter:             mrs.rawFilter,
		AggregationID:      mrs.aggregationID,
		StoragePolicies:    mrs.storagePolicies,
		LastUpdatedAtNanos: mrs.lastUpdatedAtNanos,
		LastUpdatedBy:      mrs.lastUpdatedBy,
	}, nil
}
