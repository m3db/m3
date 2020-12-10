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
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	merrors "github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"

	"github.com/pborman/uuid"
)

const (
	nanosPerMilli = int64(time.Millisecond / time.Nanosecond)
)

var (
	errNoStoragePoliciesAndDropPolicyInMappingRuleSnapshot = errors.New("no storage policies and no drop policy in mapping rule snapshot")
	errInvalidDropPolicyInMappRuleSnapshot                 = errors.New("invalid drop policy in mapping rule snapshot")
	errStoragePoliciesAndDropPolicyInMappingRuleSnapshot   = errors.New("storage policies and a drop policy specified in mapping rule snapshot")
	errMappingRuleSnapshotIndexOutOfRange                  = errors.New("mapping rule snapshot index out of range")
	errNilMappingRuleSnapshotProto                         = errors.New("nil mapping rule snapshot proto")
	errNilMappingRuleProto                                 = errors.New("nil mapping rule proto")

	pathSeparator = []byte(".")
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
	dropPolicy         policy.DropPolicy
	tags               []models.Tag
	graphitePrefix     [][]byte
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
		dropPolicy      policy.DropPolicy
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
	}

	if r.DropPolicy != policypb.DropPolicy_NONE {
		dropPolicy = policy.DropPolicy(r.DropPolicy)
		if !dropPolicy.IsValid() {
			return nil, errInvalidDropPolicyInMappRuleSnapshot
		}
	}

	if !r.Tombstoned && len(storagePolicies) == 0 && dropPolicy == policy.DropNone {
		return nil, errNoStoragePoliciesAndDropPolicyInMappingRuleSnapshot
	}

	if len(storagePolicies) > 0 && dropPolicy != policy.DropNone {
		return nil, errStoragePoliciesAndDropPolicyInMappingRuleSnapshot
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
		policy.DropPolicy(r.DropPolicy),
		models.TagsFromProto(r.Tags),
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
	dropPolicy policy.DropPolicy,
	tags []models.Tag,
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
		dropPolicy,
		tags,
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
	dropPolicy policy.DropPolicy,
	tags []models.Tag,
	lastUpdatedAtNanos int64,
	lastUpdatedBy string,
) *mappingRuleSnapshot {
	// If we have a graphite prefix tag, then parse that out here so that it
	// can be used later.
	var graphitePrefix [][]byte
	for _, tag := range tags {
		if bytes.Equal(tag.Name, metric.M3MetricsGraphitePrefix) {
			graphitePrefix = bytes.Split(tag.Value, pathSeparator)
		}
	}

	return &mappingRuleSnapshot{
		name:               name,
		tombstoned:         tombstoned,
		cutoverNanos:       cutoverNanos,
		filter:             filter,
		rawFilter:          rawFilter,
		aggregationID:      aggregationID,
		storagePolicies:    storagePolicies,
		dropPolicy:         dropPolicy,
		tags:               tags,
		graphitePrefix:     graphitePrefix,
		lastUpdatedAtNanos: lastUpdatedAtNanos,
		lastUpdatedBy:      lastUpdatedBy,
	}
}

func (mrs *mappingRuleSnapshot) clone() mappingRuleSnapshot {
	var filter filters.Filter
	if mrs.filter != nil {
		filter = mrs.filter.Clone()
	}
	tags := make([]models.Tag, len(mrs.tags))
	copy(tags, mrs.tags)
	return mappingRuleSnapshot{
		name:               mrs.name,
		tombstoned:         mrs.tombstoned,
		cutoverNanos:       mrs.cutoverNanos,
		filter:             filter,
		rawFilter:          mrs.rawFilter,
		aggregationID:      mrs.aggregationID,
		storagePolicies:    mrs.storagePolicies.Clone(),
		dropPolicy:         mrs.dropPolicy,
		tags:               mrs.tags,
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
	tags := make([]*metricpb.Tag, 0, len(mrs.tags))
	for _, tag := range mrs.tags {
		tags = append(tags, tag.ToProto())
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
		DropPolicy:         policypb.DropPolicy(mrs.dropPolicy),
		Tags:               tags,
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
	dropPolicy policy.DropPolicy,
	tags []models.Tag,
	meta UpdateMetadata,
) error {
	snapshot, err := newMappingRuleSnapshotFromFields(
		name,
		meta.cutoverNanos,
		nil,
		rawFilter,
		aggregationID,
		storagePolicies,
		dropPolicy,
		tags,
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
	snapshot := mc.snapshots[len(mc.snapshots)-1].clone()
	snapshot.tombstoned = true
	snapshot.cutoverNanos = meta.cutoverNanos
	snapshot.lastUpdatedAtNanos = meta.updatedAtNanos
	snapshot.lastUpdatedBy = meta.updatedBy
	snapshot.aggregationID = aggregation.DefaultID
	snapshot.storagePolicies = nil
	snapshot.dropPolicy = 0
	mc.snapshots = append(mc.snapshots, &snapshot)
	return nil
}

func (mc *mappingRule) revive(
	name string,
	rawFilter string,
	aggregationID aggregation.ID,
	storagePolicies policy.StoragePolicies,
	dropPolicy policy.DropPolicy,
	tags []models.Tag,
	meta UpdateMetadata,
) error {
	n, err := mc.name()
	if err != nil {
		return err
	}
	if !mc.tombstoned() {
		return merrors.NewInvalidInputError(fmt.Sprintf("%s is not tombstoned", n))
	}
	return mc.addSnapshot(name, rawFilter, aggregationID, storagePolicies,
		dropPolicy, tags, meta)
}

func (mc *mappingRule) activeIndex(timeNanos int64) int {
	idx := len(mc.snapshots) - 1
	for idx >= 0 && mc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func (mc *mappingRule) history() ([]view.MappingRule, error) {
	lastIdx := len(mc.snapshots) - 1
	views := make([]view.MappingRule, len(mc.snapshots))
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

func (mc *mappingRule) mappingRuleView(snapshotIdx int) (view.MappingRule, error) {
	if snapshotIdx < 0 || snapshotIdx >= len(mc.snapshots) {
		return view.MappingRule{}, errMappingRuleSnapshotIndexOutOfRange
	}

	mrs := mc.snapshots[snapshotIdx].clone()
	return view.MappingRule{
		ID:                  mc.uuid,
		Name:                mrs.name,
		Tombstoned:          mrs.tombstoned,
		CutoverMillis:       mrs.cutoverNanos / nanosPerMilli,
		DropPolicy:          mrs.dropPolicy,
		Filter:              mrs.rawFilter,
		AggregationID:       mrs.aggregationID,
		StoragePolicies:     mrs.storagePolicies,
		LastUpdatedBy:       mrs.lastUpdatedBy,
		LastUpdatedAtMillis: mrs.lastUpdatedAtNanos / nanosPerMilli,
		Tags:                mrs.tags,
	}, nil
}
