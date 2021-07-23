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
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/errors"
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/query/models"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testMappingRuleSnapshot1V1Proto = &rulepb.MappingRuleSnapshot{
		Name:         "foo",
		Tombstoned:   false,
		CutoverNanos: 12345000000,
		Filter:       "tag1:value1 tag2:value2",
		Policies: []*policypb.Policy{
			&policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: int64(10 * time.Second),
						Precision:  int64(time.Second),
					},
					Retention: policypb.Retention{
						Period: int64(24 * time.Hour),
					},
				},
			},
		},
		DropPolicy:         policypb.DropPolicy_NONE,
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
	}
	testMappingRuleSnapshot2V1Proto = &rulepb.MappingRuleSnapshot{
		Name:         "bar",
		Tombstoned:   true,
		CutoverNanos: 67890000000,
		Filter:       "tag3:value3 tag4:value4",
		Policies: []*policypb.Policy{
			&policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: int64(time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: policypb.Retention{
						Period: int64(24 * time.Hour),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{
					aggregationpb.AggregationType_MEAN,
				},
			},
			&policypb.Policy{
				StoragePolicy: &policypb.StoragePolicy{
					Resolution: policypb.Resolution{
						WindowSize: int64(5 * time.Minute),
						Precision:  int64(time.Minute),
					},
					Retention: policypb.Retention{
						Period: int64(48 * time.Hour),
					},
				},
				AggregationTypes: []aggregationpb.AggregationType{
					aggregationpb.AggregationType_MEAN,
				},
			},
		},
		DropPolicy:         policypb.DropPolicy_NONE,
		LastUpdatedAtNanos: 67890000000,
		LastUpdatedBy:      "someone-else",
	}
	testMappingRuleSnapshot3V2Proto = &rulepb.MappingRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       12345000000,
		Filter:             "tag1:value1 tag2:value2",
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
		StoragePolicies: []*policypb.StoragePolicy{
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 720 * time.Hour.Nanoseconds(),
				},
			},
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: time.Hour.Nanoseconds(),
					Precision:  time.Hour.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 365 * 24 * time.Hour.Nanoseconds(),
				},
			},
		},
		DropPolicy: policypb.DropPolicy_NONE,
		Tags:       []*metricpb.Tag{},
	}
	testMappingRuleSnapshot4V2Proto = &rulepb.MappingRuleSnapshot{
		Name:               "bar",
		Tombstoned:         true,
		CutoverNanos:       67890000000,
		Filter:             "tag3:value3 tag4:value4",
		LastUpdatedAtNanos: 67890000000,
		LastUpdatedBy:      "someone-else",
		AggregationTypes: []aggregationpb.AggregationType{
			aggregationpb.AggregationType_MIN,
			aggregationpb.AggregationType_MAX,
		},
		StoragePolicies: []*policypb.StoragePolicy{
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Minute.Nanoseconds(),
					Precision:  time.Minute.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 1800 * time.Hour.Nanoseconds(),
				},
			},
		},
		DropPolicy: policypb.DropPolicy_NONE,
		Tags:       []*metricpb.Tag{},
	}
	testMappingRuleSnapshot5V2Proto = &rulepb.MappingRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       12345000000,
		Filter:             "tag1:value1 tag2:value2",
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
		StoragePolicies:    []*policypb.StoragePolicy{},
		DropPolicy:         policypb.DropPolicy_DROP_MUST,
		Tags:               []*metricpb.Tag{},
	}
	testMappingRuleSnapshot6V2Proto = &rulepb.MappingRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       67890000000,
		Filter:             "tag1:value1 tag2:value2",
		LastUpdatedAtNanos: 67890000000,
		LastUpdatedBy:      "someone-else",
		StoragePolicies:    []*policypb.StoragePolicy{},
		DropPolicy:         policypb.DropPolicy_DROP_IF_ONLY_MATCH,
		Tags:               []*metricpb.Tag{},
	}
	testMappingRule1V1Proto = &rulepb.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.MappingRuleSnapshot{
			testMappingRuleSnapshot1V1Proto,
			testMappingRuleSnapshot2V1Proto,
		},
	}
	testMappingRule2V2Proto = &rulepb.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.MappingRuleSnapshot{
			testMappingRuleSnapshot3V2Proto,
			testMappingRuleSnapshot4V2Proto,
		},
	}
	testMappingRule3V2Proto = &rulepb.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.MappingRuleSnapshot{
			testMappingRuleSnapshot5V2Proto,
			testMappingRuleSnapshot6V2Proto,
		},
	}
	testMappingRuleSnapshot1 = &mappingRuleSnapshot{
		name:          "foo",
		tombstoned:    false,
		cutoverNanos:  12345000000,
		rawFilter:     "tag1:value1 tag2:value2",
		aggregationID: aggregation.DefaultID,
		storagePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
		},
		dropPolicy:         policy.DropNone,
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
		tags:               []models.Tag{},
	}
	testMappingRuleSnapshot2 = &mappingRuleSnapshot{
		name:          "bar",
		tombstoned:    true,
		cutoverNanos:  67890000000,
		rawFilter:     "tag3:value3 tag4:value4",
		aggregationID: aggregation.MustCompressTypes(aggregation.Mean),
		storagePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		},
		dropPolicy:         policy.DropNone,
		lastUpdatedAtNanos: 67890000000,
		lastUpdatedBy:      "someone-else",
		tags:               []models.Tag{},
	}
	testMappingRuleSnapshot3 = &mappingRuleSnapshot{
		name:          "foo",
		tombstoned:    false,
		cutoverNanos:  12345000000,
		rawFilter:     "tag1:value1 tag2:value2",
		aggregationID: aggregation.DefaultID,
		storagePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
			policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
			policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
		},
		dropPolicy:         policy.DropNone,
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
		tags:               []models.Tag{},
	}
	testMappingRuleSnapshot4 = &mappingRuleSnapshot{
		name:          "bar",
		tombstoned:    true,
		cutoverNanos:  67890000000,
		rawFilter:     "tag3:value3 tag4:value4",
		aggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
		storagePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
		},
		dropPolicy:         policy.DropNone,
		lastUpdatedAtNanos: 67890000000,
		lastUpdatedBy:      "someone-else",
		tags:               []models.Tag{},
	}
	testMappingRuleSnapshot5 = &mappingRuleSnapshot{
		name:               "foo",
		tombstoned:         false,
		cutoverNanos:       12345000000,
		rawFilter:          "tag1:value1 tag2:value2",
		aggregationID:      aggregation.DefaultID,
		storagePolicies:    policy.StoragePolicies{},
		dropPolicy:         policy.DropMust,
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
		tags:               []models.Tag{},
	}
	testMappingRuleSnapshot6 = &mappingRuleSnapshot{
		name:               "foo",
		tombstoned:         false,
		cutoverNanos:       67890000000,
		rawFilter:          "tag1:value1 tag2:value2",
		aggregationID:      aggregation.DefaultID,
		storagePolicies:    policy.StoragePolicies{},
		dropPolicy:         policy.DropIfOnlyMatch,
		lastUpdatedAtNanos: 67890000000,
		lastUpdatedBy:      "someone-else",
		tags:               []models.Tag{},
	}
	testMappingRule1 = &mappingRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*mappingRuleSnapshot{
			testMappingRuleSnapshot1,
			testMappingRuleSnapshot2,
		},
	}
	testMappingRule2 = &mappingRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*mappingRuleSnapshot{
			testMappingRuleSnapshot3,
			testMappingRuleSnapshot4,
		},
	}
	testMappingRule3 = &mappingRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*mappingRuleSnapshot{
			testMappingRuleSnapshot5,
			testMappingRuleSnapshot6,
		},
	}
	testMappingRuleSnapshotCmpOpts = []cmp.Option{
		cmp.AllowUnexported(mappingRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
	testMappingRuleCmpOpts = []cmp.Option{
		cmp.AllowUnexported(mappingRule{}),
		cmp.AllowUnexported(mappingRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
)

func TestNewMappingRuleSnapshotFromProtoNilProto(t *testing.T) {
	_, err := newMappingRuleSnapshotFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilMappingRuleSnapshotProto, err)
}

func TestNewMappingRuleSnapshotFromV1ProtoInvalidProto(t *testing.T) {
	proto := &rulepb.MappingRuleSnapshot{
		Policies: []*policypb.Policy{
			&policypb.Policy{},
		},
	}
	_, err := newMappingRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Error(t, err)
}

func TestNewMappingRuleSnapshotFromV1Proto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.MappingRuleSnapshot{
		testMappingRuleSnapshot1V1Proto,
		testMappingRuleSnapshot2V1Proto,
	}
	expected := []*mappingRuleSnapshot{
		testMappingRuleSnapshot1,
		testMappingRuleSnapshot2,
	}
	for i, input := range inputs {
		res, err := newMappingRuleSnapshotFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testMappingRuleSnapshotCmpOpts...))
		require.NotNil(t, res.filter)
	}
}

func TestNewMappingRuleSnapshotFromV2ProtoInvalidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	proto := &rulepb.MappingRuleSnapshot{
		AggregationTypes: []aggregationpb.AggregationType{
			aggregationpb.AggregationType_UNKNOWN,
		},
	}
	_, err := newMappingRuleSnapshotFromProto(proto, filterOpts)
	require.Error(t, err)
}

func TestNewMappingRuleSnapshotFromV2Proto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.MappingRuleSnapshot{
		testMappingRuleSnapshot3V2Proto,
		testMappingRuleSnapshot4V2Proto,
		testMappingRuleSnapshot5V2Proto,
		testMappingRuleSnapshot6V2Proto,
	}
	expected := []*mappingRuleSnapshot{
		testMappingRuleSnapshot3,
		testMappingRuleSnapshot4,
		testMappingRuleSnapshot5,
		testMappingRuleSnapshot6,
	}
	for i, input := range inputs {
		res, err := newMappingRuleSnapshotFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testMappingRuleSnapshotCmpOpts...))
		require.NotNil(t, res.filter)
	}
}

func TestNewMappingRuleSnapshotFromProtoTombstoned(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	input := &rulepb.MappingRuleSnapshot{
		Name:               "foo",
		Tombstoned:         true,
		CutoverNanos:       12345000000,
		Filter:             "tag1:value1 tag2:value2",
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
		Tags:               []*metricpb.Tag{},
	}
	res, err := newMappingRuleSnapshotFromProto(input, filterOpts)
	require.NoError(t, err)

	expected := &mappingRuleSnapshot{
		name:               "foo",
		tombstoned:         true,
		cutoverNanos:       12345000000,
		rawFilter:          "tag1:value1 tag2:value2",
		aggregationID:      aggregation.DefaultID,
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
		tags:               []models.Tag{},
	}
	require.True(t, cmp.Equal(expected, res, testMappingRuleSnapshotCmpOpts...))
	require.NotNil(t, res.filter)
}

func TestNewMappingRuleSnapshotNoStoragePoliciesAndDropPolicy(t *testing.T) {
	proto := &rulepb.MappingRuleSnapshot{}
	_, err := newMappingRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Equal(t, errNoStoragePoliciesAndDropPolicyInMappingRuleSnapshot, err)
}

func TestNewMappingRuleSnapshotStoragePoliciesAndDropPolicy(t *testing.T) {
	proto := &rulepb.MappingRuleSnapshot{
		StoragePolicies: []*policypb.StoragePolicy{
			&policypb.StoragePolicy{
				Resolution: policypb.Resolution{
					WindowSize: 10 * time.Second.Nanoseconds(),
					Precision:  time.Second.Nanoseconds(),
				},
				Retention: policypb.Retention{
					Period: 24 * time.Hour.Nanoseconds(),
				},
			},
		},
		DropPolicy: policypb.DropPolicy_DROP_MUST,
	}
	_, err := newMappingRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Equal(t, errStoragePoliciesAndDropPolicyInMappingRuleSnapshot, err)
}

func TestNewMappingRuleSnapshotInvalidDropPolicy(t *testing.T) {
	proto := &rulepb.MappingRuleSnapshot{
		DropPolicy: policypb.DropPolicy(-1),
	}
	_, err := newMappingRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Equal(t, errInvalidDropPolicyInMappRuleSnapshot, err)
}

func TestNewMappingRuleSnapshotFromFields(t *testing.T) {
	res, err := newMappingRuleSnapshotFromFields(
		testMappingRuleSnapshot3.name,
		testMappingRuleSnapshot3.cutoverNanos,
		testMappingRuleSnapshot3.filter,
		testMappingRuleSnapshot3.rawFilter,
		testMappingRuleSnapshot3.aggregationID,
		testMappingRuleSnapshot3.storagePolicies,
		testMappingRuleSnapshot3.dropPolicy,
		testMappingRuleSnapshot3.tags,
		testMappingRuleSnapshot3.lastUpdatedAtNanos,
		testMappingRuleSnapshot3.lastUpdatedBy,
	)
	require.NoError(t, err)
	require.True(t, cmp.Equal(testMappingRuleSnapshot3, res, testMappingRuleSnapshotCmpOpts...))
}

func TestNewMappingRuleSnapshotFromFieldsValidationError(t *testing.T) {
	badFilters := []string{
		"tag3:",
		"tag3:*a*b*c*d",
		"ab[cd",
	}

	for _, f := range badFilters {
		_, err := newMappingRuleSnapshotFromFields(
			"bar",
			12345000000,
			nil,
			f,
			aggregation.DefaultID,
			nil,
			policy.DropNone,
			nil,
			1234,
			"test_user",
		)
		require.Error(t, err)
		_, ok := err.(errors.ValidationError)
		require.True(t, ok)
	}
}

func TestMappingRuleSnapshotProto(t *testing.T) {
	snapshots := []*mappingRuleSnapshot{
		testMappingRuleSnapshot3,
		testMappingRuleSnapshot4,
	}
	expected := []*rulepb.MappingRuleSnapshot{
		testMappingRuleSnapshot3V2Proto,
		testMappingRuleSnapshot4V2Proto,
	}
	for i, snapshot := range snapshots {
		proto, err := snapshot.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], proto)
	}
}

func TestNewMappingRuleFromProtoNilProto(t *testing.T) {
	_, err := newMappingRuleFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilMappingRuleProto, err)
}

func TestNewMappingRuleFromProtoValidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.MappingRule{
		testMappingRule1V1Proto,
		testMappingRule2V2Proto,
	}
	expected := []*mappingRule{
		testMappingRule1,
		testMappingRule2,
	}
	for i, input := range inputs {
		res, err := newMappingRuleFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testMappingRuleCmpOpts...))
	}
}

func TestMappingRuleClone(t *testing.T) {
	inputs := []*mappingRule{
		testMappingRule1,
		testMappingRule2,
		testMappingRule3,
	}
	for _, input := range inputs {
		cloned := input.clone()
		require.True(t, cmp.Equal(&cloned, input, testMappingRuleCmpOpts...))

		// Asserting that modifying the clone doesn't modify the original mapping rule.
		cloned2 := input.clone()
		require.True(t, cmp.Equal(&cloned2, input, testMappingRuleCmpOpts...))
		cloned2.snapshots[0].tombstoned = true
		require.False(t, cmp.Equal(&cloned2, input, testMappingRuleCmpOpts...))
		require.True(t, cmp.Equal(&cloned, input, testMappingRuleCmpOpts...))
	}
}

func TestMappingRuleProto(t *testing.T) {
	inputs := []*mappingRule{
		testMappingRule2,
		testMappingRule3,
	}
	expected := []*rulepb.MappingRule{
		testMappingRule2V2Proto,
		testMappingRule3V2Proto,
	}
	for i, input := range inputs {
		res, err := input.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], res)
	}
}

func TestMappingRuleActiveSnapshotNotFound(t *testing.T) {
	require.Nil(t, testMappingRule2.activeSnapshot(0))
}

func TestMappingRuleActiveSnapshotFound(t *testing.T) {
	require.Equal(t, testMappingRule2.snapshots[1], testMappingRule2.activeSnapshot(100000000000))
}

func TestMappingRuleActiveRuleNotFound(t *testing.T) {
	require.Equal(t, testMappingRule2, testMappingRule2.activeRule(0))
}

func TestMappingRuleActiveRuleFound(t *testing.T) {
	expected := &mappingRule{
		uuid:      testMappingRule2.uuid,
		snapshots: testMappingRule2.snapshots[1:],
	}
	require.Equal(t, expected, testMappingRule2.activeRule(100000000000))
}

func TestMappingNameNoSnapshot(t *testing.T) {
	rr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	_, err := rr.name()
	require.Equal(t, errNoRuleSnapshots, err)
}

func TestMappingTombstonedNoSnapshot(t *testing.T) {
	rr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	require.True(t, rr.tombstoned())
}

func TestMappingTombstoned(t *testing.T) {
	require.True(t, testMappingRule2.tombstoned())
}

func TestMappingRuleMarkTombstoned(t *testing.T) {
	proto := &rulepb.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.MappingRuleSnapshot{
			testMappingRuleSnapshot3V2Proto,
		},
	}
	rr, err := newMappingRuleFromProto(proto, testTagsFilterOptions())
	require.NoError(t, err)

	meta := UpdateMetadata{
		cutoverNanos:   67890000000,
		updatedAtNanos: 10000,
		updatedBy:      "john",
	}
	require.NoError(t, rr.markTombstoned(meta))
	require.Equal(t, 2, len(rr.snapshots))
	require.True(t, cmp.Equal(testMappingRuleSnapshot3, rr.snapshots[0], testMappingRuleSnapshotCmpOpts...))

	expected := &mappingRuleSnapshot{
		name:               "foo",
		tombstoned:         true,
		cutoverNanos:       67890000000,
		rawFilter:          "tag1:value1 tag2:value2",
		lastUpdatedAtNanos: 10000,
		lastUpdatedBy:      "john",
		tags:               []models.Tag{},
	}
	require.True(t, cmp.Equal(expected, rr.snapshots[1], testMappingRuleSnapshotCmpOpts...))
}

func TestMappingRuleMarkTombstonedNoSnapshots(t *testing.T) {
	rr := &mappingRule{}
	require.Error(t, rr.markTombstoned(UpdateMetadata{}))
}

func TestMappingRuleMarkTombstonedAlreadyTombstoned(t *testing.T) {
	err := testMappingRule2.markTombstoned(UpdateMetadata{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "bar is already tombstoned"))
}

func TestMappingRuleMappingRuleView(t *testing.T) {
	res, err := testMappingRule2.mappingRuleView(1)
	require.NoError(t, err)

	expected := view.MappingRule{
		ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
		Name:          "bar",
		Tombstoned:    true,
		CutoverMillis: 67890,
		Filter:        "tag3:value3 tag4:value4",
		AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
		StoragePolicies: policy.StoragePolicies{
			policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
		},
		LastUpdatedAtMillis: 67890,
		LastUpdatedBy:       "someone-else",
		DropPolicy:          res.DropPolicy,
		Tags:                []models.Tag{},
	}
	require.Equal(t, expected, res)
}

func TestNewMappingRuleViewError(t *testing.T) {
	badIndices := []int{-2, 2, 30}
	for _, i := range badIndices {
		_, err := testMappingRule2.mappingRuleView(i)
		require.Equal(t, errMappingRuleSnapshotIndexOutOfRange, err)
	}
}

func TestNewMappingRuleHistory(t *testing.T) {
	history, err := testMappingRule2.history()
	require.NoError(t, err)

	expected := []view.MappingRule{
		{
			ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:          "bar",
			Tombstoned:    true,
			CutoverMillis: 67890,
			Filter:        "tag3:value3 tag4:value4",
			AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
			},
			LastUpdatedAtMillis: 67890,
			LastUpdatedBy:       "someone-else",
			Tags:                []models.Tag{},
		},
		{
			ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:          "foo",
			Tombstoned:    false,
			CutoverMillis: 12345,
			Filter:        "tag1:value1 tag2:value2",
			AggregationID: aggregation.DefaultID,
			StoragePolicies: policy.StoragePolicies{
				policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
				policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
			},
			LastUpdatedAtMillis: 12345,
			LastUpdatedBy:       "someone",
			Tags:                []models.Tag{},
		},
	}
	require.Equal(t, expected, history)
}
