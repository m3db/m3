// Copyright (c) 2021 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/generated/proto/rulepb"
	"github.com/m3db/m3/src/metrics/generated/proto/transformationpb"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testUtilizationRuleSnapshotProto1 = &rulepb.UtilizationRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       12345000000,
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
		Filter:             "tag1:value1 tag2:value2",
		Targets: []*rulepb.RollupTargetV2{
			&rulepb.RollupTargetV2{
				Pipeline: &pipelinepb.Pipeline{
					Ops: []pipelinepb.PipelineOp{
						{
							Type: pipelinepb.PipelineOp_AGGREGATION,
							Aggregation: &pipelinepb.AggregationOp{
								Type: aggregationpb.AggregationType_SUM,
							},
						},
						{
							Type: pipelinepb.PipelineOp_TRANSFORMATION,
							Transformation: &pipelinepb.TransformationOp{
								Type: transformationpb.TransformationType_ABSOLUTE,
							},
						},
						{
							Type: pipelinepb.PipelineOp_ROLLUP,
							Rollup: &pipelinepb.RollupOp{
								NewName: "testRollupOp",
								Tags:    []string{"testTag1", "testTag2"},
								AggregationTypes: []aggregationpb.AggregationType{
									aggregationpb.AggregationType_MIN,
									aggregationpb.AggregationType_MAX,
								},
							},
						},
					},
				},
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
			},
			&rulepb.RollupTargetV2{
				Pipeline: &pipelinepb.Pipeline{
					Ops: []pipelinepb.PipelineOp{
						{
							Type: pipelinepb.PipelineOp_TRANSFORMATION,
							Transformation: &pipelinepb.TransformationOp{
								Type: transformationpb.TransformationType_PERSECOND,
							},
						},
						{
							Type: pipelinepb.PipelineOp_ROLLUP,
							Rollup: &pipelinepb.RollupOp{
								NewName: "testRollupOp2",
								Tags:    []string{"testTag3", "testTag4"},
							},
						},
					},
				},
				StoragePolicies: []*policypb.StoragePolicy{
					&policypb.StoragePolicy{
						Resolution: policypb.Resolution{
							WindowSize: time.Minute.Nanoseconds(),
							Precision:  time.Minute.Nanoseconds(),
						},
						Retention: policypb.Retention{
							Period: 720 * time.Hour.Nanoseconds(),
						},
					},
				},
			},
		},
		Value: "value",
	}
	testUtilizationRuleSnapshotProto2 = &rulepb.UtilizationRuleSnapshot{
		Name:               "bar",
		Tombstoned:         true,
		CutoverNanos:       67890000000,
		LastUpdatedAtNanos: 67890000000,
		LastUpdatedBy:      "someone-else",
		Filter:             "tag3:value3 tag4:value4",
		Targets: []*rulepb.RollupTargetV2{
			&rulepb.RollupTargetV2{
				Pipeline: &pipelinepb.Pipeline{
					Ops: []pipelinepb.PipelineOp{
						{
							Type: pipelinepb.PipelineOp_ROLLUP,
							Rollup: &pipelinepb.RollupOp{
								NewName: "testRollupOp2",
								Tags:    []string{"testTag3", "testTag4"},
								AggregationTypes: []aggregationpb.AggregationType{
									aggregationpb.AggregationType_LAST,
								},
							},
						},
					},
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
			},
		},
		Value: "value2",
	}
	testUtilizationRuleProto1 = &rulepb.UtilizationRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.UtilizationRuleSnapshot{
			testUtilizationRuleSnapshotProto1,
			testUtilizationRuleSnapshotProto2,
		},
	}
	testUtilizationRuleProto2 = &rulepb.UtilizationRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.UtilizationRuleSnapshot{
			testUtilizationRuleSnapshotProto1,
			testUtilizationRuleSnapshotProto2,
		},
	}
	testUtilizationRuleSnapshot1 = &utilizationRuleSnapshot{
		name:         "foo",
		tombstoned:   false,
		cutoverNanos: 12345000000,
		rawFilter:    "tag1:value1 tag2:value2",
		targets: []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("rName1"),
							Tags:          bs("rtagName1", "rtagName2"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
				},
			},
		},
		value:              "value",
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
	}
	testUtilizationRuleSnapshot2 = &utilizationRuleSnapshot{
		name:         "bar",
		tombstoned:   true,
		cutoverNanos: 67890000000,
		rawFilter:    "tag3:value3 tag4:value4",
		targets: []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("rName1"),
							Tags:          bs("rtagName1", "rtagName2"),
							AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
		},
		value:              "value2",
		lastUpdatedAtNanos: 67890000000,
		lastUpdatedBy:      "someone-else",
	}
	testUtilizationRuleSnapshot3 = &utilizationRuleSnapshot{
		name:         "foo",
		tombstoned:   false,
		cutoverNanos: 12345000000,
		rawFilter:    "tag1:value1 tag2:value2",
		targets: []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.AggregationOpType,
						Aggregation: pipeline.AggregationOp{
							Type: aggregation.Sum,
						},
					},
					{
						Type: pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{
							Type: transformation.Absolute,
						},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("testRollupOp"),
							Tags:          bs("testTag1", "testTag2"),
							AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
					policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
				},
			},
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.TransformationOpType,
						Transformation: pipeline.TransformationOp{
							Type: transformation.PerSecond,
						},
					},
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("testRollupOp2"),
							Tags:          bs("testTag3", "testTag4"),
							AggregationID: aggregation.DefaultID,
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
				},
			},
		},
		value:              "value",
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
	}
	testUtilizationRuleSnapshot4 = &utilizationRuleSnapshot{
		name:         "bar",
		tombstoned:   true,
		cutoverNanos: 67890000000,
		rawFilter:    "tag3:value3 tag4:value4",
		targets: []rollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("testRollupOp2"),
							Tags:          bs("testTag3", "testTag4"),
							AggregationID: aggregation.MustCompressTypes(aggregation.Last),
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
				},
			},
		},
		value:              "value2",
		lastUpdatedAtNanos: 67890000000,
		lastUpdatedBy:      "someone-else",
	}
	testUtilizationRule1 = &utilizationRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*utilizationRuleSnapshot{
			testUtilizationRuleSnapshot1,
			testUtilizationRuleSnapshot2,
		},
	}
	testUtilizationRule2 = &utilizationRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*utilizationRuleSnapshot{
			testUtilizationRuleSnapshot3,
			testUtilizationRuleSnapshot4,
		},
	}
	testUtilizationRuleSnapshotCmpOpts = []cmp.Option{
		cmp.AllowUnexported(utilizationRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
	testUtilizationRuleCmpOpts = []cmp.Option{
		cmp.AllowUnexported(utilizationRule{}),
		cmp.AllowUnexported(utilizationRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
)

func TestNewUtilizationRuleSnapshotFromProtoNilProto(t *testing.T) {
	_, err := newUtilizationRuleSnapshotFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilUtilizationRuleSnapshotProto, err)
}

func TestNewUtilizationRuleSnapshotFromProtoInvalidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	proto := &rulepb.UtilizationRuleSnapshot{
		Targets: []*rulepb.RollupTargetV2{
			&rulepb.RollupTargetV2{
				Pipeline: &pipelinepb.Pipeline{
					Ops: []pipelinepb.PipelineOp{
						{
							Type: pipelinepb.PipelineOp_TRANSFORMATION,
							Transformation: &pipelinepb.TransformationOp{
								Type: transformationpb.TransformationType_UNKNOWN,
							},
						},
					},
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
			},
		},
	}
	_, err := newUtilizationRuleSnapshotFromProto(proto, filterOpts)
	require.Error(t, err)
}

func TestNewUtilizationRuleSnapshotFromProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.UtilizationRuleSnapshot{
		testUtilizationRuleSnapshotProto1,
		testUtilizationRuleSnapshotProto2,
	}
	expected := []*utilizationRuleSnapshot{
		testUtilizationRuleSnapshot3,
		testUtilizationRuleSnapshot4,
	}
	for i, input := range inputs {
		res, err := newUtilizationRuleSnapshotFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testUtilizationRuleSnapshotCmpOpts...))
		require.NotNil(t, res.filter)
	}
}

func TestNewUtilizationRuleSnapshotFromProtoTombstoned(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	input := &rulepb.UtilizationRuleSnapshot{
		Name:               "foo",
		Tombstoned:         true,
		CutoverNanos:       12345000000,
		LastUpdatedAtNanos: 12345000000,
		LastUpdatedBy:      "someone",
		Filter:             "tag1:value1 tag2:value2",
	}
	res, err := newUtilizationRuleSnapshotFromProto(input, filterOpts)
	require.NoError(t, err)

	expected := &utilizationRuleSnapshot{
		name:               "foo",
		tombstoned:         true,
		cutoverNanos:       12345000000,
		rawFilter:          "tag1:value1 tag2:value2",
		lastUpdatedAtNanos: 12345000000,
		lastUpdatedBy:      "someone",
	}
	require.True(t, cmp.Equal(expected, res, testUtilizationRuleSnapshotCmpOpts...))
	require.NotNil(t, res.filter)
}

func TestNewUtilizationRuleSnapshotNoRollupTargets(t *testing.T) {
	proto := &rulepb.UtilizationRuleSnapshot{}
	_, err := newUtilizationRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Equal(t, errNoRollupTargetsInUtilizationRuleSnapshot, err)
}

func TestNewUtilizationRuleSnapshotFromFields(t *testing.T) {
	res, err := newUtilizationRuleSnapshotFromFields(
		testUtilizationRuleSnapshot3.name,
		testUtilizationRuleSnapshot3.cutoverNanos,
		testUtilizationRuleSnapshot3.rawFilter,
		testUtilizationRuleSnapshot3.targets,
		testUtilizationRuleSnapshot3.filter,
		testUtilizationRuleSnapshot3.value,
		testUtilizationRuleSnapshot3.lastUpdatedAtNanos,
		testUtilizationRuleSnapshot3.lastUpdatedBy,
	)
	require.NoError(t, err)
	require.True(t, cmp.Equal(testUtilizationRuleSnapshot3, res, testUtilizationRuleSnapshotCmpOpts...))
}

func TestNewUtilizationRuleSnapshotFromFieldsValidationError(t *testing.T) {
	badFilters := []string{
		"tag3:",
		"tag3:*a*b*c*d",
		"ab[cd",
	}

	for _, f := range badFilters {
		_, err := newUtilizationRuleSnapshotFromFields(
			"bar",
			12345000000,
			f,
			nil,
			nil,
			"value",
			1234,
			"test_user",
		)
		require.Error(t, err)
		_, ok := err.(errors.ValidationError)
		require.True(t, ok)
	}
}

func TestUtilizationRuleSnapshotProto(t *testing.T) {
	snapshots := []*utilizationRuleSnapshot{
		testUtilizationRuleSnapshot3,
		testUtilizationRuleSnapshot4,
	}
	expected := []*rulepb.UtilizationRuleSnapshot{
		testUtilizationRuleSnapshotProto1,
		testUtilizationRuleSnapshotProto2,
	}
	for i, snapshot := range snapshots {
		proto, err := snapshot.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], proto)
	}
}

func TestNewUtilizationRuleFromProtoNilProto(t *testing.T) {
	_, err := newUtilizationRuleFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilUtilizationRuleProto, err)
}

func TestNewUtilizationRuleFromProtoValidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.UtilizationRule{
		testUtilizationRuleProto1,
		testUtilizationRuleProto2,
	}
	expected := []*utilizationRule{
		testUtilizationRule1,
		testUtilizationRule2,
	}
	for i, input := range inputs {
		res, err := newUtilizationRuleFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testUtilizationRuleCmpOpts...))
	}
}

func TestUtilizationRuleClone(t *testing.T) {
	inputs := []*utilizationRule{
		testUtilizationRule1,
		testUtilizationRule2,
	}
	for _, input := range inputs {
		cloned := input.clone()
		require.True(t, cmp.Equal(&cloned, input, testUtilizationRuleCmpOpts...))

		// Asserting that modifying the clone doesn't modify the original utilization rule.
		cloned2 := input.clone()
		require.True(t, cmp.Equal(&cloned2, input, testUtilizationRuleCmpOpts...))
		cloned2.snapshots[0].tombstoned = true
		require.False(t, cmp.Equal(&cloned2, input, testUtilizationRuleCmpOpts...))
		require.True(t, cmp.Equal(&cloned, input, testUtilizationRuleCmpOpts...))
	}
}

func TestUtilizationRuleProto(t *testing.T) {
	inputs := []*utilizationRule{
		testUtilizationRule2,
	}
	expected := []*rulepb.UtilizationRule{
		testUtilizationRuleProto2,
	}
	for i, input := range inputs {
		res, err := input.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], res)
	}
}

func TestUtilizationRuleActiveSnapshotNotFound(t *testing.T) {
	require.Nil(t, testUtilizationRule2.activeSnapshot(0))
}

func TestUtilizationRuleActiveSnapshotFound(t *testing.T) {
	require.Equal(t, testUtilizationRule2.snapshots[1], testUtilizationRule2.activeSnapshot(100000000000))
}

func TestUtilizationRuleActiveRuleNotFound(t *testing.T) {
	require.Equal(t, testUtilizationRule2, testUtilizationRule2.activeRule(0))
}

func TestUtilizationRuleActiveRuleFound(t *testing.T) {
	expected := &utilizationRule{
		uuid:      testUtilizationRule2.uuid,
		snapshots: testUtilizationRule2.snapshots[1:],
	}
	require.Equal(t, expected, testUtilizationRule2.activeRule(100000000000))
}

func TestUtilizationNameNoSnapshot(t *testing.T) {
	rr := utilizationRule{
		uuid:      "blah",
		snapshots: []*utilizationRuleSnapshot{},
	}
	_, err := rr.name()
	require.Equal(t, errNoRuleSnapshots, err)
}

func TestUtilizationTombstonedNoSnapshot(t *testing.T) {
	rr := utilizationRule{
		uuid:      "blah",
		snapshots: []*utilizationRuleSnapshot{},
	}
	require.True(t, rr.tombstoned())
}

func TestUtilizationTombstoned(t *testing.T) {
	require.True(t, testUtilizationRule2.tombstoned())
}

func TestUtilizationRuleMarkTombstoned(t *testing.T) {
	proto := &rulepb.UtilizationRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.UtilizationRuleSnapshot{
			testUtilizationRuleSnapshotProto1,
		},
	}
	rr, err := newUtilizationRuleFromProto(proto, testTagsFilterOptions())
	require.NoError(t, err)

	meta := UpdateMetadata{
		cutoverNanos:   67890000000,
		updatedAtNanos: 10000,
		updatedBy:      "john",
	}
	require.NoError(t, rr.markTombstoned(meta))
	require.Equal(t, 2, len(rr.snapshots))
	require.True(t, cmp.Equal(testUtilizationRuleSnapshot3, rr.snapshots[0], testUtilizationRuleSnapshotCmpOpts...))

	expected := &utilizationRuleSnapshot{
		name:               "foo",
		tombstoned:         true,
		cutoverNanos:       67890000000,
		rawFilter:          "tag1:value1 tag2:value2",
		lastUpdatedAtNanos: 10000,
		value:              "value",
		lastUpdatedBy:      "john",
	}
	require.True(t, cmp.Equal(expected, rr.snapshots[1], testUtilizationRuleSnapshotCmpOpts...))
}

func TestUtilizationRuleMarkTombstonedNoSnapshots(t *testing.T) {
	rr := &utilizationRule{}
	require.Error(t, rr.markTombstoned(UpdateMetadata{}))
}

func TestUtilizationRuleMarkTombstonedAlreadyTombstoned(t *testing.T) {
	err := testUtilizationRule2.markTombstoned(UpdateMetadata{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "bar is already tombstoned"))
}

func TestUtilizationRuleUtilizationRuleView(t *testing.T) {
	res, err := testUtilizationRule2.utilizationRuleView(1)
	require.NoError(t, err)

	expected := view.UtilizationRule{
		ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
		Name:          "bar",
		Tombstoned:    true,
		CutoverMillis: 67890,
		Value:         "value",
		Filter:        "tag3:value3 tag4:value4",
		Targets: []view.RollupTarget{
			{
				Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
					{
						Type: pipeline.RollupOpType,
						Rollup: pipeline.RollupOp{
							NewName:       []byte("testRollupOp2"),
							Tags:          bs("testTag3", "testTag4"),
							AggregationID: aggregation.MustCompressTypes(aggregation.Last),
						},
					},
				}),
				StoragePolicies: policy.StoragePolicies{
					policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
				},
			},
		},
		LastUpdatedAtMillis: 67890,
		LastUpdatedBy:       "someone-else",
	}
	require.Equal(t, expected, res)
}

func TestNewUtilizationRuleViewError(t *testing.T) {
	badIndices := []int{-2, 2, 30}
	for _, i := range badIndices {
		_, err := testUtilizationRule2.utilizationRuleView(i)
		require.Equal(t, errUtilizationRuleSnapshotIndexOutOfRange, err)
	}
}

func TestNewUtilizationRuleHistory(t *testing.T) {
	history, err := testUtilizationRule2.history()
	require.NoError(t, err)

	expected := []view.UtilizationRule{
		{
			ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:          "bar",
			Tombstoned:    true,
			CutoverMillis: 67890,
			Filter:        "tag3:value3 tag4:value4",
			Value:         "value",
			Targets: []view.RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("testRollupOp2"),
								Tags:          bs("testTag3", "testTag4"),
								AggregationID: aggregation.MustCompressTypes(aggregation.Last),
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 1800*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 67890,
			LastUpdatedBy:       "someone-else",
		},
		{
			ID:            "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:          "foo",
			Tombstoned:    false,
			CutoverMillis: 12345,
			Filter:        "tag1:value1 tag2:value2",
			Targets: []view.RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type: pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{
								Type: aggregation.Sum,
							},
						},
						{
							Type: pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{
								Type: transformation.Absolute,
							},
						},
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("testRollupOp"),
								Tags:          bs("testTag1", "testTag2"),
								AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour),
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
						policy.NewStoragePolicy(time.Hour, xtime.Hour, 365*24*time.Hour),
					},
				},
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type: pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{
								Type: transformation.PerSecond,
							},
						},
						{
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       []byte("testRollupOp2"),
								Tags:          bs("testTag3", "testTag4"),
								AggregationID: aggregation.DefaultID,
							},
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.NewStoragePolicy(time.Minute, xtime.Minute, 720*time.Hour),
					},
				},
			},
			LastUpdatedAtMillis: 12345,
			LastUpdatedBy:       "someone",
		},
	}
	require.Equal(t, expected, history)
}
