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

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/aggregationpb"
	"github.com/m3db/m3metrics/generated/proto/pipelinepb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/generated/proto/rulepb"
	"github.com/m3db/m3metrics/generated/proto/transformationpb"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	"github.com/m3db/m3metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testRollupRuleSnapshot1V1Proto = &rulepb.RollupRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       12345,
		LastUpdatedAtNanos: 12345,
		LastUpdatedBy:      "someone",
		Filter:             "tag1:value1 tag2:value2",
		Targets: []*rulepb.RollupTarget{
			&rulepb.RollupTarget{
				Name: "rName1",
				Tags: []string{"rtagName1", "rtagName2"},
				Policies: []*policypb.Policy{
					&policypb.Policy{
						StoragePolicy: &policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(10 * time.Second),
								Precision:  int64(time.Second),
							},
							Retention: &policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
					},
				},
			},
		},
	}
	testRollupRuleSnapshot2V1Proto = &rulepb.RollupRuleSnapshot{
		Name:               "bar",
		Tombstoned:         true,
		CutoverNanos:       67890,
		LastUpdatedAtNanos: 67890,
		LastUpdatedBy:      "someone-else",
		Filter:             "tag3:value3 tag4:value4",
		Targets: []*rulepb.RollupTarget{
			&rulepb.RollupTarget{
				Name: "rName1",
				Tags: []string{"rtagName1", "rtagName2"},
				Policies: []*policypb.Policy{
					&policypb.Policy{
						StoragePolicy: &policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(24 * time.Hour),
							},
						},
						AggregationTypes: []aggregationpb.AggregationType{
							aggregationpb.AggregationType_MEAN,
						},
					},
					&policypb.Policy{
						StoragePolicy: &policypb.StoragePolicy{
							Resolution: &policypb.Resolution{
								WindowSize: int64(5 * time.Minute),
								Precision:  int64(time.Minute),
							},
							Retention: &policypb.Retention{
								Period: int64(48 * time.Hour),
							},
						},
						AggregationTypes: []aggregationpb.AggregationType{
							aggregationpb.AggregationType_MEAN,
						},
					},
				},
			},
		},
	}
	testRollupRuleSnapshot3V2Proto = &rulepb.RollupRuleSnapshot{
		Name:               "foo",
		Tombstoned:         false,
		CutoverNanos:       12345,
		LastUpdatedAtNanos: 12345,
		LastUpdatedBy:      "someone",
		Filter:             "tag1:value1 tag2:value2",
		TargetsV2: []*rulepb.RollupTargetV2{
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
						Resolution: &policypb.Resolution{
							WindowSize: 10 * time.Second.Nanoseconds(),
							Precision:  time.Second.Nanoseconds(),
						},
						Retention: &policypb.Retention{
							Period: 24 * time.Hour.Nanoseconds(),
						},
					},
					&policypb.StoragePolicy{
						Resolution: &policypb.Resolution{
							WindowSize: time.Minute.Nanoseconds(),
							Precision:  time.Minute.Nanoseconds(),
						},
						Retention: &policypb.Retention{
							Period: 720 * time.Hour.Nanoseconds(),
						},
					},
					&policypb.StoragePolicy{
						Resolution: &policypb.Resolution{
							WindowSize: time.Hour.Nanoseconds(),
							Precision:  time.Hour.Nanoseconds(),
						},
						Retention: &policypb.Retention{
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
						Resolution: &policypb.Resolution{
							WindowSize: time.Minute.Nanoseconds(),
							Precision:  time.Minute.Nanoseconds(),
						},
						Retention: &policypb.Retention{
							Period: 720 * time.Hour.Nanoseconds(),
						},
					},
				},
			},
		},
	}
	testRollupRuleSnapshot4V2Proto = &rulepb.RollupRuleSnapshot{
		Name:               "bar",
		Tombstoned:         true,
		CutoverNanos:       67890,
		LastUpdatedAtNanos: 67890,
		LastUpdatedBy:      "someone-else",
		Filter:             "tag3:value3 tag4:value4",
		TargetsV2: []*rulepb.RollupTargetV2{
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
						Resolution: &policypb.Resolution{
							WindowSize: 10 * time.Minute.Nanoseconds(),
							Precision:  time.Minute.Nanoseconds(),
						},
						Retention: &policypb.Retention{
							Period: 1800 * time.Hour.Nanoseconds(),
						},
					},
				},
			},
		},
	}
	testRollupRule1V1Proto = &rulepb.RollupRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.RollupRuleSnapshot{
			testRollupRuleSnapshot1V1Proto,
			testRollupRuleSnapshot2V1Proto,
		},
	}
	testRollupRule2V2Proto = &rulepb.RollupRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.RollupRuleSnapshot{
			testRollupRuleSnapshot3V2Proto,
			testRollupRuleSnapshot4V2Proto,
		},
	}
	testRollupRuleSnapshot1 = &rollupRuleSnapshot{
		name:         "foo",
		tombstoned:   false,
		cutoverNanos: 12345,
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
		lastUpdatedAtNanos: 12345,
		lastUpdatedBy:      "someone",
	}
	testRollupRuleSnapshot2 = &rollupRuleSnapshot{
		name:         "bar",
		tombstoned:   true,
		cutoverNanos: 67890,
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
		lastUpdatedAtNanos: 67890,
		lastUpdatedBy:      "someone-else",
	}
	testRollupRuleSnapshot3 = &rollupRuleSnapshot{
		name:         "foo",
		tombstoned:   false,
		cutoverNanos: 12345,
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
		lastUpdatedAtNanos: 12345,
		lastUpdatedBy:      "someone",
	}
	testRollupRuleSnapshot4 = &rollupRuleSnapshot{
		name:         "bar",
		tombstoned:   true,
		cutoverNanos: 67890,
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
		lastUpdatedAtNanos: 67890,
		lastUpdatedBy:      "someone-else",
	}
	testRollupRule1 = &rollupRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*rollupRuleSnapshot{
			testRollupRuleSnapshot1,
			testRollupRuleSnapshot2,
		},
	}
	testRollupRule2 = &rollupRule{
		uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		snapshots: []*rollupRuleSnapshot{
			testRollupRuleSnapshot3,
			testRollupRuleSnapshot4,
		},
	}
	testRollupRuleSnapshotCmpOpts = []cmp.Option{
		cmp.AllowUnexported(rollupRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
	testRollupRuleCmpOpts = []cmp.Option{
		cmp.AllowUnexported(rollupRule{}),
		cmp.AllowUnexported(rollupRuleSnapshot{}),
		cmpopts.IgnoreInterfaces(struct{ filters.Filter }{}),
	}
)

func TestNewRollupRuleSnapshotFromProtoNilProto(t *testing.T) {
	_, err := newRollupRuleSnapshotFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilRollupRuleSnapshotProto, err)
}

func TestNewRollupRuleSnapshotFromV1ProtoInvalidProto(t *testing.T) {
	proto := &rulepb.RollupRuleSnapshot{
		Targets: []*rulepb.RollupTarget{
			&rulepb.RollupTarget{
				Name: "rName1",
				Tags: []string{"rtagName1", "rtagName2"},
				Policies: []*policypb.Policy{
					&policypb.Policy{},
				},
			},
		},
	}
	_, err := newRollupRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Error(t, err)
}

func TestNewRollupRuleSnapshotFromV1Proto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.RollupRuleSnapshot{
		testRollupRuleSnapshot1V1Proto,
		testRollupRuleSnapshot2V1Proto,
	}
	expected := []*rollupRuleSnapshot{
		testRollupRuleSnapshot1,
		testRollupRuleSnapshot2,
	}
	for i, input := range inputs {
		res, err := newRollupRuleSnapshotFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testRollupRuleSnapshotCmpOpts...))
		require.NotNil(t, res.filter)
	}
}

func TestNewRollupRuleSnapshotFromV2ProtoInvalidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	proto := &rulepb.RollupRuleSnapshot{
		TargetsV2: []*rulepb.RollupTargetV2{
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
						Resolution: &policypb.Resolution{
							WindowSize: 10 * time.Minute.Nanoseconds(),
							Precision:  time.Minute.Nanoseconds(),
						},
						Retention: &policypb.Retention{
							Period: 1800 * time.Hour.Nanoseconds(),
						},
					},
				},
			},
		},
	}
	_, err := newRollupRuleSnapshotFromProto(proto, filterOpts)
	require.Error(t, err)
}

func TestNewRollupRuleSnapshotFromV2Proto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.RollupRuleSnapshot{
		testRollupRuleSnapshot3V2Proto,
		testRollupRuleSnapshot4V2Proto,
	}
	expected := []*rollupRuleSnapshot{
		testRollupRuleSnapshot3,
		testRollupRuleSnapshot4,
	}
	for i, input := range inputs {
		res, err := newRollupRuleSnapshotFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testRollupRuleSnapshotCmpOpts...))
		require.NotNil(t, res.filter)
	}
}

func TestNewRollupRuleSnapshotNoRollupTargets(t *testing.T) {
	proto := &rulepb.RollupRuleSnapshot{}
	_, err := newRollupRuleSnapshotFromProto(proto, testTagsFilterOptions())
	require.Equal(t, errNoRollupTargetsInRollupRuleSnapshot, err)
}

func TestNewRollupRuleSnapshotFromFields(t *testing.T) {
	res, err := newRollupRuleSnapshotFromFields(
		testRollupRuleSnapshot3.name,
		testRollupRuleSnapshot3.cutoverNanos,
		testRollupRuleSnapshot3.rawFilter,
		testRollupRuleSnapshot3.targets,
		testRollupRuleSnapshot3.filter,
		testRollupRuleSnapshot3.lastUpdatedAtNanos,
		testRollupRuleSnapshot3.lastUpdatedBy,
	)
	require.NoError(t, err)
	require.True(t, cmp.Equal(testRollupRuleSnapshot3, res, testRollupRuleSnapshotCmpOpts...))
}

func TestNewRollupRuleSnapshotFromFieldsValidationError(t *testing.T) {
	badFilters := []string{
		"tag3:",
		"tag3:*a*b*c*d",
		"ab[cd",
	}

	for _, f := range badFilters {
		_, err := newRollupRuleSnapshotFromFields(
			"bar",
			12345,
			f,
			nil,
			nil,
			1234,
			"test_user",
		)
		require.Error(t, err)
		_, ok := err.(errors.ValidationError)
		require.True(t, ok)
	}
}

func TestRollupRuleSnapshotProto(t *testing.T) {
	snapshots := []*rollupRuleSnapshot{
		testRollupRuleSnapshot3,
		testRollupRuleSnapshot4,
	}
	expected := []*rulepb.RollupRuleSnapshot{
		testRollupRuleSnapshot3V2Proto,
		testRollupRuleSnapshot4V2Proto,
	}
	for i, snapshot := range snapshots {
		proto, err := snapshot.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], proto)
	}
}

func TestNewRollupRuleFromProtoNilProto(t *testing.T) {
	_, err := newRollupRuleFromProto(nil, testTagsFilterOptions())
	require.Equal(t, errNilRollupRuleProto, err)
}

func TestNewRollupRuleFromProtoValidProto(t *testing.T) {
	filterOpts := testTagsFilterOptions()
	inputs := []*rulepb.RollupRule{
		testRollupRule1V1Proto,
		testRollupRule2V2Proto,
	}
	expected := []*rollupRule{
		testRollupRule1,
		testRollupRule2,
	}
	for i, input := range inputs {
		res, err := newRollupRuleFromProto(input, filterOpts)
		require.NoError(t, err)
		require.True(t, cmp.Equal(expected[i], res, testRollupRuleCmpOpts...))
	}
}

func TestRollupRuleClone(t *testing.T) {
	inputs := []*rollupRule{
		testRollupRule1,
		testRollupRule2,
	}
	for _, input := range inputs {
		cloned := input.clone()
		require.True(t, cmp.Equal(&cloned, input, testRollupRuleCmpOpts...))

		// Asserting that modifying the clone doesn't modify the original rollup rule.
		cloned2 := input.clone()
		require.True(t, cmp.Equal(&cloned2, input, testRollupRuleCmpOpts...))
		cloned2.snapshots[0].tombstoned = true
		require.False(t, cmp.Equal(&cloned2, input, testRollupRuleCmpOpts...))
		require.True(t, cmp.Equal(&cloned, input, testRollupRuleCmpOpts...))
	}
}

func TestRollupRuleProto(t *testing.T) {
	inputs := []*rollupRule{
		testRollupRule2,
	}
	expected := []*rulepb.RollupRule{
		testRollupRule2V2Proto,
	}
	for i, input := range inputs {
		res, err := input.proto()
		require.NoError(t, err)
		require.Equal(t, expected[i], res)
	}
}

func TestRollupRuleActiveSnapshotNotFound(t *testing.T) {
	require.Nil(t, testRollupRule2.activeSnapshot(0))
}

func TestRollupRuleActiveSnapshotFound(t *testing.T) {
	require.Equal(t, testRollupRule2.snapshots[1], testRollupRule2.activeSnapshot(100000))
}

func TestRollupRuleActiveRuleNotFound(t *testing.T) {
	require.Equal(t, testRollupRule2, testRollupRule2.activeRule(0))
}

func TestRollupRuleActiveRuleFound(t *testing.T) {
	expected := &rollupRule{
		uuid:      testRollupRule2.uuid,
		snapshots: testRollupRule2.snapshots[1:],
	}
	require.Equal(t, expected, testRollupRule2.activeRule(100000))
}

func TestRollupNameNoSnapshot(t *testing.T) {
	rr := rollupRule{
		uuid:      "blah",
		snapshots: []*rollupRuleSnapshot{},
	}
	_, err := rr.name()
	require.Equal(t, errNoRuleSnapshots, err)
}

func TestRollupTombstonedNoSnapshot(t *testing.T) {
	rr := rollupRule{
		uuid:      "blah",
		snapshots: []*rollupRuleSnapshot{},
	}
	require.True(t, rr.tombstoned())
}

func TestRollupTombstoned(t *testing.T) {
	require.True(t, testRollupRule2.tombstoned())
}

func TestRollupRuleMarkTombstoned(t *testing.T) {
	proto := &rulepb.RollupRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*rulepb.RollupRuleSnapshot{
			testRollupRuleSnapshot3V2Proto,
		},
	}
	rr, err := newRollupRuleFromProto(proto, testTagsFilterOptions())
	require.NoError(t, err)

	meta := UpdateMetadata{
		cutoverNanos:   67890,
		updatedAtNanos: 10000,
		updatedBy:      "john",
	}
	require.NoError(t, rr.markTombstoned(meta))
	require.Equal(t, 2, len(rr.snapshots))
	require.True(t, cmp.Equal(testRollupRuleSnapshot3, rr.snapshots[0], testRollupRuleSnapshotCmpOpts...))

	expected := &rollupRuleSnapshot{
		name:               "foo",
		tombstoned:         true,
		cutoverNanos:       67890,
		rawFilter:          "tag1:value1 tag2:value2",
		lastUpdatedAtNanos: 10000,
		lastUpdatedBy:      "john",
	}
	require.True(t, cmp.Equal(expected, rr.snapshots[1], testRollupRuleSnapshotCmpOpts...))
}

func TestRollupRuleMarkTombstonedNoSnapshots(t *testing.T) {
	rr := &rollupRule{}
	require.Error(t, rr.markTombstoned(UpdateMetadata{}))
}

func TestRollupRuleMarkTombstonedAlreadyTombstoned(t *testing.T) {
	err := testRollupRule2.markTombstoned(UpdateMetadata{})
	require.Error(t, err)
	require.True(t, strings.Contains(err.Error(), "bar is already tombstoned"))
}

func TestRollupRuleRollupRuleView(t *testing.T) {
	res, err := testRollupRule2.rollupRuleView(1)
	require.NoError(t, err)

	expected := &models.RollupRuleView{
		ID:           "12669817-13ae-40e6-ba2f-33087b262c68",
		Name:         "bar",
		Tombstoned:   true,
		CutoverNanos: 67890,
		Filter:       "tag3:value3 tag4:value4",
		Targets: []models.RollupTargetView{
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
		LastUpdatedAtNanos: 67890,
		LastUpdatedBy:      "someone-else",
	}
	require.Equal(t, expected, res)
}

func TestNewRollupRuleViewError(t *testing.T) {
	badIndices := []int{-2, 2, 30}
	for _, i := range badIndices {
		res, err := testRollupRule2.rollupRuleView(i)
		require.Equal(t, errRollupRuleSnapshotIndexOutOfRange, err)
		require.Nil(t, res)
	}
}

func TestNewRollupRuleHistory(t *testing.T) {
	history, err := testRollupRule2.history()
	require.NoError(t, err)

	expected := []*models.RollupRuleView{
		&models.RollupRuleView{
			ID:           "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:         "bar",
			Tombstoned:   true,
			CutoverNanos: 67890,
			Filter:       "tag3:value3 tag4:value4",
			Targets: []models.RollupTargetView{
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
			LastUpdatedAtNanos: 67890,
			LastUpdatedBy:      "someone-else",
		},
		&models.RollupRuleView{
			ID:           "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:         "foo",
			Tombstoned:   false,
			CutoverNanos: 12345,
			Filter:       "tag1:value1 tag2:value2",
			Targets: []models.RollupTargetView{
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
			LastUpdatedAtNanos: 12345,
			LastUpdatedBy:      "someone",
		},
	}
	require.Equal(t, expected, history)
}
