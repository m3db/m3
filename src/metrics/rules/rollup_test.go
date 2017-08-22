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
	"testing"
	"time"

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testRollupRuleSchema = &schema.RollupRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*schema.RollupRuleSnapshot{
			&schema.RollupRuleSnapshot{
				Name:        "foo",
				Tombstoned:  false,
				CutoverTime: 12345,
				TagFilters: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
				Targets: []*schema.RollupTarget{
					&schema.RollupTarget{
						Name: "rName1",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(10 * time.Second),
										Precision:  int64(time.Second),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
						},
					},
				},
			},
			&schema.RollupRuleSnapshot{
				Name:        "bar",
				Tombstoned:  true,
				CutoverTime: 67890,
				TagFilters: map[string]string{
					"tag3": "value3",
					"tag4": "value4",
				},
				Targets: []*schema.RollupTarget{
					&schema.RollupTarget{
						Name: "rName1",
						Tags: []string{"rtagName1", "rtagName2"},
						Policies: []*schema.Policy{
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &schema.Retention{
										Period: int64(24 * time.Hour),
									},
								},
							},
							&schema.Policy{
								StoragePolicy: &schema.StoragePolicy{
									Resolution: &schema.Resolution{
										WindowSize: int64(5 * time.Minute),
										Precision:  int64(time.Minute),
									},
									Retention: &schema.Retention{
										Period: int64(48 * time.Hour),
									},
								},
								AggregationTypes: []schema.AggregationType{
									schema.AggregationType_MEAN,
								},
							},
						},
					},
				},
			},
		},
	}
)

func TestNewRollupTargetNilTargetSchema(t *testing.T) {
	_, err := newRollupTarget(nil)
	require.Equal(t, errNilRollupTargetSchema, err)
}

func TestNewRollupTargetNilPolicySchema(t *testing.T) {
	_, err := newRollupTarget(&schema.RollupTarget{
		Policies: []*schema.Policy{nil},
	})
	require.Error(t, err)
}

func TestRollupTargetSameTransform(t *testing.T) {
	policies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.DefaultAggregationID),
	}
	target := RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar2")}
	inputs := []testRollupTargetData{
		{
			target: RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar2"), Policies: policies},
			result: true,
		},
		{
			target: RollupTarget{Name: b("baz"), Tags: bs("bar1", "bar2")},
			result: false,
		},
		{
			target: RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar3")},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, target.sameTransform(input.target))
	}
}

func TestRollupTargetClone(t *testing.T) {
	policies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.DefaultAggregationID),
	}
	target := RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar2"), Policies: policies}
	cloned := target.clone()

	// Cloned object should look exactly the same as the original one.
	require.Equal(t, target, cloned)

	// Change references in the cloned object should not mutate the original object.
	cloned.Tags[0] = b("bar3")
	cloned.Policies[0] = policy.DefaultPolicy
	require.Equal(t, target.Tags, bs("bar1", "bar2"))
	require.Equal(t, target.Policies, policies)
}

func TestRollupRuleSnapshotNilSchema(t *testing.T) {
	_, err := newRollupRuleSnapshot(nil, testTagsFilterOptions())
	require.Equal(t, errNilRollupRuleSnapshotSchema, err)
}

func TestRollupRuleNilSchema(t *testing.T) {
	_, err := newRollupRule(nil, testTagsFilterOptions())
	require.Equal(t, errNilRollupRuleSchema, err)
}

func TestRollupRuleValidSchema(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, "12669817-13ae-40e6-ba2f-33087b262c68", rr.uuid)

	expectedSnapshots := []struct {
		name         string
		tombstoned   bool
		cutoverNanos int64
		targets      []RollupTarget
	}{
		{
			name:         "foo",
			tombstoned:   false,
			cutoverNanos: 12345,
			targets: []RollupTarget{
				{
					Name: b("rName1"),
					Tags: [][]byte{b("rtagName1"), b("rtagName2")},
					Policies: []policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
					},
				},
			},
		},
		{
			name:         "bar",
			tombstoned:   true,
			cutoverNanos: 67890,
			targets: []RollupTarget{
				{
					Name: b("rName1"),
					Tags: [][]byte{b("rtagName1"), b("rtagName2")},
					Policies: []policy.Policy{
						policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
						policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), compressedMean),
					},
				},
			},
		},
	}
	for i, snapshot := range expectedSnapshots {
		require.Equal(t, snapshot.name, rr.snapshots[i].name)
		require.Equal(t, snapshot.tombstoned, rr.snapshots[i].tombstoned)
		require.Equal(t, snapshot.cutoverNanos, rr.snapshots[i].cutoverNanos)
		require.Equal(t, snapshot.targets, rr.snapshots[i].targets)
	}
}

func TestRollupRuleActiveSnapshotNotFound(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Nil(t, rr.ActiveSnapshot(0))
}

func TestRollupRuleActiveSnapshotFound(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, rr.snapshots[1], rr.ActiveSnapshot(100000))
}

func TestRollupRuleActiveRuleNotFound(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, rr, rr.ActiveRule(0))
}

func TestRollupRuleActiveRuleFound(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	expected := &rollupRule{
		uuid:      rr.uuid,
		snapshots: rr.snapshots[1:],
	}
	require.Equal(t, expected, rr.ActiveRule(100000))
}

type testRollupTargetData struct {
	target RollupTarget
	result bool
}

func TestRollupTargetSchema(t *testing.T) {
	expectedSchema := testRollupRuleSchema.Snapshots[0].Targets[0]
	rt, err := newRollupTarget(expectedSchema)
	require.NoError(t, err)
	schema, err := rt.Schema()
	require.NoError(t, err)
	require.EqualValues(t, expectedSchema, schema)
}

func TestRollupRuleSnapshotSchema(t *testing.T) {
	expectedSchema := testRollupRuleSchema.Snapshots[0]
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := rr.snapshots[0].Schema()
	require.NoError(t, err)
	require.EqualValues(t, expectedSchema, schema)
}

func TestRollupRuleSchema(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := rr.Schema()
	require.NoError(t, err)
	require.Equal(t, testRollupRuleSchema, schema)
}

func TestNewRollupRuleFromFields(t *testing.T) {
	rawFilters := map[string]string{"tag3": "value3"}
	rr, err := newRollupRuleFromFields(
		"bar",
		rawFilters,
		[]RollupTarget{
			{
				Name: b("rName1"),
				Tags: [][]byte{b("rtagName1"), b("rtagName2")},
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
		12345,
	)
	require.NoError(t, err)
	expectedSnapshot := rollupRuleSnapshot{
		name:         "bar",
		tombstoned:   false,
		cutoverNanos: 12345,
		filter:       nil,
		rawFilters:   rawFilters,
		targets: []RollupTarget{
			{
				Name: b("rName1"),
				Tags: [][]byte{b("rtagName1"), b("rtagName2")},
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
	}

	require.NoError(t, err)
	n, err := rr.Name()
	require.NoError(t, err)

	require.Equal(t, n, "bar")
	require.False(t, rr.Tombstoned())
	require.Len(t, rr.snapshots, 1)
	require.Equal(t, rr.snapshots[0].cutoverNanos, expectedSnapshot.cutoverNanos)
	require.Equal(t, rr.snapshots[0].rawFilters, expectedSnapshot.rawFilters)
	require.Equal(t, rr.snapshots[0].targets, expectedSnapshot.targets)
}

func TestRollupNameNoSnapshot(t *testing.T) {
	rr := rollupRule{
		uuid:      "blah",
		snapshots: []*rollupRuleSnapshot{},
	}
	_, err := rr.Name()
	require.Error(t, err)
}

func TestRollupTombstonedNoSnapshot(t *testing.T) {
	rr := rollupRule{
		uuid:      "blah",
		snapshots: []*rollupRuleSnapshot{},
	}
	require.True(t, rr.Tombstoned())
}

func TestRollupTombstoned(t *testing.T) {
	rr, _ := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.True(t, rr.Tombstoned())
}
