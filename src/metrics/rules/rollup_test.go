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

	"github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testRollupRuleSchema = &schema.RollupRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*schema.RollupRuleSnapshot{
			&schema.RollupRuleSnapshot{
				Name:               "foo",
				Tombstoned:         false,
				CutoverNanos:       12345,
				LastUpdatedAtNanos: 12345,
				LastUpdatedBy:      "someone-else",
				Filter:             "tag1:value1 tag2:value2",
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
				Name:               "bar",
				Tombstoned:         true,
				CutoverNanos:       67890,
				LastUpdatedAtNanos: 67890,
				LastUpdatedBy:      "someone",
				Filter:             "tag3:value3 tag4:value4",
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
			target: RollupTarget{Name: b("foo"), Tags: bs("bar2", "bar1"), Policies: policies},
			result: true,
		},
		{
			target: RollupTarget{Name: b("foo"), Tags: bs("bar1")},
			result: false,
		},
		{
			target: RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar2", "bar3")},
			result: false,
		},
		{
			target: RollupTarget{Name: b("foo"), Tags: bs("bar1", "bar3")},
			result: false,
		},
		{
			target: RollupTarget{Name: b("baz"), Tags: bs("bar1", "bar2")},
			result: false,
		},
		{
			target: RollupTarget{Name: b("baz"), Tags: bs("bar2", "bar1")},
			result: false,
		},
	}
	for _, input := range inputs {
		require.Equal(t, input.result, target.SameTransform(input.target))
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

func TestNewRollupRuleSnapshotFromSchema(t *testing.T) {
	res, err := newRollupRuleSnapshot(testRollupRuleSchema.Snapshots[0], testTagsFilterOptions())
	expectedTargets := []RollupTarget{
		{
			Name: b("rName1"),
			Tags: [][]byte{b("rtagName1"), b("rtagName2")},
			Policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
			},
		},
	}
	require.NoError(t, err)
	require.Equal(t, "foo", res.name)
	require.Equal(t, false, res.tombstoned)
	require.Equal(t, int64(12345), res.cutoverNanos)
	require.NotNil(t, res.filter)
	require.Equal(t, "tag1:value1 tag2:value2", res.rawFilter)
	require.Equal(t, expectedTargets, res.targets)
	require.Equal(t, int64(12345), res.lastUpdatedAtNanos)
	require.Equal(t, "someone-else", res.lastUpdatedBy)
}

func TestNewRollupRuleSnapshotNilSchema(t *testing.T) {
	_, err := newRollupRuleSnapshot(nil, testTagsFilterOptions())
	require.Equal(t, errNilRollupRuleSnapshotSchema, err)
}

func TestNewRollupRuleSnapshotFromSchemaError(t *testing.T) {
	badFilters := []string{
		"tag3:",
		"tag3:*a*b*c*d",
		"ab[cd",
	}

	for _, f := range badFilters {
		cloned := *testRollupRuleSchema.Snapshots[0]
		cloned.Filter = f
		_, err := newRollupRuleSnapshot(&cloned, testTagsFilterOptions())
		require.Error(t, err)
	}
}

func TestNewRollupRuleSnapshotFromFields(t *testing.T) {
	var (
		name         = "testSnapshot"
		cutoverNanos = int64(12345)
		rawFilter    = "tagName1:tagValue1 tagName2:tagValue2"
		targets      = []RollupTarget{
			{
				Name: b("rName1"),
				Tags: [][]byte{b("rtagName1"), b("rtagName2")},
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		}
		lastUpdatedAtNanos = int64(67890)
		lastUpdatedBy      = "testUser"
	)
	res, err := newRollupRuleSnapshotFromFields(
		name,
		cutoverNanos,
		rawFilter,
		targets,
		nil,
		lastUpdatedAtNanos,
		lastUpdatedBy,
	)
	require.NoError(t, err)
	require.Equal(t, name, res.name)
	require.Equal(t, false, res.tombstoned)
	require.Equal(t, cutoverNanos, res.cutoverNanos)
	require.Equal(t, nil, res.filter)
	require.Equal(t, rawFilter, res.rawFilter)
	require.Equal(t, targets, res.targets)
	require.Equal(t, lastUpdatedAtNanos, res.lastUpdatedAtNanos)
	require.Equal(t, lastUpdatedBy, res.lastUpdatedBy)
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
	rawFilter := "tag3:value3"
	rr, err := newRollupRuleFromFields(
		"bar",
		rawFilter,
		[]RollupTarget{
			{
				Name: b("rName1"),
				Tags: [][]byte{b("rtagName1"), b("rtagName2")},
				Policies: []policy.Policy{
					policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
				},
			},
		},
		UpdateMetadata{12345, 12345, "test_user"},
	)
	require.NoError(t, err)
	expectedSnapshot := rollupRuleSnapshot{
		name:         "bar",
		tombstoned:   false,
		cutoverNanos: 12345,
		filter:       nil,
		rawFilter:    rawFilter,
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
	require.Equal(t, rr.snapshots[0].rawFilter, expectedSnapshot.rawFilter)
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

func TestRollupRuleClone(t *testing.T) {
	rr, _ := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	clone := rr.clone()
	require.Equal(t, *rr, clone)
	for i, r := range rr.snapshots {
		c := clone.snapshots[i]
		require.False(t, c == r)
	}

	clone.snapshots[1].tombstoned = !clone.snapshots[1].tombstoned
	require.NotEqual(t, clone.snapshots[1].tombstoned, rr.snapshots[1].tombstoned)
}

func TestRollupRuleSnapshotClone(t *testing.T) {
	rr, _ := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	s1 := rr.snapshots[0]
	s1Clone := s1.clone()

	require.Equal(t, *s1, s1Clone)
	require.False(t, s1 == &s1Clone)

	// Checking that things are cloned and not just referenced.
	s1Clone.rawFilter = "blah:foo"
	require.NotEqual(t, s1.rawFilter, "blah:foo")

	s1Clone.targets = append(s1Clone.targets, s1Clone.targets[0])
	require.NotEqual(t, s1.targets, s1Clone.targets)
}
func TestNewRollupRuleView(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	actual, err := rr.rollupRuleView(0)
	require.NoError(t, err)

	p, _ := policy.ParsePolicy("10s:24h")
	expected := &RollupRuleView{
		ID:                 "12669817-13ae-40e6-ba2f-33087b262c68",
		Name:               "foo",
		CutoverNanos:       12345,
		LastUpdatedAtNanos: 12345,
		LastUpdatedBy:      "someone-else",
		Filter:             "tag1:value1 tag2:value2",
		Targets: []RollupTargetView{
			RollupTargetView{
				Name:     "rName1",
				Tags:     []string{"rtagName1", "rtagName2"},
				Policies: []policy.Policy{p},
			},
		},
	}
	require.Equal(t, expected, actual)
}

func TestNewRollupRuleViewError(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	badIdx := []int{-2, 2, 30}
	for _, i := range badIdx {
		actual, err := rr.rollupRuleView(i)
		require.Error(t, err)
		require.Nil(t, actual)
	}
}

func TestNewRollupRuleHistory(t *testing.T) {
	rr, err := newRollupRule(testRollupRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	hist, err := rr.history()
	require.NoError(t, err)

	p0, _ := policy.ParsePolicy("10s:24h")
	p1, _ := policy.ParsePolicy("1m:24h")
	p2, _ := policy.ParsePolicy("5m:2d|Mean")
	expected := []*RollupRuleView{
		&RollupRuleView{
			ID:                 "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:               "bar",
			CutoverNanos:       67890,
			Tombstoned:         true,
			LastUpdatedAtNanos: 67890,
			LastUpdatedBy:      "someone",
			Filter:             "tag3:value3 tag4:value4",
			Targets: []RollupTargetView{
				RollupTargetView{
					Name:     "rName1",
					Tags:     []string{"rtagName1", "rtagName2"},
					Policies: []policy.Policy{p1, p2},
				},
			},
		},
		&RollupRuleView{
			ID:                 "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:               "foo",
			CutoverNanos:       12345,
			Tombstoned:         false,
			LastUpdatedAtNanos: 12345,
			LastUpdatedBy:      "someone-else",
			Filter:             "tag1:value1 tag2:value2",
			Targets: []RollupTargetView{
				RollupTargetView{
					Name:     "rName1",
					Tags:     []string{"rtagName1", "rtagName2"},
					Policies: []policy.Policy{p0},
				},
			},
		},
	}

	require.Equal(t, expected, hist)
}
