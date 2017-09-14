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
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMappingRuleSchema = &schema.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*schema.MappingRuleSnapshot{
			&schema.MappingRuleSnapshot{
				Name:        "foo",
				Tombstoned:  false,
				CutoverTime: 12345,
				TagFilters: map[string]string{
					"tag1": "value1",
					"tag2": "value2",
				},
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
						AggregationTypes: []schema.AggregationType{
							schema.AggregationType_P999,
						},
					},
				},
			},
			&schema.MappingRuleSnapshot{
				Name:        "bar",
				Tombstoned:  true,
				CutoverTime: 67890,
				TagFilters: map[string]string{
					"tag3": "value3",
					"tag4": "value4",
				},
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
					},
				},
			},
		},
	}
)

func TestMappingRuleSnapshotNilSchema(t *testing.T) {
	_, err := newMappingRuleSnapshot(nil, testTagsFilterOptions())
	require.Equal(t, err, errNilMappingRuleSnapshotSchema)
}

func TestNewMappingRuleNilSchema(t *testing.T) {
	_, err := newMappingRule(nil, testTagsFilterOptions())
	require.Equal(t, err, errNilMappingRuleSchema)
}

func TestNewMappingRule(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, "12669817-13ae-40e6-ba2f-33087b262c68", mr.uuid)
	expectedSnapshots := []struct {
		name         string
		tombstoned   bool
		cutoverNanos int64
		policies     []policy.Policy
	}{
		{
			name:         "foo",
			tombstoned:   false,
			cutoverNanos: 12345,
			policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), compressedP999),
			},
		},
		{
			name:         "bar",
			tombstoned:   true,
			cutoverNanos: 67890,
			policies: []policy.Policy{
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), policy.DefaultAggregationID),
				policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), policy.DefaultAggregationID),
			},
		},
	}
	for i, snapshot := range expectedSnapshots {
		require.Equal(t, snapshot.name, mr.snapshots[i].name)
		require.Equal(t, snapshot.tombstoned, mr.snapshots[i].tombstoned)
		require.Equal(t, snapshot.cutoverNanos, mr.snapshots[i].cutoverNanos)
		require.Equal(t, snapshot.policies, mr.snapshots[i].policies)
	}
}

func TestMappingRuleActiveSnapshotNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Nil(t, mr.ActiveSnapshot(0))
}

func TestMappingRuleActiveSnapshotFound_Second(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[1], mr.ActiveSnapshot(100000))
}

func TestMappingRuleActiveSnapshotFound_First(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[0], mr.ActiveSnapshot(20000))
}

func TestMappingRuleActiveRuleNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(0))
}

func TestMappingRuleActiveRuleFound_Second(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	expected := &mappingRule{
		uuid:      mr.uuid,
		snapshots: mr.snapshots[1:],
	}
	require.Equal(t, expected, mr.ActiveRule(100000))
}

func TestMappingRuleActiveRuleFound_First(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(20000))
}

func TestMappingRuleSnapshotSchema(t *testing.T) {
	expectedSchema := testMappingRuleSchema.Snapshots[0]
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := mr.snapshots[0].Schema()
	require.NoError(t, err)
	require.EqualValues(t, expectedSchema, schema)
}

func TestMappingRuleSchema(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	schema, err := mr.Schema()
	require.NoError(t, err)
	require.Equal(t, testMappingRuleSchema, schema)
}

func TestNewMappingRuleFromFields(t *testing.T) {
	rawFilters := map[string]string{"tag3": "value3"}
	mr, err := newMappingRuleFromFields(
		"bar",
		rawFilters,
		[]policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID)},
		12345,
	)
	require.NoError(t, err)
	expectedSnapshot := mappingRuleSnapshot{
		name:         "bar",
		tombstoned:   false,
		cutoverNanos: 12345,
		filter:       nil,
		rawFilters:   rawFilters,
		policies:     []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), policy.DefaultAggregationID)},
	}

	require.NoError(t, err)
	n, err := mr.Name()
	require.NoError(t, err)

	require.Equal(t, n, "bar")
	require.False(t, mr.Tombstoned())
	require.Len(t, mr.snapshots, 1)
	require.Equal(t, mr.snapshots[0].cutoverNanos, expectedSnapshot.cutoverNanos)
	require.Equal(t, mr.snapshots[0].rawFilters, expectedSnapshot.rawFilters)
	require.Equal(t, mr.snapshots[0].policies, expectedSnapshot.policies)
}

func TestMappingNameNoSnapshot(t *testing.T) {
	mr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	_, err := mr.Name()
	require.Error(t, err)
}

func TestMappingTombstonedNoSnapshot(t *testing.T) {
	mr := mappingRule{
		uuid:      "blah",
		snapshots: []*mappingRuleSnapshot{},
	}
	require.True(t, mr.Tombstoned())
}

func TestMappingTombstoned(t *testing.T) {
	mr, _ := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.True(t, mr.Tombstoned())
}

func TestMappingRuleClone(t *testing.T) {
	mr, _ := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	clone := mr.clone()

	require.Equal(t, *mr, clone)
	for i, m := range mr.snapshots {
		c := clone.snapshots[i]
		require.False(t, c == m)
	}

	clone.snapshots[1].tombstoned = !clone.snapshots[1].tombstoned
	require.NotEqual(t, clone.snapshots[1].tombstoned, mr.snapshots[1].tombstoned)
}

func TestMappingRuleSnapshotClone(t *testing.T) {
	mr, _ := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	s1 := mr.snapshots[0]
	s1Clone := s1.clone()

	require.Equal(t, *s1, s1Clone)
	require.False(t, s1 == &s1Clone)

	s1Clone.rawFilters["blah"] = "foo"
	require.NotContains(t, s1.rawFilters, "blah")

	s1Clone.policies = append(s1Clone.policies, s1Clone.policies[0])
	require.NotEqual(t, s1.policies, s1Clone.policies)
}
