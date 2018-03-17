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

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/errors"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	testMappingRuleSchema = &schema.MappingRule{
		Uuid: "12669817-13ae-40e6-ba2f-33087b262c68",
		Snapshots: []*schema.MappingRuleSnapshot{
			&schema.MappingRuleSnapshot{
				Name:               "foo",
				Tombstoned:         false,
				CutoverNanos:       12345,
				LastUpdatedAtNanos: 1234,
				LastUpdatedBy:      "someone",
				Filter:             "tag1:value1 tag2:value2",
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
				Name:               "bar",
				Tombstoned:         true,
				CutoverNanos:       67890,
				LastUpdatedAtNanos: 1234,
				LastUpdatedBy:      "someone",
				Filter:             "tag3:value3 tag4:value4",
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

func TestNewMappingRuleSnapshotFromSchema(t *testing.T) {
	res, err := newMappingRuleSnapshot(testMappingRuleSchema.Snapshots[0], testTagsFilterOptions())
	expectedPolicies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.MustCompressTypes(aggregation.P999)),
	}
	require.NoError(t, err)
	require.Equal(t, "foo", res.name)
	require.Equal(t, false, res.tombstoned)
	require.Equal(t, int64(12345), res.cutoverNanos)
	require.NotNil(t, res.filter)
	require.Equal(t, "tag1:value1 tag2:value2", res.rawFilter)
	require.Equal(t, expectedPolicies, res.policies)
	require.Equal(t, int64(1234), res.lastUpdatedAtNanos)
	require.Equal(t, "someone", res.lastUpdatedBy)
}

func TestNewMappingRuleSnapshotNilSchema(t *testing.T) {
	_, err := newMappingRuleSnapshot(nil, testTagsFilterOptions())
	require.Equal(t, err, errNilMappingRuleSnapshotSchema)
}

func TestNewMappingRuleSnapshotFromSchemaError(t *testing.T) {
	badFilters := []string{
		"tag3:",
		"tag3:*a*b*c*d",
		"ab[cd",
	}

	for _, f := range badFilters {
		cloned := *testMappingRuleSchema.Snapshots[0]
		cloned.Filter = f
		_, err := newMappingRuleSnapshot(&cloned, testTagsFilterOptions())
		require.Error(t, err)
	}
}

func TestNewMappingRuleSnapshotFromFields(t *testing.T) {
	var (
		name         = "testSnapshot"
		cutoverNanos = int64(12345)
		rawFilter    = "tagName1:tagValue1 tagName2:tagValue2"
		policies     = []policy.Policy{
			policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
			policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
		}
		lastUpdatedAtNanos = int64(67890)
		lastUpdatedBy      = "testUser"
	)
	res, err := newMappingRuleSnapshotFromFields(
		name,
		cutoverNanos,
		rawFilter,
		policies,
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
	require.Equal(t, policies, res.policies)
	require.Equal(t, lastUpdatedAtNanos, res.lastUpdatedAtNanos)
	require.Equal(t, lastUpdatedBy, res.lastUpdatedBy)
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
				policy.NewPolicy(policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour), aggregation.DefaultID),
				policy.NewPolicy(policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour), aggregation.DefaultID),
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

func TestMappingRuleActiveSnapshotFoundSecond(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[1], mr.ActiveSnapshot(100000))
}

func TestMappingRuleActiveSnapshotFoundFirst(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[0], mr.ActiveSnapshot(20000))
}

func TestMappingRuleActiveRuleNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(0))
}

func TestMappingRuleActiveRuleFoundSecond(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	expected := &mappingRule{
		uuid:      mr.uuid,
		snapshots: mr.snapshots[1:],
	}
	require.Equal(t, expected, mr.ActiveRule(100000))
}

func TestMappingRuleActiveRuleFoundFirst(t *testing.T) {
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
	rawFilter := "tag3:value3"
	mr, err := newMappingRuleFromFields(
		"bar",
		rawFilter,
		[]policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), aggregation.DefaultID)},
		UpdateMetadata{12345, 12345, "test_user"},
	)
	require.NoError(t, err)
	expectedSnapshot := mappingRuleSnapshot{
		name:         "bar",
		tombstoned:   false,
		cutoverNanos: 12345,
		filter:       nil,
		rawFilter:    rawFilter,
		policies:     []policy.Policy{policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour), aggregation.DefaultID)},
	}

	require.NoError(t, err)
	n, err := mr.Name()
	require.NoError(t, err)

	require.Equal(t, n, "bar")
	require.False(t, mr.Tombstoned())
	require.Len(t, mr.snapshots, 1)
	require.Equal(t, mr.snapshots[0].cutoverNanos, expectedSnapshot.cutoverNanos)
	require.Equal(t, mr.snapshots[0].rawFilter, expectedSnapshot.rawFilter)
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
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.True(t, mr.Tombstoned())
}

func TestMappingRuleMarkTombstoned(t *testing.T) {
	schema := &schema.MappingRule{
		Snapshots: []*schema.MappingRuleSnapshot{testMappingRuleSchema.Snapshots[0]},
	}
	mr, err := newMappingRule(schema, testTagsFilterOptions())
	require.NoError(t, err)

	expectedPolicies := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.MustCompressTypes(aggregation.P999)),
	}
	require.Equal(t, 1, len(mr.snapshots))
	lastSnapshot := mr.snapshots[0]
	require.Equal(t, "foo", lastSnapshot.name)
	require.False(t, lastSnapshot.tombstoned)
	require.Equal(t, int64(12345), lastSnapshot.cutoverNanos)
	require.NotNil(t, lastSnapshot.filter)
	require.Equal(t, "tag1:value1 tag2:value2", lastSnapshot.rawFilter)
	require.Equal(t, expectedPolicies, lastSnapshot.policies)
	require.Equal(t, int64(1234), lastSnapshot.lastUpdatedAtNanos)
	require.Equal(t, "someone", lastSnapshot.lastUpdatedBy)

	meta := UpdateMetadata{
		cutoverNanos:   67890,
		updatedAtNanos: 10000,
		updatedBy:      "someone else",
	}
	require.NoError(t, mr.markTombstoned(meta))
	require.Equal(t, 2, len(mr.snapshots))
	require.Equal(t, lastSnapshot, mr.snapshots[0])
	lastSnapshot = mr.snapshots[1]
	require.Equal(t, "foo", lastSnapshot.name)
	require.True(t, lastSnapshot.tombstoned)
	require.Equal(t, int64(67890), lastSnapshot.cutoverNanos)
	require.NotNil(t, lastSnapshot.filter)
	require.Equal(t, "tag1:value1 tag2:value2", lastSnapshot.rawFilter)
	require.Nil(t, lastSnapshot.policies)
	require.Equal(t, int64(10000), lastSnapshot.lastUpdatedAtNanos)
	require.Equal(t, "someone else", lastSnapshot.lastUpdatedBy)
}

func TestMappingRuleMarkTombstonedNoSnapshots(t *testing.T) {
	schema := &schema.MappingRule{}
	mr, err := newMappingRule(schema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Error(t, mr.markTombstoned(UpdateMetadata{}))
}

func TestMappingRuleMarkTombstonedAlreadyTombstoned(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)
	require.Error(t, mr.markTombstoned(UpdateMetadata{}))
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

	s1Clone.rawFilter = "blah:foo"
	require.NotEqual(t, s1.rawFilter, "blah:foo")

	s1Clone.policies = append(s1Clone.policies, s1Clone.policies[0])
	require.NotEqual(t, s1.policies, s1Clone.policies)
}

func TestMappingRuleHistory(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)

	hist, err := mr.history()
	require.NoError(t, err)
	require.Equal(t, len(mr.snapshots), len(hist))
	p0, _ := policy.ParsePolicy("10s:24h|P999")
	p1, _ := policy.ParsePolicy("1m:24h")
	p2, _ := policy.ParsePolicy("5m:2d")

	expectedViews := []*models.MappingRuleView{
		&models.MappingRuleView{
			ID:                 "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:               "bar",
			CutoverNanos:       67890,
			Tombstoned:         true,
			Filter:             "tag3:value3 tag4:value4",
			Policies:           []policy.Policy{p1, p2},
			LastUpdatedAtNanos: 1234,
			LastUpdatedBy:      "someone",
		},
		&models.MappingRuleView{
			ID:                 "12669817-13ae-40e6-ba2f-33087b262c68",
			Name:               "foo",
			CutoverNanos:       12345,
			Tombstoned:         false,
			Filter:             "tag1:value1 tag2:value2",
			Policies:           []policy.Policy{p0},
			LastUpdatedAtNanos: 1234,
			LastUpdatedBy:      "someone",
		},
	}

	require.Equal(t, expectedViews, hist)
}

func TestNewMappingRuleViewError(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, testTagsFilterOptions())
	require.NoError(t, err)

	actual, err := mr.mappingRuleView(20)
	require.Error(t, err)
	require.Nil(t, actual)
}
