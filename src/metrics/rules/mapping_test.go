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
						Resolution: &schema.Resolution{
							WindowSize: int64(time.Minute),
							Precision:  int64(time.Minute),
						},
						Retention: &schema.Retention{
							Period: int64(24 * time.Hour),
						},
					},
					&schema.Policy{
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
	}
)

func TestMappingRuleSnapshotNilSchema(t *testing.T) {
	_, err := newMappingRuleSnapshot(nil, nil)
	require.Equal(t, err, errNilMappingRuleSnapshotSchema)
}

func TestNewMappingRuleNilSchema(t *testing.T) {
	_, err := newMappingRule(nil, nil)
	require.Equal(t, err, errNilMappingRuleSchema)
}

func TestNewMappingRule(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, nil)
	require.NoError(t, err)
	require.Equal(t, "12669817-13ae-40e6-ba2f-33087b262c68", mr.uuid)
	expectedSnapshots := []struct {
		name       string
		tombstoned bool
		cutoverNs  int64
		policies   []policy.Policy
	}{
		{
			name:       "foo",
			tombstoned: false,
			cutoverNs:  12345,
			policies: []policy.Policy{
				policy.NewPolicy(10*time.Second, xtime.Second, 24*time.Hour),
			},
		},
		{
			name:       "bar",
			tombstoned: true,
			cutoverNs:  67890,
			policies: []policy.Policy{
				policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
				policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
			},
		},
	}
	for i, snapshot := range expectedSnapshots {
		require.Equal(t, snapshot.name, mr.snapshots[i].name)
		require.Equal(t, snapshot.tombstoned, mr.snapshots[i].tombstoned)
		require.Equal(t, snapshot.cutoverNs, mr.snapshots[i].cutoverNs)
		require.Equal(t, snapshot.policies, mr.snapshots[i].policies)
	}
}

func TestMappingRuleActiveSnapshotNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, nil)
	require.NoError(t, err)
	require.Nil(t, mr.ActiveSnapshot(0))
}

func TestMappingRuleActiveSnapshotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, nil)
	require.NoError(t, err)
	require.Equal(t, mr.snapshots[1], mr.ActiveSnapshot(100000))
}

func TestMappingRuleActiveRuleNotFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, nil)
	require.NoError(t, err)
	require.Equal(t, mr, mr.ActiveRule(0))
}

func TestMappingRuleActiveRuleFound(t *testing.T) {
	mr, err := newMappingRule(testMappingRuleSchema, nil)
	require.NoError(t, err)
	expected := &mappingRule{
		uuid:      mr.uuid,
		snapshots: mr.snapshots[1:],
	}
	require.Equal(t, expected, mr.ActiveRule(100000))
}
