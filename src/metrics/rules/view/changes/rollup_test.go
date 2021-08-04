// Copyright (c) 2018 Uber Technologies, Inc.
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

package changes

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/rules/view"
	"github.com/m3db/m3/src/metrics/transformation"

	"github.com/stretchr/testify/require"
)

func TestSortRollupRuleChanges(t *testing.T) {
	ruleChanges := []RollupRuleChange{
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID4"),
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID1"),
			RuleData: &view.RollupRule{
				Name: "change3",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.RollupRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.RollupRule{
				Name: "change1",
			},
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID4"),
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID3"),
			RuleData: &view.RollupRule{
				Name: "change2",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.RollupRule{
				Name: "Add1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.RollupRule{
				Name: "change1",
			},
		},
	}
	expected := []RollupRuleChange{
		{
			Op: AddOp,
			RuleData: &view.RollupRule{
				Name: "Add1",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.RollupRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.RollupRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.RollupRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID3"),
			RuleData: &view.RollupRule{
				Name: "change2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID1"),
			RuleData: &view.RollupRule{
				Name: "change3",
			},
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID4"),
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID4"),
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID5"),
		},
		{
			Op:     DeleteOp,
			RuleID: ptr("rrID5"),
		},
	}

	sort.Sort(rollupRuleChangesByOpAscNameAscIDAsc(ruleChanges))
	require.Equal(t, expected, ruleChanges)
}

func TestRollupRuleJSONDeserialize(t *testing.T) {
	jsonInput := []byte(`{
    "op": "change",
    "ruleData": {
        "id": "validID",
        "name": "valid Rule Name",
        "filter": "env:production clientname:client device:*  name:validMetricName regionname:global type:timer",
        "targets": [
            {
                "pipeline": [
                  {
                    "aggregation":"Sum"
                  },
                  {
                    "transformation":"PerSecond"
                  },
                  {
                    "rollup": {
                      "newName":"testRollup",
                      "tags":["tag1","tag2"],
                      "aggregation":["Min","Max"]
                    }
                  }
                ],
                "storagePolicies": [
                    "10s:2d",
                    "1m:40d"
                ]
            },
            {
                "pipeline": [
                  {
                    "rollup": {
                      "newName":"testRollup",
                      "tags":["tag1","tag2"]
                    }
                  }
                ],
                "storagePolicies": [
                    "1m:2d"
                ]
            }
        ],
        "cutoverMillis": 1519332893139,
        "lastUpdatedBy": "validUserName",
        "lastUpdatedAtMillis": 1519332833139
    },
    "ruleID": "validID"
  }`)

	var ruleChange RollupRuleChange
	err := json.Unmarshal(jsonInput, &ruleChange)
	require.NoError(t, err)

	rr1, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testRollup",
		[]string{"tag1", "tag2"},
		aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
	)
	require.NoError(t, err)
	rr2, err := pipeline.NewRollupOp(
		pipeline.GroupByRollupType,
		"testRollup",
		[]string{"tag1", "tag2"},
		aggregation.DefaultID,
	)
	require.NoError(t, err)

	expected := RollupRuleChange{
		Op:     ChangeOp,
		RuleID: ptr("validID"),
		RuleData: &view.RollupRule{
			ID:     "validID",
			Name:   "valid Rule Name",
			Filter: "env:production clientname:client device:*  name:validMetricName regionname:global type:timer",
			Targets: []view.RollupTarget{
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:        pipeline.AggregationOpType,
							Aggregation: pipeline.AggregationOp{Type: aggregation.Sum},
						},
						{
							Type:           pipeline.TransformationOpType,
							Transformation: pipeline.TransformationOp{Type: transformation.PerSecond},
						},
						{
							Type:   pipeline.RollupOpType,
							Rollup: rr1,
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.MustParseStoragePolicy("10s:2d"),
						policy.MustParseStoragePolicy("1m:40d"),
					},
				},
				{
					Pipeline: pipeline.NewPipeline([]pipeline.OpUnion{
						{
							Type:   pipeline.RollupOpType,
							Rollup: rr2,
						},
					}),
					StoragePolicies: policy.StoragePolicies{
						policy.MustParseStoragePolicy("1m:2d"),
					},
				},
			},
			CutoverMillis:       1519332893139,
			LastUpdatedBy:       "validUserName",
			LastUpdatedAtMillis: 1519332833139,
		},
	}

	require.Equal(t, expected, ruleChange)
}
