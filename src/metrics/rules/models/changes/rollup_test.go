package changes

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"
	"github.com/m3db/m3metrics/transformation"

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
			RuleData: &models.RollupRule{
				Name: "change3",
			},
		},
		{
			Op: AddOp,
			RuleData: &models.RollupRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.RollupRule{
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
			RuleData: &models.RollupRule{
				Name: "change2",
			},
		},
		{
			Op: AddOp,
			RuleData: &models.RollupRule{
				Name: "Add1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.RollupRule{
				Name: "change1",
			},
		},
	}
	expected := []RollupRuleChange{
		{
			Op: AddOp,
			RuleData: &models.RollupRule{
				Name: "Add1",
			},
		},
		{
			Op: AddOp,
			RuleData: &models.RollupRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.RollupRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.RollupRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID3"),
			RuleData: &models.RollupRule{
				Name: "change2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID1"),
			RuleData: &models.RollupRule{
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

	expected := RollupRuleChange{
		Op:     ChangeOp,
		RuleID: ptr("validID"),
		RuleData: &models.RollupRule{
			ID:     "validID",
			Name:   "valid Rule Name",
			Filter: "env:production clientname:client device:*  name:validMetricName regionname:global type:timer",
			Targets: []models.RollupTarget{
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
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       b("testRollup"),
								Tags:          bs("tag1", "tag2"),
								AggregationID: aggregation.MustCompressTypes(aggregation.Min, aggregation.Max),
							},
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
							Type: pipeline.RollupOpType,
							Rollup: pipeline.RollupOp{
								NewName:       b("testRollup"),
								Tags:          bs("tag1", "tag2"),
								AggregationID: aggregation.DefaultID,
							},
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
