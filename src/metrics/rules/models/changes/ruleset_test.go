package changes

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/models"

	"github.com/stretchr/testify/require"
)

func TestRuleSetChangeSetsSort(t *testing.T) {
	expected := RuleSetChanges{
		Namespace: "service1",
		MappingRuleChanges: []MappingRuleChange{
			{
				Op: AddOp,
				RuleData: &models.MappingRule{
					Name: "Add1",
				},
			},
			{
				Op: AddOp,
				RuleData: &models.MappingRule{
					Name: "Add2",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID2"),
				RuleData: &models.MappingRule{
					Name: "change1",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID3"),
				RuleData: &models.MappingRule{
					Name: "change2",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID1"),
				RuleData: &models.MappingRule{
					Name: "change3",
				},
			},
		},
		RollupRuleChanges: []RollupRuleChange{
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
		},
	}

	ruleSet.Sort()
	require.Equal(t, expected, ruleSet)
	require.Equal(t, expected, ruleSet)
}

var (
	ruleSet = RuleSetChanges{
		Namespace: "service1",
		MappingRuleChanges: []MappingRuleChange{
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID1"),
				RuleData: &models.MappingRule{
					Name: "change3",
				},
			},
			{
				Op: AddOp,
				RuleData: &models.MappingRule{
					Name: "Add2",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID2"),
				RuleData: &models.MappingRule{
					Name: "change1",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID3"),
				RuleData: &models.MappingRule{
					Name: "change2",
				},
			},
			{
				Op: AddOp,
				RuleData: &models.MappingRule{
					Name: "Add1",
				},
			},
		},
		RollupRuleChanges: []RollupRuleChange{
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
		},
	}
)

func TestRollupRuleJSONDeserialize(t *testing.T) {
	fixtureBytes := []byte(`{
		"op": "change",
		"ruleData": {
				"id": "validID",
				"name": "valid Rule Name",
				"filter": "env:production clientname:client device:*  name:validMetricName regionname:global type:timer",
				"targets": [
						{
								"name": "target1",
								"tags": [
										"clientname",
										"container",
										"dc",
										"env",
										"regionname",
										"service",
										"type"
								],
								"policies": [
										"10s:2d",
										"1m:40d"
								]
						},
						{
								"name": "target2",
								"tags": [
										"clientname",
										"container",
										"dc",
										"device",
										"env",
										"regionname",
										"service",
										"type"
								],
								"policies": [
										"10s:2d",
										"1m:40d"
								]
						}
				],
				"cutoverMillis": 1519332893139,
				"lastUpdatedBy": "validUserName",
				"lastUpdatedAtMillis": 1519332833139
		},
		"ruleID": "validID"
	}`)

	ruleChange := &RollupRuleChange{}
	err := json.Unmarshal(fixtureBytes, ruleChange)
	require.NoError(t, err)

	policy10S2D, err := policy.ParsePolicy("10s:2d")
	require.NoError(t, err)
	policy1m40d, err := policy.ParsePolicy("1m:40d")
	require.NoError(t, err)
	expected := RollupRuleChange{
		Op:     ChangeOp,
		RuleID: ptr("validID"),
		RuleData: &models.RollupRule{
			ID:     "validID",
			Name:   "valid Rule Name",
			Filter: "env:production clientname:client device:*  name:validMetricName regionname:global type:timer",
			Targets: []models.RollupTarget{
				models.RollupTarget{
					Name: "target1",
					Tags: []string{
						"clientname",
						"container",
						"dc",
						"env",
						"regionname",
						"service",
						"type",
					},
					Policies: []policy.Policy{
						policy10S2D,
						policy1m40d,
					},
				},
				models.RollupTarget{
					Name: "target2",
					Tags: []string{
						"clientname",
						"container",
						"dc",
						"device",
						"env",
						"regionname",
						"service",
						"type",
					},
					Policies: []policy.Policy{
						policy10S2D,
						policy1m40d,
					},
				},
			},
			CutoverMillis:       1519332893139,
			LastUpdatedBy:       "validUserName",
			LastUpdatedAtMillis: 1519332833139,
		},
	}

	require.Equal(t, expected, *ruleChange)
}

func ptr(s string) *string {
	return &s
}
