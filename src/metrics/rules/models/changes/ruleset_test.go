package changes

import (
	"testing"

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

func ptr(s string) *string {
	return &s
}
