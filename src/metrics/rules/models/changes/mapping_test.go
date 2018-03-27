package changes

import (
	"sort"
	"testing"

	"github.com/m3db/m3metrics/rules/models"

	"github.com/stretchr/testify/require"
)

func TestSortMappingRuleChanges(t *testing.T) {
	ruleChanges := []MappingRuleChange{
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
			RuleID: ptr("rrID2"),
			RuleData: &models.MappingRule{
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
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
	}
	expected := []MappingRuleChange{
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
			RuleID: ptr("rrID2"),
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &models.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID3"),
			RuleData: &models.MappingRule{
				Name: "change2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID1"),
			RuleData: &models.MappingRule{
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

	sort.Sort(mappingRuleChangesByOpAscNameAscIDAsc(ruleChanges))
	require.Equal(t, expected, ruleChanges)
}
