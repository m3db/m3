package changes

import (
	"encoding/json"
	"sort"
	"testing"

	"github.com/m3db/m3metrics/policy"
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

func TestMappingRuleJSONDeserialization(t *testing.T) {
	fixtureBytes := []byte(`{
		"op": "change",
		"ruleData": {
			"id": "validID",
			"name": "valid rule name",
			"cutoverMillis": 61522,
			"filter": "name:servers.* service:servers",
			"policies": [
					"1m:2d",
					"1m:40d"
			],
			"lastUpdatedBy": "valid user name",
			"lastUpdatedAtMillis": 1522
		},
		"ruleID": "validID"
	}`)

	ruleChange := &MappingRuleChange{}
	err := json.Unmarshal(fixtureBytes, ruleChange)
	require.NoError(t, err)

	policy1M2D, err := policy.ParsePolicy("1m:2d")
	require.NoError(t, err)
	policy1m40d, err := policy.ParsePolicy("1m:40d")
	require.NoError(t, err)
	expected := MappingRuleChange{
		Op: "change",
		RuleData: &models.MappingRule{
			ID:            "validID",
			Name:          "valid rule name",
			CutoverMillis: 61522,
			Filter:        "name:servers.* service:servers",
			Policies: []policy.Policy{
				policy1M2D,
				policy1m40d,
			},
			LastUpdatedBy:       "valid user name",
			LastUpdatedAtMillis: 1522,
		},
		RuleID: ptr("validID"),
	}
	require.Equal(t, expected, *ruleChange)
}
