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

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules/view"

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
			RuleData: &view.MappingRule{
				Name: "change3",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.MappingRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.MappingRule{
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
			RuleData: &view.MappingRule{
				Name: "change2",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.MappingRule{
				Name: "Add1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.MappingRule{
				Name: "change1",
			},
		},
	}
	expected := []MappingRuleChange{
		{
			Op: AddOp,
			RuleData: &view.MappingRule{
				Name: "Add1",
			},
		},
		{
			Op: AddOp,
			RuleData: &view.MappingRule{
				Name: "Add2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID2"),
			RuleData: &view.MappingRule{
				Name: "change1",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID3"),
			RuleData: &view.MappingRule{
				Name: "change2",
			},
		},
		{
			Op:     ChangeOp,
			RuleID: ptr("rrID1"),
			RuleData: &view.MappingRule{
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
	jsonInput := []byte(`{
		"op": "change",
		"ruleData": {
			"id": "validID",
			"name": "valid rule name",
			"cutoverMillis": 61522,
			"filter": "name:servers.* service:servers",
			"storagePolicies": [
					"10s:2d",
					"1m:40d"
			],
			"lastUpdatedBy": "valid user name",
			"lastUpdatedAtMillis": 1522
		},
		"ruleID": "validID"
	}`)

	var ruleChange MappingRuleChange
	err := json.Unmarshal(jsonInput, &ruleChange)
	require.NoError(t, err)

	expected := MappingRuleChange{
		Op: "change",
		RuleData: &view.MappingRule{
			ID:            "validID",
			Name:          "valid rule name",
			CutoverMillis: 61522,
			Filter:        "name:servers.* service:servers",
			StoragePolicies: policy.StoragePolicies{
				policy.MustParseStoragePolicy("10s:2d"),
				policy.MustParseStoragePolicy("1m:40d"),
			},
			LastUpdatedBy:       "valid user name",
			LastUpdatedAtMillis: 1522,
		},
		RuleID: ptr("validID"),
	}
	require.Equal(t, expected, ruleChange)
}
