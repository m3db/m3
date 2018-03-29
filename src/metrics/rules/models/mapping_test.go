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

package models

import (
	"encoding/json"
	"testing"

	"github.com/m3db/m3metrics/policy"

	"github.com/stretchr/testify/require"
)

func TestNewMappingRule(t *testing.T) {
	id := "mr_id"
	name := "mr_name"
	fixture := testMappingRuleView(id, name)
	expected := MappingRule{
		ID:                  id,
		Name:                name,
		Filter:              "filter",
		Policies:            []policy.Policy{},
		CutoverMillis:       0,
		LastUpdatedBy:       "",
		LastUpdatedAtMillis: 0,
	}
	require.EqualValues(t, expected, NewMappingRule(fixture))
}

func TestToMappingRuleView(t *testing.T) {
	id := "id"
	name := "name"
	fixture := testMappingRule(id, name)
	expected := &MappingRuleView{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
	require.EqualValues(t, expected, fixture.ToMappingRuleView())
}
func TestMappingRuleEqual(t *testing.T) {
	mappingRule1 := `
		{
			"name": "sample_mapping_rule_1",
			"filter": "filter_1",
			"policies": [
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	mappingRule2 := `
		{
			"filter": "filter_1",
			"name": "sample_mapping_rule_1",
			"policies": [
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	var mr1 MappingRule
	err := json.Unmarshal([]byte(mappingRule1), &mr1)
	require.NoError(t, err)
	var mr2 MappingRule
	err = json.Unmarshal([]byte(mappingRule2), &mr2)
	require.NoError(t, err)

	require.True(t, mr1.Equals(&mr2))
}

func TestMappingRuleNotEqual(t *testing.T) {
	mappingRule1 := `
		{
			"name": "sample_mapping_rule_1",
			"filter": "filter_1",
			"policies": [
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	mappingRule2 := `
		{
			"filter": "filter_2",
			"name": "sample_mapping_rule_1",
			"policies": [
				"10s:2d|Count,P99,P9999","1m:40d|Count,P99,P9999"
			]
		}
	`
	mappingRule3 := `
		{
			"name": "sample_mapping_rule_2",
			"filter": "filter_1",
			"policies": [
				"1m:2d","1m:40d"
			]
		}
	`
	var mr1 MappingRule
	err := json.Unmarshal([]byte(mappingRule1), &mr1)
	require.NoError(t, err)
	var mr2 MappingRule
	err = json.Unmarshal([]byte(mappingRule2), &mr2)
	require.NoError(t, err)
	var mr3 MappingRule
	err = json.Unmarshal([]byte(mappingRule3), &mr3)
	require.NoError(t, err)

	require.False(t, mr1.Equals(&mr2))
	require.False(t, mr1.Equals(&mr3))
	require.False(t, mr2.Equals(&mr3))
}

func TestMappingRuleNilCases(t *testing.T) {
	var mr1 *MappingRule

	require.True(t, mr1.Equals(nil))

	var mr2 MappingRule
	mappingRule := &mr2
	require.False(t, mappingRule.Equals(mr1))
}

func TestMappingRuleSort(t *testing.T) {
	mappingRule := `
		{
			"name":"sample_mapping_rule_1",
			"filter":"filter_1",
			"policies":[
				"10s:2d|Count,P99,P9999",
				"1m:40d|Count,P99,P9999",
				"1m:40d|Count,P9999"
			]
		}
	`
	var mr MappingRule
	err := json.Unmarshal([]byte(mappingRule), &mr)
	require.NoError(t, err)

	expected := `["10s:2d|Count,P99,P9999","1m:40d|Count,P9999","1m:40d|Count,P99,P9999"]`
	mr.Sort()
	actual, err := json.Marshal(mr.Policies)
	require.NoError(t, err)
	require.Equal(t, expected, string(actual))
}
func TestNewMappingRuleHistoryJSON(t *testing.T) {
	id := "id"
	hist := []*MappingRuleView{
		testMappingRuleView(id, "name1"),
		testMappingRuleView(id, "name2"),
	}
	expected := MappingRuleSnapshots{
		MappingRules: []MappingRule{
			{
				ID:                  id,
				Name:                "name1",
				Filter:              "filter",
				Policies:            []policy.Policy{},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
			{
				ID:                  id,
				Name:                "name2",
				Filter:              "filter",
				Policies:            []policy.Policy{},
				CutoverMillis:       0,
				LastUpdatedBy:       "",
				LastUpdatedAtMillis: 0,
			},
		},
	}
	require.EqualValues(t, expected, NewMappingRuleSnapshots(hist))
}

// nolint:unparam
func testMappingRuleView(id, name string) *MappingRuleView {
	return &MappingRuleView{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
}

func testMappingRule(id, name string) *MappingRule {
	return &MappingRule{
		ID:       id,
		Name:     name,
		Filter:   "filter",
		Policies: []policy.Policy{},
	}
}
