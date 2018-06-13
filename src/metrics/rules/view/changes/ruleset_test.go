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
	"testing"

	"github.com/m3db/m3metrics/rules/view"
	"github.com/m3db/m3metrics/x/bytes"

	"github.com/stretchr/testify/require"
)

func TestRuleSetChangeSetsSort(t *testing.T) {
	expected := RuleSetChanges{
		Namespace: "service1",
		MappingRuleChanges: []MappingRuleChange{
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
				RuleID: ptr("mrID2"),
				RuleData: &view.MappingRule{
					Name: "change1",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID3"),
				RuleData: &view.MappingRule{
					Name: "change2",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID1"),
				RuleData: &view.MappingRule{
					Name: "change3",
				},
			},
		},
		RollupRuleChanges: []RollupRuleChange{
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
				RuleID: ptr("mrID2"),
				RuleData: &view.MappingRule{
					Name: "change1",
				},
			},
			{
				Op:     ChangeOp,
				RuleID: ptr("mrID3"),
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
		},
		RollupRuleChanges: []RollupRuleChange{
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
		},
	}
)

// nolint:unparam
func b(v string) []byte       { return []byte(v) }
func bs(v ...string) [][]byte { return bytes.ArraysFromStringArray(v) }
func ptr(str string) *string  { return &str }
