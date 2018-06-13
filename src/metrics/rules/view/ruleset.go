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

package view

import (
	"sort"
)

// RuleSet is a snapshot of the rule set at a given point in time.
type RuleSet struct {
	Namespace     string        `json:"id"`
	Version       int           `json:"version"`
	CutoverMillis int64         `json:"cutoverMillis"`
	MappingRules  []MappingRule `json:"mappingRules"`
	RollupRules   []RollupRule  `json:"rollupRules"`
}

// Sort sorts the rules in the ruleset.
func (r *RuleSet) Sort() {
	sort.Sort(MappingRulesByNameAsc(r.MappingRules))
	sort.Sort(RollupRulesByNameAsc(r.RollupRules))
}

// RuleSets is a collection of rulesets.
type RuleSets map[string]*RuleSet

// Sort sorts each ruleset based on it's own sort method.
func (rss RuleSets) Sort() {
	for _, rs := range rss {
		rs.Sort()
	}
}
