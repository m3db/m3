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
	"fmt"
	"sort"
	"time"

	"github.com/pborman/uuid"
)

const (
	nanosPerMilli = int64(time.Millisecond / time.Nanosecond)
)

// IDGenType describes the scheme for generating IDs in rule snapshots from RuleSet structs
// which dont have a ID.
type IDGenType int

const (
	// GenerateID Generates a UUID v4.
	GenerateID IDGenType = iota
	// DontGenerateID does not generate ID for the rule snapshot.
	DontGenerateID
)

// RuleSet is a common json serializable rule set.
type RuleSet struct {
	Namespace     string        `json:"id"`
	Version       int           `json:"version"`
	CutoverMillis int64         `json:"cutoverMillis"`
	MappingRules  []MappingRule `json:"mappingRules"`
	RollupRules   []RollupRule  `json:"rollupRules"`
}

// RuleSetSnapshotView represents a snapshot of a rule set containing snapshots of rules
// in the ruleset.
type RuleSetSnapshotView struct {
	Namespace    string
	Version      int
	CutoverNanos int64
	MappingRules map[string]*MappingRuleView
	RollupRules  map[string]*RollupRuleView
}

// NewRuleSet takes a RuleSetSnapshotView and returns the equivalent RuleSet.
func NewRuleSet(latest *RuleSetSnapshotView) RuleSet {
	mr := make([]MappingRule, 0, len(latest.MappingRules))
	for _, m := range latest.MappingRules {
		mr = append(mr, NewMappingRule(m))
	}
	rr := make([]RollupRule, 0, len(latest.RollupRules))
	for _, r := range latest.RollupRules {
		rr = append(rr, NewRollupRule(r))
	}
	return RuleSet{
		Namespace:     latest.Namespace,
		Version:       latest.Version,
		CutoverMillis: latest.CutoverNanos / nanosPerMilli,
		MappingRules:  mr,
		RollupRules:   rr,
	}
}

// ToRuleSetSnapshotView create a ToRuleSetSnapshot from a RuleSet. If the RuleSet has no IDs
// for any of its mapping rules or rollup rules, it generates missing IDs and sets as a string UUID
// string so they can be stored in a mapping (id -> rule).
func (r RuleSet) ToRuleSetSnapshotView(IDGenType IDGenType) (*RuleSetSnapshotView, error) {
	mappingRules := make(map[string]*MappingRuleView, len(r.MappingRules))
	for _, mr := range r.MappingRules {
		id := mr.ID
		if id == "" {
			if IDGenType == DontGenerateID {
				return nil, fmt.Errorf("can't convert RuleSet to ruleSetSnapshot, no mapping rule id for %v", mr)
			}
			id = uuid.New()
			mr.ID = id
		}
		mappingRules[id] = mr.ToMappingRuleView()
	}

	rollupRules := make(map[string]*RollupRuleView, len(r.RollupRules))
	for _, rr := range r.RollupRules {
		id := rr.ID
		if id == "" {
			if IDGenType == DontGenerateID {
				return nil, fmt.Errorf("can't convert RuleSet to ruleSetSnapshot, no rollup rule id for %v", rr)
			}
			id = uuid.New()
			rr.ID = id
		}
		rollupRules[id] = rr.ToRollupRuleView()
	}

	return &RuleSetSnapshotView{
		Namespace:    r.Namespace,
		Version:      r.Version,
		CutoverNanos: r.CutoverMillis * nanosPerMilli,
		MappingRules: mappingRules,
		RollupRules:  rollupRules,
	}, nil
}

// Sort sorts the rules in the ruleset.
func (r *RuleSet) Sort() {
	sort.Sort(mappingRulesByNameAsc(r.MappingRules))
	sort.Sort(rollupRulesByNameAsc(r.RollupRules))
}

// RuleSets is a collection of rulesets.
type RuleSets map[string]*RuleSet

// Sort sorts each ruleset based on it's own sort method.
func (rss RuleSets) Sort() {
	for _, rs := range rss {
		rs.Sort()
	}
}
