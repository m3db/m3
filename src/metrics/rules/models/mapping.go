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
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/policy"
)

// MappingRule is a common json serializable mapping rule.
type MappingRule struct {
	ID                  string                 `json:"id,omitempty"`
	Name                string                 `json:"name" validate:"required"`
	CutoverMillis       int64                  `json:"cutoverMillis,omitempty"`
	Filter              string                 `json:"filter" validate:"required"`
	AggregationID       aggregation.ID         `json:"aggregation"`
	StoragePolicies     policy.StoragePolicies `json:"storagePolicies"`
	LastUpdatedBy       string                 `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64                  `json:"lastUpdatedAtMillis"`
}

// MappingRuleView is a human friendly representation of a mapping rule at a given point in time.
type MappingRuleView struct {
	ID                 string
	Name               string
	Tombstoned         bool
	CutoverNanos       int64
	Filter             string
	AggregationID      aggregation.ID
	StoragePolicies    policy.StoragePolicies
	LastUpdatedBy      string
	LastUpdatedAtNanos int64
}

// MappingRuleViews belonging to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type MappingRuleViews map[string][]*MappingRuleView

// NewMappingRule takes a MappingRuleView and returns the equivalent MappingRule.
func NewMappingRule(mrv *MappingRuleView) MappingRule {
	return MappingRule{
		ID:                  mrv.ID,
		Name:                mrv.Name,
		Filter:              mrv.Filter,
		AggregationID:       mrv.AggregationID,
		StoragePolicies:     mrv.StoragePolicies,
		CutoverMillis:       mrv.CutoverNanos / nanosPerMilli,
		LastUpdatedBy:       mrv.LastUpdatedBy,
		LastUpdatedAtMillis: mrv.LastUpdatedAtNanos / nanosPerMilli,
	}
}

// ToMappingRuleView returns a ToMappingRuleView type.
func (m MappingRule) ToMappingRuleView() *MappingRuleView {
	return &MappingRuleView{
		ID:                 m.ID,
		Name:               m.Name,
		Tombstoned:         false,
		CutoverNanos:       m.CutoverMillis * nanosPerMilli,
		Filter:             m.Filter,
		AggregationID:      m.AggregationID,
		StoragePolicies:    m.StoragePolicies,
		LastUpdatedBy:      m.LastUpdatedBy,
		LastUpdatedAtNanos: m.LastUpdatedAtMillis * nanosPerMilli,
	}
}

// Equal determines whether two mapping rules are equal.
func (m *MappingRule) Equal(other *MappingRule) bool {
	if m == nil && other == nil {
		return true
	}
	if m == nil || other == nil {
		return false
	}
	return m.ID == other.ID &&
		m.Name == other.Name &&
		m.Filter == other.Filter &&
		m.AggregationID.Equal(other.AggregationID) &&
		m.StoragePolicies.Equal(other.StoragePolicies)
}

type mappingRulesByNameAsc []MappingRule

func (a mappingRulesByNameAsc) Len() int           { return len(a) }
func (a mappingRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a mappingRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// MappingRuleSnapshots contains a list of mapping rule snapshots.
type MappingRuleSnapshots struct {
	MappingRules []MappingRule `json:"mappingRules"`
}

// NewMappingRuleSnapshots returns a new MappingRuleSnapshots object.
func NewMappingRuleSnapshots(hist []*MappingRuleView) MappingRuleSnapshots {
	mappingRules := make([]MappingRule, len(hist))
	for i, view := range hist {
		mappingRules[i] = NewMappingRule(view)
	}
	return MappingRuleSnapshots{MappingRules: mappingRules}
}
