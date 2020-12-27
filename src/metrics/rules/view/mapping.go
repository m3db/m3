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
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
)

// MappingRule is a mapping rule model at a given point in time.
type MappingRule struct {
	ID                  string                 `json:"id,omitempty"`
	Name                string                 `json:"name" validate:"required"`
	Tombstoned          bool                   `json:"tombstoned"`
	CutoverMillis       int64                  `json:"cutoverMillis,omitempty"`
	Filter              string                 `json:"filter" validate:"required"`
	AggregationID       aggregation.ID         `json:"aggregation"`
	StoragePolicies     policy.StoragePolicies `json:"storagePolicies"`
	DropPolicy          policy.DropPolicy      `json:"dropPolicy"`
	Tags                []models.Tag           `json:"tags"`
	LastUpdatedBy       string                 `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64                  `json:"lastUpdatedAtMillis"`
}

// Equal determines whether two mapping rules are equal.
func (m *MappingRule) Equal(other *MappingRule) bool {
	if m == nil && other == nil {
		return true
	}
	if m == nil || other == nil {
		return false
	}
	if len(m.Tags) != len(other.Tags) {
		return false
	}

	for i := 0; i < len(m.Tags); i++ {
		if !m.Tags[i].Equal(other.Tags[i]) {
			return false
		}
	}

	return m.ID == other.ID &&
		m.Name == other.Name &&
		m.Filter == other.Filter &&
		m.AggregationID.Equal(other.AggregationID) &&
		m.StoragePolicies.Equal(other.StoragePolicies) &&
		m.DropPolicy == other.DropPolicy
}

// MappingRules belonging to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type MappingRules map[string][]MappingRule

// MappingRulesByNameAsc sorts mapping rules by name in ascending order.
type MappingRulesByNameAsc []MappingRule

func (a MappingRulesByNameAsc) Len() int           { return len(a) }
func (a MappingRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a MappingRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// MappingRuleSnapshots contains a list of mapping rule snapshots.
type MappingRuleSnapshots struct {
	MappingRules []MappingRule `json:"mappingRules"`
}
