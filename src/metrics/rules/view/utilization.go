// Copyright (c) 2021 Uber Technologies, Inc.
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

type UtilizationRule struct {
	ID                  string        `json:"id,omitempty"`
	Name                string        `json:"name" validate:"required"`
	Tombstoned          bool          `json:"tombstoned"`
	CutoverMillis       int64         `json:"cutoverMillis,omitempty"`
	Value               string        `json:"value,omitempty"`
	Filter              string        `json:"filter" validate:"required"`
	Targets             rollupTargets `json:"targets" validate:"required,dive,required"`
	LastUpdatedBy       string        `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64         `json:"lastUpdatedAtMillis"`
}

// Equal determines whether two utilization rules are equal.
func (u *UtilizationRule) Equal(other *UtilizationRule) bool {
	if u == nil && other == nil {
		return true
	}
	if u == nil || other == nil {
		return false
	}
	return u.ID == other.ID &&
		u.Name == other.Name &&
		u.Filter == other.Filter &&
		u.Value == other.Value &&
		u.Targets.Equal(other.Targets)
}

// UtilizationRules belong to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type UtilizationRules map[string][]UtilizationRule

// UtilizationRulesByNameAsc sorts utilization rules by name in ascending order.
type UtilizationRulesByNameAsc []UtilizationRule

func (a UtilizationRulesByNameAsc) Len() int           { return len(a) }
func (a UtilizationRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a UtilizationRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// UtilizationRuleSnapshots contains a list of utilization rule snapshots.
type UtilizationRuleSnapshots struct {
	UtilizationRules []UtilizationRule `json:"utilizationRules"`
}
