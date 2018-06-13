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
	"github.com/m3db/m3metrics/pipeline"
	"github.com/m3db/m3metrics/policy"
)

// RollupTarget is a rollup target model.
type RollupTarget struct {
	Pipeline        pipeline.Pipeline      `json:"pipeline" validate:"required"`
	StoragePolicies policy.StoragePolicies `json:"storagePolicies" validate:"required"`
}

// Equal determines whether two rollup targets are equal.
func (t *RollupTarget) Equal(other *RollupTarget) bool {
	if t == nil && other == nil {
		return true
	}
	if t == nil || other == nil {
		return false
	}
	return t.Pipeline.Equal(other.Pipeline) && t.StoragePolicies.Equal(other.StoragePolicies)
}

// RollupRule is rollup rule model.
type RollupRule struct {
	ID                  string         `json:"id,omitempty"`
	Name                string         `json:"name" validate:"required"`
	Tombstoned          bool           `json:"tombstoned"`
	CutoverMillis       int64          `json:"cutoverMillis,omitempty"`
	Filter              string         `json:"filter" validate:"required"`
	Targets             []RollupTarget `json:"targets" validate:"required,dive,required"`
	LastUpdatedBy       string         `json:"lastUpdatedBy"`
	LastUpdatedAtMillis int64          `json:"lastUpdatedAtMillis"`
}

// Equal determines whether two rollup rules are equal.
func (r *RollupRule) Equal(other *RollupRule) bool {
	if r == nil && other == nil {
		return true
	}
	if r == nil || other == nil {
		return false
	}
	return r.ID == other.ID &&
		r.Name == other.Name &&
		r.Filter == other.Filter &&
		rollupTargets(r.Targets).Equal(other.Targets)
}

// RollupRules belong to a ruleset indexed by uuid.
// Each value contains the entire snapshot history of the rule.
type RollupRules map[string][]RollupRule

type rollupTargets []RollupTarget

func (t rollupTargets) Equal(other rollupTargets) bool {
	if len(t) != len(other) {
		return false
	}
	for i := 0; i < len(t); i++ {
		if !t[i].Equal(&other[i]) {
			return false
		}
	}
	return true
}

// RollupRulesByNameAsc sorts rollup rules by name in ascending order.
type RollupRulesByNameAsc []RollupRule

func (a RollupRulesByNameAsc) Len() int           { return len(a) }
func (a RollupRulesByNameAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a RollupRulesByNameAsc) Less(i, j int) bool { return a[i].Name < a[j].Name }

// RollupRuleSnapshots contains a list of rollup rule snapshots.
type RollupRuleSnapshots struct {
	RollupRules []RollupRule `json:"rollupRules"`
}
