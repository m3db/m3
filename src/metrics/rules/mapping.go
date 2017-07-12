// Copyright (c) 2017 Uber Technologies, Inc.
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

package rules

import (
	"errors"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	errNilMappingRuleSnapshotSchema = errors.New("nil mapping rule snapshot schema")
	errNilMappingRuleSchema         = errors.New("nil mapping rule schema")
)

// mappingRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is aggregated and retained under the provided set of policies.
type mappingRuleSnapshot struct {
	name         string
	tombstoned   bool
	cutoverNanos int64
	filter       filters.Filter
	rawFilters   map[string]string
	policies     []policy.Policy
}

func newMappingRuleSnapshot(
	r *schema.MappingRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*mappingRuleSnapshot, error) {
	if r == nil {
		return nil, errNilMappingRuleSnapshotSchema
	}
	policies, err := policy.NewPoliciesFromSchema(r.Policies)
	if err != nil {
		return nil, err
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}
	return &mappingRuleSnapshot{
		name:         r.Name,
		tombstoned:   r.Tombstoned,
		cutoverNanos: r.CutoverTime,
		filter:       filter,
		policies:     policies,
		rawFilters:   r.TagFilters,
	}, nil
}

// Schema returns the given MappingRuleSnapshot in protobuf form.
func (mrs mappingRuleSnapshot) Schema() (*schema.MappingRuleSnapshot, error) {
	res := &schema.MappingRuleSnapshot{
		Name:        mrs.name,
		Tombstoned:  mrs.tombstoned,
		CutoverTime: mrs.cutoverNanos,
		TagFilters:  mrs.rawFilters,
	}

	policies := make([]*schema.Policy, len(mrs.policies))
	for i, p := range mrs.policies {
		policy, err := p.Schema()
		if err != nil {
			return nil, err
		}
		policies[i] = policy
	}
	res.Policies = policies

	return res, nil
}

// mappingRule stores mapping rule snapshots.
type mappingRule struct {
	uuid      string
	snapshots []*mappingRuleSnapshot
}

func newMappingRule(
	mc *schema.MappingRule,
	opts filters.TagsFilterOptions,
) (*mappingRule, error) {
	if mc == nil {
		return nil, errNilMappingRuleSchema
	}
	snapshots := make([]*mappingRuleSnapshot, 0, len(mc.Snapshots))
	for i := 0; i < len(mc.Snapshots); i++ {
		mr, err := newMappingRuleSnapshot(mc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, mr)
	}
	return &mappingRule{
		uuid:      mc.Uuid,
		snapshots: snapshots,
	}, nil
}

// ActiveSnapshot returns the latest snapshot whose cutover time is earlier than or
// equal to timeNanos, or nil if not found.
func (mc *mappingRule) ActiveSnapshot(timeNanos int64) *mappingRuleSnapshot {
	idx := mc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return mc.snapshots[idx]
}

// ActiveRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future snapshots after time timeNanos.
func (mc *mappingRule) ActiveRule(timeNanos int64) *mappingRule {
	idx := mc.activeIndex(timeNanos)
	// If there are no snapshots that are currently in effect, it means either all
	// snapshots are in the future, or there are no snapshots.
	if idx < 0 {
		return mc
	}
	return &mappingRule{uuid: mc.uuid, snapshots: mc.snapshots[idx:]}
}

func (mc *mappingRule) activeIndex(timeNanos int64) int {
	idx := len(mc.snapshots) - 1
	for idx >= 0 && mc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

// Schema returns the given MappingRule in protobuf form.
func (mc mappingRule) Schema() (*schema.MappingRule, error) {
	res := &schema.MappingRule{
		Uuid: mc.uuid,
	}

	snapshots := make([]*schema.MappingRuleSnapshot, len(mc.snapshots))
	for i, s := range mc.snapshots {
		snapshot, err := s.Schema()
		if err != nil {
			return nil, err
		}
		snapshots[i] = snapshot
	}
	res.Snapshots = snapshots

	return res, nil
}
