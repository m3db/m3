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
	"bytes"
	"errors"
	"sort"

	"github.com/m3db/m3metrics/filters"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3metrics/policy"
)

var (
	emptyRollupTarget rollupTarget

	errNilRollupTargetSchema       = errors.New("nil rollup target schema")
	errNilRollupRuleSnapshotSchema = errors.New("nil rollup rule snapshot schema")
	errNilRollupRuleSchema         = errors.New("nil rollup rule schema")
)

// rollupTarget dictates how to roll up metrics. Metrics associated with a rollup
// target will be grouped and rolled up across the provided set of tags, named
// with the provided name, and aggregated and retained under the provided policies.
type rollupTarget struct {
	Name     []byte
	Tags     [][]byte
	Policies []policy.Policy
}

func newRollupTarget(target *schema.RollupTarget) (rollupTarget, error) {
	if target == nil {
		return emptyRollupTarget, errNilRollupTargetSchema
	}
	policies, err := policy.NewPoliciesFromSchema(target.Policies)
	if err != nil {
		return emptyRollupTarget, err
	}
	tags := make([]string, len(target.Tags))
	copy(tags, target.Tags)
	sort.Strings(tags)
	return rollupTarget{
		Name:     []byte(target.Name),
		Tags:     bytesArrayFromStringArray(tags),
		Policies: policies,
	}, nil
}

// sameTransform determines whether two targets have the same transformation.
func (t *rollupTarget) sameTransform(other rollupTarget) bool {
	if !bytes.Equal(t.Name, other.Name) {
		return false
	}
	if len(t.Tags) != len(other.Tags) {
		return false
	}
	for i := 0; i < len(t.Tags); i++ {
		if !bytes.Equal(t.Tags[i], other.Tags[i]) {
			return false
		}
	}
	return true
}

// clone clones a rollup target.
func (t *rollupTarget) clone() rollupTarget {
	name := make([]byte, len(t.Name))
	copy(name, t.Name)
	policies := make([]policy.Policy, len(t.Policies))
	copy(policies, t.Policies)
	return rollupTarget{
		Name:     name,
		Tags:     bytesArrayCopy(t.Tags),
		Policies: policies,
	}
}

// rollupRuleSnapshot defines a rule snapshot such that if a metric matches the
// provided filters, it is rolled up using the provided list of rollup targets.
type rollupRuleSnapshot struct {
	name         string
	tombstoned   bool
	cutoverNanos int64
	filter       filters.Filter
	targets      []rollupTarget
}

func newRollupRuleSnapshot(
	r *schema.RollupRuleSnapshot,
	opts filters.TagsFilterOptions,
) (*rollupRuleSnapshot, error) {
	if r == nil {
		return nil, errNilRollupRuleSnapshotSchema
	}
	targets := make([]rollupTarget, 0, len(r.Targets))
	for _, t := range r.Targets {
		target, err := newRollupTarget(t)
		if err != nil {
			return nil, err
		}
		targets = append(targets, target)
	}
	filter, err := filters.NewTagsFilter(r.TagFilters, filters.Conjunction, opts)
	if err != nil {
		return nil, err
	}
	return &rollupRuleSnapshot{
		name:         r.Name,
		tombstoned:   r.Tombstoned,
		cutoverNanos: r.CutoverTime,
		filter:       filter,
		targets:      targets,
	}, nil
}

// rollupRule stores rollup rule snapshots.
type rollupRule struct {
	uuid      string
	snapshots []*rollupRuleSnapshot
}

func newRollupRule(
	mc *schema.RollupRule,
	opts filters.TagsFilterOptions,
) (*rollupRule, error) {
	if mc == nil {
		return nil, errNilRollupRuleSchema
	}
	snapshots := make([]*rollupRuleSnapshot, 0, len(mc.Snapshots))
	for i := 0; i < len(mc.Snapshots); i++ {
		mr, err := newRollupRuleSnapshot(mc.Snapshots[i], opts)
		if err != nil {
			return nil, err
		}
		snapshots = append(snapshots, mr)
	}
	return &rollupRule{
		uuid:      mc.Uuid,
		snapshots: snapshots,
	}, nil
}

// ActiveSnapshot returns the latest rule snapshot whose cutover time is earlier
// than or equal to timeNanos, or nil if not found.
func (rc *rollupRule) ActiveSnapshot(timeNanos int64) *rollupRuleSnapshot {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return nil
	}
	return rc.snapshots[idx]
}

// ActiveRule returns the rule containing snapshots that's in effect at time timeNanos
// and all future rules after time timeNanos.
func (rc *rollupRule) ActiveRule(timeNanos int64) *rollupRule {
	idx := rc.activeIndex(timeNanos)
	if idx < 0 {
		return rc
	}
	return &rollupRule{uuid: rc.uuid, snapshots: rc.snapshots[idx:]}
}

func (rc *rollupRule) activeIndex(timeNanos int64) int {
	idx := len(rc.snapshots) - 1
	for idx >= 0 && rc.snapshots[idx].cutoverNanos > timeNanos {
		idx--
	}
	return idx
}

func bytesArrayFromStringArray(values []string) [][]byte {
	result := make([][]byte, len(values))
	for i, str := range values {
		result[i] = []byte(str)
	}
	return result
}

func bytesArrayCopy(values [][]byte) [][]byte {
	result := make([][]byte, len(values))
	for i, b := range values {
		result[i] = make([]byte, len(b))
		copy(result[i], b)
	}
	return result
}
