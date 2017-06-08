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

package policy

import (
	"bytes"
	"fmt"
	"time"
)

var (
	// DefaultStagedPolicies represents a default staged policies.
	DefaultStagedPolicies StagedPolicies

	// DefaultPoliciesList represents a default policies list.
	DefaultPoliciesList = PoliciesList{DefaultStagedPolicies}
)

// StagedPolicies represent a list of policies at a specified version.
type StagedPolicies struct {
	// Cutover is when the policies take effect.
	CutoverNanos int64

	// Tombstoned determines whether the associated (rollup) metric has been tombstoned.
	Tombstoned bool

	// policies represent the list of policies.
	policies []Policy
}

// NewStagedPolicies create a new staged policies.
func NewStagedPolicies(cutoverNanos int64, tombstoned bool, policies []Policy) StagedPolicies {
	return StagedPolicies{CutoverNanos: cutoverNanos, Tombstoned: tombstoned, policies: policies}
}

// Reset resets the staged policies.
func (p *StagedPolicies) Reset() { *p = DefaultStagedPolicies }

// IsDefault returns whether this is a default staged policies.
func (p StagedPolicies) IsDefault() bool {
	return p.CutoverNanos == 0 && !p.Tombstoned && p.hasDefaultPolicies()
}

// Policies returns the policies and whether the policies are the default policies.
func (p StagedPolicies) Policies() ([]Policy, bool) {
	return p.policies, p.hasDefaultPolicies()
}

// SamePolicies returns whether two staged policies have the same policy list,
// assuming the policies are sorted in the same order.
func (p StagedPolicies) SamePolicies(other StagedPolicies) bool {
	currPolicies, currIsDefault := p.Policies()
	otherPolicies, otherIsDefault := other.Policies()
	if currIsDefault && otherIsDefault {
		return true
	}
	if currIsDefault || otherIsDefault {
		return false
	}
	if len(currPolicies) != len(otherPolicies) {
		return false
	}
	for i := 0; i < len(currPolicies); i++ {
		if currPolicies[i] != otherPolicies[i] {
			return false
		}
	}
	return true
}

// String is the representation of staged policies.
func (p StagedPolicies) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("{cutover:%s,tombstoned:%v,policies:[", time.Unix(0, p.CutoverNanos).String(), p.Tombstoned))
	for i := range p.policies {
		buf.WriteString(p.policies[i].String())
		if i < len(p.policies)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]}")
	return buf.String()
}

func (p StagedPolicies) hasDefaultPolicies() bool {
	return len(p.policies) == 0
}

// PoliciesList is a list of staged policies.
type PoliciesList []StagedPolicies

// IsDefault determines whether this is a default policies list.
func (l PoliciesList) IsDefault() bool {
	return len(l) == 1 && l[0].IsDefault()
}
