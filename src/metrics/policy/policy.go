// Copyright (c) 2016 Uber Technologies, Inc.
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

	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3x/time"
)

const (
	// InitPolicyVersion is the version of an uninitialized policy
	InitPolicyVersion = -1

	// DefaultPolicyVersion is the version for the default policy
	DefaultPolicyVersion = 0
)

var (
	emptyPolicy            Policy
	emptyVersionedPolicies VersionedPolicies

	// defaultPolicies are the default policies
	// TODO(xichen): possibly make this dynamically configurable in the future
	defaultPolicies = []Policy{
		{
			Resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
			Retention:  Retention(2 * 24 * time.Hour),
		},
		{
			Resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
			Retention:  Retention(30 * 24 * time.Hour),
		},
	}
)

// Policy represents the resolution and retention period metric datapoints
// are stored at
type Policy struct {
	// Resolution is the resolution datapoints are stored at
	Resolution Resolution

	// Retention is the period datatpoints are retained for
	Retention Retention
}

// String is the string representation of a policy
func (p Policy) String() string {
	return fmt.Sprintf("{resolution:%s,retention:%s}", p.Resolution.String(), p.Retention.String())
}

func newPolicy(p *schema.Policy) (Policy, error) {
	precision := time.Duration(p.Resolution.Precision)
	unit, err := xtime.UnitFromDuration(precision)
	if err != nil {
		return emptyPolicy, err
	}
	return Policy{
		Resolution: Resolution{
			Window:    time.Duration(p.Resolution.WindowSize),
			Precision: unit,
		},
		Retention: Retention(p.Retention.Period),
	}, nil
}

func newPolicies(policies []*schema.Policy) ([]Policy, error) {
	res := make([]Policy, 0, len(policies))
	for _, p := range policies {
		policy, err := newPolicy(p)
		if err != nil {
			return nil, err
		}
		res = append(res, policy)
	}
	return res, nil
}

// ByResolutionAsc implements the sort.Sort interface to sort
// policies by resolution in ascending order, finest resolution first.
// If two policies have the same resolution, the one with longer
// retention period comes first.
type ByResolutionAsc []Policy

func (pr ByResolutionAsc) Len() int      { return len(pr) }
func (pr ByResolutionAsc) Swap(i, j int) { pr[i], pr[j] = pr[j], pr[i] }

func (pr ByResolutionAsc) Less(i, j int) bool {
	w1, w2 := pr[i].Resolution.Window, pr[j].Resolution.Window
	if w1 < w2 {
		return true
	}
	if w1 > w2 {
		return false
	}
	r1, r2 := pr[i].Retention, pr[j].Retention
	if r1 > r2 {
		return true
	}
	if r1 < r2 {
		return false
	}
	// NB(xichen): compare precision to ensure a deterministic ordering
	return pr[i].Resolution.Precision < pr[i].Resolution.Precision
}

// VersionedPolicies represent a list of policies at a specified version
type VersionedPolicies struct {
	// Version is the version of the policies
	Version int

	// Cutover is when the policies take effect
	Cutover time.Time

	// isDefault determines whether the policies are the default policies
	isDefault bool

	// policies represent the list of policies
	policies []Policy
}

// IsDefault determines whether the policies are the default policies
func (vp VersionedPolicies) IsDefault() bool { return vp.isDefault }

// Policies returns the policies
func (vp VersionedPolicies) Policies() []Policy {
	if vp.isDefault {
		return defaultPolicies
	}
	return vp.policies
}

// String is the representation of versioned policies
func (vp VersionedPolicies) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("{version:%d,cutover:%s,isDefault:%v,policies:[", vp.Version, vp.Cutover.String(), vp.isDefault))
	for i := range vp.policies {
		buf.WriteString(vp.policies[i].String())
		if i < len(vp.policies)-1 {
			buf.WriteString(",")
		}
	}
	buf.WriteString("]}")
	return buf.String()
}

// Reset resets the versioned policies
func (vp *VersionedPolicies) Reset() {
	*vp = emptyVersionedPolicies
}

// DefaultVersionedPolicies creates a new default versioned policies
func DefaultVersionedPolicies(version int, cutover time.Time) VersionedPolicies {
	return VersionedPolicies{
		Version:   version,
		Cutover:   cutover,
		isDefault: true,
	}
}

// CustomVersionedPolicies creates a new custom versioned policies
func CustomVersionedPolicies(version int, cutover time.Time, policies []Policy) VersionedPolicies {
	return VersionedPolicies{
		Version:   version,
		Cutover:   cutover,
		isDefault: false,
		policies:  policies,
	}
}
