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

	"github.com/m3db/m3x/time"
)

const (
	// InitPolicyVersion is the version of an uninitialized policy
	InitPolicyVersion = -1

	// DefaultPolicyVersion is the version for the default policy
	DefaultPolicyVersion = 0
)

var (
	emptyVersionedPolicies VersionedPolicies

	// DefaultVersionedPolicies are the default versioned policies
	DefaultVersionedPolicies = VersionedPolicies{
		Version: DefaultPolicyVersion,
		Policies: []Policy{
			{
				Resolution: Resolution{Window: 10 * time.Second, Precision: xtime.Second},
				Retention:  Retention(2 * 24 * time.Hour),
			},
			{
				Resolution: Resolution{Window: time.Minute, Precision: xtime.Minute},
				Retention:  Retention(30 * 24 * time.Hour),
			},
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

// VersionedPolicies represent a list of policies at a specified version
type VersionedPolicies struct {
	// Version is the version of the policies
	Version int

	// Cutover is when the policies take effect
	Cutover time.Time

	// Policies represent the list of policies
	Policies []Policy
}

// String is the representation of versioned policies
func (vp VersionedPolicies) String() string {
	var buf bytes.Buffer
	buf.WriteString(fmt.Sprintf("{version:%d,cutover:%s,policies:[", vp.Version, vp.Cutover.String()))
	for i := range vp.Policies {
		buf.WriteString(vp.Policies[i].String())
		if i < len(vp.Policies)-1 {
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
