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
	"time"

	"github.com/m3db/m3x/time"
)

// DefaultPolicyVersion is the version for the default policy
const (
	DefaultPolicyVersion = 0
)

// DefaultVersionedPolicies are the default versioned policies
var (
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
