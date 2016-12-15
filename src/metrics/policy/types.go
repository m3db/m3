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

// Resolution is the sampling resolution for datapoints
type Resolution struct {
	// Window is the bucket size represented by the resolution
	Window time.Duration

	// Precision is the precision of datapoints stored at this resoluion
	Precision xtime.Unit
}

// Retention is the retention period for datapoints
type Retention time.Duration

// Policy represents the resolution and retention period metric datapoints
// are stored at
type Policy struct {
	// Resolution is the resolution datapoints are stored at
	Resolution Resolution

	// Retention is the period datatpoints are retained for
	Retention Retention
}

// VersionedPolicies represent a list of policies at a specified version
type VersionedPolicies struct {
	// Version is the version of the policies
	Version int

	// Policies represent the list of policies
	Policies []Policy
}
