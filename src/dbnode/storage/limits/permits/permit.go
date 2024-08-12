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

package permits

import (
	"go.uber.org/atomic"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/instrument"
)

// permit tracks the quota used by a caller and provides basic sanity checks that a caller
// correctly uses the permit.
type permit struct {
	// immutable state
	quota int64
	iOpts instrument.Options

	// mutable state
	quotaUsed int64
	// refCount is used to check if a caller incorrectly double releases/acquires a permit. the value should
	// always be 0 (nobody holds the permit) or 1 (somebody holds the permit).
	refCount atomic.Int32
}

// NewPermit constructs a new Permit with the provided quota.
func NewPermit(quota int64, iOpts instrument.Options) Permit {
	return &permit{
		quota: quota,
		iOpts: iOpts,
	}
}

func (p *permit) PostRelease() {
	if p.iOpts != nil && p.refCount.Dec() != 0 {
		instrument.EmitAndLogInvariantViolation(p.iOpts, func(l *zap.Logger) {
			l.Error("permit released more than once")
		})
	}
}

func (p *permit) PreAcquire() {
	if p.iOpts != nil && p.refCount.Inc() != 1 {
		instrument.EmitAndLogInvariantViolation(p.iOpts, func(l *zap.Logger) {
			l.Error("permit acquired more than once")
		})
	}
	p.quotaUsed = 0
}

// AllowedQuota is the amount of quota the caller can use with this Permit.
func (p *permit) AllowedQuota() int64 {
	return p.quota
}

// QuotaRemaining is the amount of remaining quota for this Permit. Can be negative if the caller used more quota
// than they were allowed.
func (p *permit) QuotaRemaining() int64 {
	return p.quota - p.quotaUsed
}

// Use adds the quota to the total used quota.
func (p *permit) Use(quota int64) {
	p.quotaUsed += quota
}
