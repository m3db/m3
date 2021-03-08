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

// Package permits contains logic for granting permits to resources.
package permits

import (
	"github.com/m3db/m3/src/x/context"
)

// Options is the permit options.
type Options interface {
	// IndexQueryPermitsManager returns the index query permits manager.
	IndexQueryPermitsManager() Manager
	// SetIndexQueryPermitsManager sets the index query permits manager.
	SetIndexQueryPermitsManager(manager Manager) Options
	// SeriesReadPermitsManager returns the series read permits manager.
	SeriesReadPermitsManager() Manager
	// SetSeriesReadPermitsManager sets the series read permits manager.
	SetSeriesReadPermitsManager(manager Manager) Options
}

// Manager manages a set of permits.
type Manager interface {
	// NewPermits builds a new set of permits.
	NewPermits(ctx context.Context) (Permits, error)
}

// Permits are the set of permits that individual codepaths will utilize.
type Permits interface {
	// Acquire blocks until a Permit is available. The returned Permit is guaranteed to be non-nil if error is
	// non-nil.
	Acquire(ctx context.Context) (Permit, error)

	// TryAcquire attempts to acquire an available resource without blocking, returning
	// a non-nil a Permit if one is available. Returns nil if no Permit is currently available.
	TryAcquire(ctx context.Context) (Permit, error)

	// Release gives back one acquired permit from the specific permits instance.
	// Cannot release more permits than have been acquired.
	Release(permit Permit)
}

// Permit is granted to a caller which is allowed to consume some amount of quota.
type Permit interface {

	// Release is called when a caller releases the permit back to the permit manager.
	Release()

	// Acquire is called before the permit manager gives the permit to the caller.
	Acquire()

	// AllowedQuota is the amount of quota the caller can use with this Permit.
	AllowedQuota() int64

	// QuotaRemaining is the amount of remaining quota for this Permit. Can be negative if the caller used more quota
	// than they were allowed.
	QuotaRemaining() int64

	// Use adds the quota to the total used quota.
	Use(quota int64)
}
