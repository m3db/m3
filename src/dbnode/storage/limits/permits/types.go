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
	"errors"

	"github.com/m3db/m3/src/x/context"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	// ErrOperationWaitedOnRequireNoWait is raised when an operation
	// waits for permits but explicitly required not waiting.
	ErrOperationWaitedOnRequireNoWait = xerrors.NewInvalidParamsError(errors.New(
		"operation waited for permits when requiring no waiting"))
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
	// Acquire blocks until a Permit is available. The returned Permit is
	// guaranteed to be non-nil if error is non-nil.
	Acquire(ctx context.Context) (AcquireResult, error)

	// TryAcquire attempts to acquire an available resource without blocking, returning
	// a non-nil a Permit if one is available. Returns nil if no Permit is currently available.
	TryAcquire(ctx context.Context) (Permit, error)

	// Release gives back one acquired permit from the specific permits instance.
	// Cannot release more permits than have been acquired.
	Release(permit Permit)
}

// AcquireResult contains metadata about acquiring a permit.
type AcquireResult struct {
	// Permit is the acquired permit.
	Permit Permit
	// Waited is true if the acquire called waited before being granted permits.
	// If false, the permits were granted immediately.
	Waited bool
}

// Permit is granted to a caller which is allowed to consume some amount of quota.
type Permit interface {

	// AllowedQuota is the amount of quota the caller can use with this Permit.
	AllowedQuota() int64

	// QuotaRemaining is the amount of remaining quota for this Permit. Can be negative if the caller used more quota
	// than they were allowed.
	QuotaRemaining() int64

	// Use adds the quota to the total used quota.
	Use(quota int64)

	// PostRelease is called by the Manager after a caller releases the permit back.
	// Provides a hook for the Manager. Clients should not call this method.
	PostRelease()

	// PreAcquire is called by the Manager before giving the permit to the caller.
	// Provides a hook for the Manager. Clients should not call this method.
	PreAcquire()
}
