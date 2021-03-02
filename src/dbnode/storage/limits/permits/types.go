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

import "github.com/m3db/m3/src/x/context"

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
	// Acquire blocks until an available resource is made available for the request permit
	Acquire(ctx context.Context) error

	// TryAcquire attempts to acquire an available resource without blocking, returning
	// true if an resource was acquired.
	TryAcquire(ctx context.Context) (bool, error)

	// Release gives back one acquired permit from the specific permits instance. The user can pass an optional quota
	// indicating how much of quota was used while holding the permit.
	// Cannot release more permits than have been acquired.
	Release(quota int64)
}
