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
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

// LookbackLimitPermitManager manages permits which enforce a global lookback limit.
// This implementation is used for backwards compatibility migration from time-based
// lookback limits to more precise permits implementations.
type LookbackLimitPermitManager struct {
	Limit limits.LookbackLimit
}

// LookbackLimitPermit is a permit modeled on top of lookback-based query limits.
// On acquisition, the permit increments the underlying limit. Before costly code
// paths, callers can check the limit to see if requests should be allowed to
// proceed.
type LookbackLimitPermit struct {
	limit  limits.LookbackLimit
	source []byte
}

var _ Manager = (*LookbackLimitPermitManager)(nil)

var (
	_ Permits = (*LookbackLimitPermit)(nil)
	// use a single permit for everybody to avoid allocations. since limits don't track quotas it's fine
	// to share the same instance.
	singlePermit = &limitPermit{}
)

// NewLookbackLimitPermitsManager builds a new lookback limit permits manager.
func NewLookbackLimitPermitsManager(
	name string,
	opts limits.LookbackLimitOptions,
	instrumentOpts instrument.Options,
	sourceLoggerBuilder limits.SourceLoggerBuilder,
) *LookbackLimitPermitManager {
	lookbackLimit := limits.NewLookbackLimit(name, opts, instrumentOpts, sourceLoggerBuilder)

	// We expose this implementation type to allow caller to use Start/Stop
	// lookback functions which are not part of the Permits interface.
	return &LookbackLimitPermitManager{
		Limit: lookbackLimit,
	}
}

// NewPermits returns a new set of permits.
func (p *LookbackLimitPermitManager) NewPermits(ctx context.Context) (Permits, error) {
	s := sourceFromContext(ctx)
	// Ensure currently under limit.
	if err := p.Limit.Inc(0, s); err != nil {
		return nil, limits.NewQueryLimitExceededError(err.Error())
	}

	return &LookbackLimitPermit{
		limit:  p.Limit,
		source: s,
	}, nil
}

// Start starts background handling of the lookback limit for the permits.
func (p *LookbackLimitPermitManager) Start() {
	p.Limit.Start()
}

// Stop stops the background handling of the lookback limit for the permits.
func (p *LookbackLimitPermitManager) Stop() {
	p.Limit.Stop()
}

// Acquire increments the underlying querying limit.
func (p *LookbackLimitPermit) Acquire(context.Context) (Permit, error) {
	return singlePermit, p.limit.Inc(1, p.source)
}

// TryAcquire increments the underlying querying limit. Functionally equivalent
// to Acquire.
func (p *LookbackLimitPermit) TryAcquire(context.Context) (Permit, error) {
	return singlePermit, p.limit.Inc(1, p.source)
}

// Release is a no-op in this implementation.
func (p *LookbackLimitPermit) Release(_ Permit) {
}

func sourceFromContext(ctx context.Context) []byte {
	val := ctx.GoContext().Value(limits.SourceContextKey)
	parsed, ok := val.([]byte)
	if !ok {
		return nil
	}
	return parsed
}

type limitPermit struct{}

func (l limitPermit) Release() {
}

func (l limitPermit) Acquire() {
}

func (l limitPermit) AllowedQuota() int64 {
	return 1
}

func (l limitPermit) QuotaRemaining() int64 {
	return 0
}

func (l limitPermit) Use(_ int64) {
}
