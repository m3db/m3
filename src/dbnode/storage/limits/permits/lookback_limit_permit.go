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

type lookbackLimitPermit struct {
	limit  limits.LookbackLimit
	source []byte
}

var _ Manager = (*LookbackLimitPermitManager)(nil)

var _ Permits = (*lookbackLimitPermit)(nil)

// NewLookbackLimitPermitsManager builds a new lookback limit permits manager.
func NewLookbackLimitPermitsManager(
	instrumentOpts instrument.Options,
	opts limits.LookbackLimitOptions,
	name string,
	sourceLoggerBuilder limits.SourceLoggerBuilder,
) *LookbackLimitPermitManager {
	lookbackLimit := limits.NewLookbackLimit(instrumentOpts, opts, name, sourceLoggerBuilder)

	// We expose this implementation type to allow caller to use Start/Stop
	// lookback functions which are not part of the Permits interface.
	return &LookbackLimitPermitManager{
		Limit: lookbackLimit,
	}
}

// NewPermits returns a new set of permits.
func (p *LookbackLimitPermitManager) NewPermits(ctx context.Context) Permits {
	s := sourceFromContext(ctx)
	return &lookbackLimitPermit{
		limit:  p.Limit,
		source: s,
	}
}

// Start starts background handling of the lookback limit for the permits.
func (p *LookbackLimitPermitManager) Start() {
	p.Limit.Start()
}

// Stop stops the background handling of the lookback limit for the permits.
func (p *LookbackLimitPermitManager) Stop() {
	p.Limit.Stop()
}

func (p *lookbackLimitPermit) Acquire(_ context.Context) error {
	return p.limit.Inc(1, p.source)
}

func (p *lookbackLimitPermit) TryAcquire(_ context.Context) (bool, error) {
	err := p.limit.Inc(1, p.source)
	return err != nil, err
}

func (p *lookbackLimitPermit) Release() {
}

func sourceFromContext(ctx context.Context) []byte {
	goctx, ok := ctx.GoContext()
	if !ok {
		return nil
	}

	val := goctx.Value(limits.SourceContextKey)
	parsed, ok := val.([]byte)
	if !ok {
		return nil
	}
	return parsed
}
