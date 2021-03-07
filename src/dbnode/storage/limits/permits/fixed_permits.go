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
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

type fixedPermits struct {
	permits chan *Permit
	iOpts   instrument.Options
}

type fixedPermitsManager struct {
	fp fixedPermits
}

var (
	_ Permits = &fixedPermits{}
	_ Manager = &fixedPermitsManager{}
)

// NewFixedPermitsManager returns a permits manager that uses a fixed size of permits.
func NewFixedPermitsManager(size int, quotaPerPermit int64, iOpts instrument.Options) Manager {
	fp := fixedPermits{permits: make(chan *Permit, size), iOpts: iOpts}
	for i := 0; i < size; i++ {
		fp.permits <- &Permit{Quota: quotaPerPermit}
	}
	return &fixedPermitsManager{fp: fp}
}

func (f *fixedPermitsManager) NewPermits(_ context.Context) (Permits, error) {
	return &f.fp, nil
}

func (f *fixedPermits) Acquire(ctx context.Context) (*Permit, error) {
	// don't acquire a permit if ctx is already done.
	select {
	case <-ctx.GoContext().Done():
		return nil, ctx.GoContext().Err()
	default:
	}

	select {
	case <-ctx.GoContext().Done():
		return nil, ctx.GoContext().Err()
	case p := <-f.permits:
		f.acquire(p)
		return p, nil
	}
}

func (f *fixedPermits) TryAcquire(ctx context.Context) (*Permit, error) {
	// don't acquire a permit if ctx is already done.
	select {
	case <-ctx.GoContext().Done():
		return nil, ctx.GoContext().Err()
	default:
	}

	select {
	case p := <-f.permits:
		f.acquire(p)
		return p, nil
	default:
		return nil, nil
	}
}

func (f *fixedPermits) acquire(permit *Permit) {
	if permit.refCount.Inc() != 1 {
		instrument.EmitAndLogInvariantViolation(f.iOpts, func(l *zap.Logger) {
			l.Error("permit acquired more than once")
		})
	}
}

func (f *fixedPermits) Release(permit *Permit) {
	if permit.refCount.Dec() != 0 {
		instrument.EmitAndLogInvariantViolation(f.iOpts, func(l *zap.Logger) {
			l.Error("permit released more than once")
		})
	}

	select {
	case f.permits <- permit:
	default:
		instrument.EmitAndLogInvariantViolation(f.iOpts, func(l *zap.Logger) {
			l.Error("more permits released than acquired")
		})
	}
}
