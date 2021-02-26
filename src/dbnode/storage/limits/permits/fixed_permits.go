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
	"github.com/m3db/m3/src/x/context"
)

type fixedPermits struct {
	permits chan struct{}
}

type fixedPermitsManager struct {
	fp fixedPermits
}

var (
	_ Permits = &fixedPermits{}
	_ Manager = &fixedPermitsManager{}
)

// NewFixedPermitsManager returns a permits manager that uses a fixed size of permits.
func NewFixedPermitsManager(size int) Manager {
	fp := fixedPermits{permits: make(chan struct{}, size)}
	for i := 0; i < size; i++ {
		fp.permits <- struct{}{}
	}
	return &fixedPermitsManager{fp}
}

func (f *fixedPermitsManager) NewPermits(_ context.Context) (Permits, error) {
	return &f.fp, nil
}

func (f *fixedPermits) Acquire(ctx context.Context) error {
	// don't acquire a permit if ctx is already done.
	select {
	case <-ctx.GoContext().Done():
		return ctx.GoContext().Err()
	default:
	}

	select {
	case <-ctx.GoContext().Done():
		return ctx.GoContext().Err()
	case <-f.permits:
		return nil
	}
}

func (f *fixedPermits) TryAcquire(ctx context.Context) (bool, error) {
	// don't acquire a permit if ctx is already done.
	select {
	case <-ctx.GoContext().Done():
		return false, ctx.GoContext().Err()
	default:
	}

	select {
	case <-f.permits:
		return true, nil
	default:
		return false, nil
	}
}

func (f *fixedPermits) Release(_ int64) {
	select {
	case f.permits <- struct{}{}:
	default:
		panic("more permits released than acquired")
	}
}
