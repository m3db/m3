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
	"golang.org/x/sync/semaphore"

	"github.com/m3db/m3/src/x/context"
)

type fixedPermits struct {
	permits *semaphore.Weighted
}

type fixedPermitsManager struct {
	fp fixedPermits
}

var (
	_ Permits = &fixedPermits{}
	_ Manager = &fixedPermitsManager{}
)

// NewFixedPermitsManager returns a permits manager that uses a fixed size of permits.
func NewFixedPermitsManager(size int64) Manager {
	return &fixedPermitsManager{fixedPermits{permits: semaphore.NewWeighted(size)}}
}

func (f *fixedPermitsManager) NewPermits(ctx context.Context) Permits {
	return &f.fp
}

func (f *fixedPermits) Acquire(ctx context.Context) error {
	return f.permits.Acquire(ctx.GoContext(), 1)
}

func (f *fixedPermits) TryAcquire(ctx context.Context) (bool, error) {
	select {
	case <-ctx.GoContext().Done():
		return false, ctx.GoContext().Err()
	default:
	}
	return f.permits.TryAcquire(1), nil
}

func (f *fixedPermits) Release() {
	f.permits.Release(1)
}
