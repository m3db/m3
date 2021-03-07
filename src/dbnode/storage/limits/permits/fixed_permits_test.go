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
	stdctx "context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

func TestFixedPermits(t *testing.T) {
	ctx := context.NewBackground()
	iOpts := instrument.NewOptions()
	fp, err := NewFixedPermitsManager(3, 1, iOpts).NewPermits(ctx)
	require.NoError(t, err)
	expectedP := NewPermit(1, iOpts)
	expectedP.refCount.Inc()
	p, err := fp.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, *expectedP, *p)
	p, err = fp.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, *expectedP, *p)
	p, err = fp.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, *expectedP, *p)

	tryP, err := fp.TryAcquire(ctx)
	require.NoError(t, err)
	require.Nil(t, tryP)

	fp.Release(p)
	p, err = fp.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, *expectedP, *p)
}

func TestPanics(t *testing.T) {
	ctx := context.NewBackground()
	iOpts := instrument.NewOptions()
	fp, err := NewFixedPermitsManager(3, 1, iOpts).NewPermits(ctx)
	require.NoError(t, err)
	p, err := fp.Acquire(ctx)
	require.NoError(t, err)
	fp.Release(p)

	defer instrument.SetShouldPanicEnvironmentVariable(true)()
	require.Panics(t, func() { fp.Release(p) })
}

func TestFixedPermitsTimeouts(t *testing.T) {
	ctx := context.NewBackground()
	iOpts := instrument.NewOptions()
	fp, err := NewFixedPermitsManager(1, 1, iOpts).NewPermits(ctx)
	expectedP := NewPermit(1, iOpts)
	expectedP.refCount.Inc()
	require.NoError(t, err)
	p, err := fp.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, *expectedP, *p)

	tryP, err := fp.TryAcquire(ctx)
	require.NoError(t, err)
	require.Nil(t, tryP)

	stdCtx, cancel := stdctx.WithCancel(stdctx.Background())
	cancel()
	ctx = context.NewWithGoContext(stdCtx)

	fp.Release(p)

	_, err = fp.Acquire(ctx)
	require.Error(t, err)

	_, err = fp.TryAcquire(ctx)
	require.Error(t, err)
}
