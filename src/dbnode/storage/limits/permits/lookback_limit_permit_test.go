// Copyright (c) 2021  Uber Technologies, Inc.
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
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/instrument"
)

func TestLookbackLimitPermit(t *testing.T) {
	lookbackLimit := newTestLookbackLimit()
	manager := newManager(t, lookbackLimit)

	ctx := context.NewBackground()
	permits, err := manager.NewPermits(ctx)
	require.NoError(t, err)

	require.Equal(t, 0, lookbackLimit.count)

	_, err = permits.Acquire(ctx)
	require.NoError(t, err)
	require.Equal(t, 1, lookbackLimit.count)

	_, err = permits.TryAcquire(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, lookbackLimit.count)
}

func newManager(t *testing.T, limit limits.LookbackLimit) *LookbackLimitPermitManager {
	t.Helper()
	mgr := NewLookbackLimitPermitsManager(
		"test-limit",
		limits.DefaultLookbackLimitOptions(),
		instrument.NewTestOptions(t),
		limits.NewOptions().SourceLoggerBuilder(),
	)
	mgr.Limit = limit

	return mgr
}

func newTestLookbackLimit() *testLookbackLimit {
	return &testLookbackLimit{}
}

type testLookbackLimit struct {
	count int
}

func (t *testLookbackLimit) Options() limits.LookbackLimitOptions {
	panic("implement me")
}

func (t *testLookbackLimit) Inc(inc int, _ []byte) error {
	t.count += inc
	return nil
}

func (t *testLookbackLimit) Update(limits.LookbackLimitOptions) error {
	panic("implement me")
}

func (t *testLookbackLimit) Start() {
	panic("implement me")
}

func (t *testLookbackLimit) Stop() {
	panic("implement me")
}
