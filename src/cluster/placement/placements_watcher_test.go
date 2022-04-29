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

package placement

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPlacementKey = "testPlacementKey"
)

func TestWatcherWatchAlreadyWatching(t *testing.T) {
	watcher, _ := testWatcher(t)
	watcher.watching.Store(true)
	require.Equal(t, errWatcherIsWatching, watcher.Watch())
}

func TestWatcherWatchSuccess(t *testing.T) {
	watcher, _ := testWatcher(t)
	require.NoError(t, watcher.Watch())
}

func TestWatcherGetNotWatching(t *testing.T) {
	watcher, _ := testWatcher(t)

	_, err := watcher.Get()
	require.Equal(t, errWatcherIsNotWatching, err)
}

func TestWatcherGetSuccess(t *testing.T) {
	watcher, _ := testWatcher(t)
	require.NoError(t, watcher.Watch())
	actual, err := watcher.Get()
	require.NoError(t, err)
	expected := testLatestPlacement(t)
	expected.SetVersion(1)
	require.Equal(t, expected, actual)
}

func TestWatcherUnwatchNotWatching(t *testing.T) {
	watcher, _ := testWatcher(t)
	require.Equal(t, errWatcherIsNotWatching, watcher.Unwatch())
}

func TestWatcherUnwatchSuccess(t *testing.T) {
	watcher, _ := testWatcher(t)
	watcher.watching.Store(true)
	require.NoError(t, watcher.Unwatch())
	require.False(t, watcher.watching.Load())
	require.Nil(t, watcher.valuePayload.Load())
}

func TestWatcherToStagedPlacementNotWatching(t *testing.T) {
	watcher, _ := testWatcher(t)
	_, err := watcher.unmarshalAsPlacementSnapshots(nil)
	require.Equal(t, errWatcherIsNotWatching, err)
}

func TestWatcherToPlacementNilValue(t *testing.T) {
	watcher, _ := testWatcher(t)
	watcher.watching.Store(true)
	_, err := watcher.unmarshalAsPlacementSnapshots(nil)
	require.Equal(t, errNilValue, err)
}

func TestWatcherToStagedPlacementUnmarshalError(t *testing.T) {
	watcher, _ := testWatcher(t)
	_, err := watcher.unmarshalAsPlacementSnapshots(mem.NewValueWithData(1, []byte("abcd")))
	require.Error(t, err)
}

func TestWatcherUnmarshalAsPlacementSnapshots(t *testing.T) {
	watcher, store := testWatcher(t)
	watcher.watching.Store(true)
	val, err := store.Get(testPlacementKey)
	require.NoError(t, err)
	p, err := watcher.unmarshalAsPlacementSnapshots(val)
	require.NoError(t, err)
	actual, ok := p.(*placement)
	require.True(t, ok)

	expected := testLatestPlacement(t)
	expected.SetVersion(1)

	require.Equal(t, 1, actual.version)
	require.Equal(t, expected, actual)
}

func TestWatcherProcessNotWatching(t *testing.T) {
	watcher, _ := testWatcher(t)
	require.Equal(t, errWatcherIsNotWatching, watcher.process(nil))
}

func TestWatcherProcessSuccess(t *testing.T) {
	p := testLatestPlacement(t)
	testCases := []struct {
		name         string
		expectedPrev Placement
		expectedCurr Placement
	}{
		{
			name:         "previous_placement_must_be_nil",
			expectedPrev: nil,
			expectedCurr: p,
		}, {
			name:         "previous_placement_must_not_be_nil",
			expectedPrev: p,
			expectedCurr: p,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			var numCalls int
			opts := testWatcherOptions().
				SetOnPlacementChangedFn(
					func(prev, curr Placement) {
						numCalls++
						assert.Equal(t, tc.expectedPrev, prev) // nolint: scopelint
						assert.Equal(t, tc.expectedCurr, curr) // nolint: scopelint
					})

			watcher := testWatcherWithOpts(t, opts)
			watcher.watching.Store(true)
			if tc.expectedPrev != nil { // nolint: scopelint
				watcher.valuePayload.Store(payload{placement: p})
			}

			require.NoError(t, watcher.process(p))
			require.NotNil(t, watcher.valuePayload)
			require.Equal(t, 1, numCalls)
		})
	}
}

func testWatcher(t *testing.T) (*placementsWatcher, kv.Store) {
	t.Helper()
	opts := testWatcherOptions()
	watcher := testWatcherWithOpts(t, opts)

	return watcher, opts.StagedPlacementStore()
}

func testWatcherWithOpts(t *testing.T, opts WatcherOptions) *placementsWatcher {
	t.Helper()
	_, err := opts.StagedPlacementStore().
		SetIfNotExists(testPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)

	watcher := NewPlacementsWatcher(opts)
	return watcher.(*placementsWatcher)
}

func testWatcherOptions() WatcherOptions {
	return NewWatcherOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetStagedPlacementKey(testPlacementKey).
		SetStagedPlacementStore(mem.NewStore())
}
