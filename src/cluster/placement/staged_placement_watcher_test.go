// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/stretchr/testify/require"
)

const (
	testStagedPlacementKey = "testStagedPlacementKey"
)

func TestStagedPlacementWatcherWatchAlreadyWatching(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	require.Equal(t, errPlacementWatcherIsWatching, watcher.Watch())
}

func TestStagedPlacementWatcherWatchSuccess(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherNotWatching
	require.NoError(t, watcher.Watch())
}

func TestStagedPlacementWatcherActiveStagedPlacementNotWatching(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherNotWatching

	_, _, err := watcher.ActiveStagedPlacement()
	require.Equal(t, errPlacementWatcherIsNotWatching, err)
}

func TestStagedPlacementWatcherActiveStagedPlacementSuccess(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	_, doneFn, err := watcher.ActiveStagedPlacement()
	require.NoError(t, err)
	doneFn()
}

func TestStagedPlacementWatcherUnwatchNotWatching(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherNotWatching
	require.Equal(t, errPlacementWatcherIsNotWatching, watcher.Unwatch())
}

func TestStagedPlacementWatcherUnwatchSuccess(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	require.NoError(t, watcher.Unwatch())
	require.Equal(t, placementWatcherNotWatching, watcher.state)
	require.Nil(t, watcher.placement)
}

func TestStagedPlacementWatcherToStagedPlacementNotWatching(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	_, err := watcher.toStagedPlacement(nil)
	require.Equal(t, errPlacementWatcherIsNotWatching, err)
}

func TestStagedPlacementWatcherToPlacementNilValue(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	_, err := watcher.toStagedPlacement(nil)
	require.Equal(t, errNilValue, err)
}

func TestStagedPlacementWatcherToStagedPlacementUnmarshalError(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	_, err := watcher.toStagedPlacement(mem.NewValueWithData(1, []byte("abcd")))
	require.Error(t, err)
}

func TestStagedPlacementWatcherToStagedPlacementSuccess(t *testing.T) {
	watcher, store := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	val, err := store.Get(testStagedPlacementKey)
	require.NoError(t, err)
	p, err := watcher.toStagedPlacement(val)
	require.NoError(t, err)
	pss := p.(*stagedPlacement)
	require.Equal(t, 1, pss.version)
	require.Equal(t, len(testActivePlacements), len(pss.placements))
	for i := 0; i < len(testActivePlacements); i++ {
		validateSnapshot(t, testActivePlacements[i], pss.placements[i])
	}
}

func TestStagedPlacementWatcherProcessNotWatching(t *testing.T) {
	watcher, _ := testStagedPlacementWatcher(t)
	require.Equal(t, errPlacementWatcherIsNotWatching, watcher.process(nil))
}

func TestStagedPlacementWatcherProcessSuccess(t *testing.T) {
	var (
		allInstances [][]Instance
		numCloses    int
	)
	opts := NewActiveStagedPlacementOptions().
		SetOnPlacementsAddedFn(func(placements []Placement) {
			for _, placement := range placements {
				allInstances = append(allInstances, placement.Instances())
			}
		})
	pss, err := NewStagedPlacementFromProto(1, testStagedPlacementProto, opts)
	require.NoError(t, err)
	watcher, _ := testStagedPlacementWatcher(t)
	watcher.state = placementWatcherWatching
	watcher.nowFn = func() time.Time { return time.Unix(0, 99999) }
	watcher.placement = &mockPlacement{
		closeFn: func() error { numCloses++; return nil },
	}

	require.NoError(t, watcher.process(pss))
	require.NotNil(t, watcher.placement)
	require.Equal(t, [][]Instance{testActivePlacements[1].Instances()}, allInstances)
	require.Equal(t, 1, numCloses)
}

func testStagedPlacementWatcher(t *testing.T) (*stagedPlacementWatcher, kv.Store) {
	store := mem.NewStore()
	_, err := store.SetIfNotExists(testStagedPlacementKey, testStagedPlacementProto)
	require.NoError(t, err)

	watcherOpts := testStagedPlacementWatcherOptions().SetStagedPlacementStore(store)
	watcher := NewStagedPlacementWatcher(watcherOpts)
	return watcher.(*stagedPlacementWatcher), store
}

func testStagedPlacementWatcherOptions() StagedPlacementWatcherOptions {
	return NewStagedPlacementWatcherOptions().
		SetInitWatchTimeout(100 * time.Millisecond).
		SetStagedPlacementKey(testStagedPlacementKey).
		SetStagedPlacementStore(mem.NewStore())
}

type closeFn func() error

type mockPlacement struct {
	closeFn closeFn
}

func (mp *mockPlacement) ActivePlacement() (Placement, DoneFn, error) {
	return nil, func() {}, nil
}

func (mp *mockPlacement) Close() error { return mp.closeFn() }

func (mp *mockPlacement) Version() int { return 0 }
