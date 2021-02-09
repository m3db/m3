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
	"errors"
	"sync"

	"go.uber.org/atomic"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util/runtime"
	"github.com/m3db/m3/src/x/clock"
)

var (
	errNilValue                      = errors.New("nil value received")
	errPlacementWatcherIsNotWatching = errors.New("placement watcher is not watching")
	errPlacementWatcherIsWatching    = errors.New("placement watcher is watching")
	errPlacementWatcherCastError     = errors.New("interface cast failed, unexpected placement type")
)

type stagedPlacementWatcher struct {
	mtx           sync.Mutex
	value         runtime.Value
	nowFn         clock.NowFn
	placementOpts ActiveStagedPlacementOptions
	watching      atomic.Bool
	placement     atomic.Value
}

// plValue is a wrapper for type-safe interface storage in atomic.Value,
// as the concrete type has to be the same for each .Store() call.
type plValue struct {
	p ActiveStagedPlacement
}

// NewStagedPlacementWatcher creates a new staged placement watcher.
func NewStagedPlacementWatcher(opts StagedPlacementWatcherOptions) StagedPlacementWatcher {
	watcher := &stagedPlacementWatcher{
		nowFn:         opts.ClockOptions().NowFn(),
		placementOpts: opts.ActiveStagedPlacementOptions(),
	}

	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(opts.StagedPlacementStore()).
		SetUnmarshalFn(watcher.toStagedPlacement).
		SetProcessFn(watcher.process)
	watcher.value = runtime.NewValue(opts.StagedPlacementKey(), valueOpts)
	return watcher
}

func (t *stagedPlacementWatcher) Watch() error {
	if !t.watching.CAS(false, true) {
		return errPlacementWatcherIsWatching
	}

	return t.value.Watch()
}

func (t *stagedPlacementWatcher) ActiveStagedPlacement() (ActiveStagedPlacement, error) {
	if !t.watching.Load() {
		return nil, errPlacementWatcherIsNotWatching
	}

	pl := t.placement.Load()
	placement, ok := pl.(plValue)

	if !ok {
		return nil, errPlacementWatcherCastError
	}

	return placement.p, nil
}

func (t *stagedPlacementWatcher) Unwatch() error {
	if !t.watching.CAS(true, false) {
		return errPlacementWatcherIsNotWatching
	}

	pl := t.placement.Load()
	placement, ok := pl.(plValue)
	if ok && placement.p != nil {
		placement.p.Close() //nolint:errcheck
	}

	t.value.Unwatch()
	return nil
}

func (t *stagedPlacementWatcher) toStagedPlacement(value kv.Value) (interface{}, error) {
	if !t.watching.Load() {
		return nil, errPlacementWatcherIsNotWatching
	}
	if value == nil {
		return nil, errNilValue
	}

	var proto placementpb.PlacementSnapshots
	if err := value.Unmarshal(&proto); err != nil {
		return nil, err
	}
	version := value.Version()

	return NewStagedPlacementFromProto(version, &proto, t.placementOpts)
}

func (t *stagedPlacementWatcher) process(value interface{}) error {
	t.mtx.Lock() // serialize value processing
	defer t.mtx.Unlock()

	if !t.watching.Load() {
		return errPlacementWatcherIsNotWatching
	}
	ps := value.(StagedPlacement)
	placement := ps.ActiveStagedPlacement(t.nowFn().UnixNano())

	pl := t.placement.Load()
	oldPlacement, ok := pl.(plValue)
	if ok && oldPlacement.p != nil {
		oldPlacement.p.Close() //nolint:errcheck
	}

	t.placement.Store(plValue{p: placement})
	return nil
}
