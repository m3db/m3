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
	"errors"
	"sync"

	"go.uber.org/atomic"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/util/runtime"
)

var (
	errNilValue             = errors.New("nil value received")
	errWatcherIsNotWatching = errors.New("placement watcher is not watching")
	errWatcherIsWatching    = errors.New("placement watcher is watching")
	errWatcherCastError     = errors.New("interface cast failed, unexpected placement type")

	_ Watcher = (*placementsWatcher)(nil) // enforce interface compliance
)

// placementsWatcher implements watcher of staged placement.
// Currently, the aggregator placement is stored as staged placement in etcd using
// protobuf type `*placementpb.PlacementSnapshots` as a remnant of now deprecated concept
// of "staged placement". This Watcher abstracts that detail from the clients while
// maintaining backward compatibility.
// TODO: Consider migrating to storing protobuf type `*placementpb.Placement` in etcd.
type placementsWatcher struct {
	mtx                sync.Mutex
	valuePayload       atomic.Value
	value              runtime.Value
	watching           atomic.Bool
	onPlacementChanged OnPlacementChangedFn
}

// payload is a wrapper for type-safe interface storage in atomic.Value,
// as the concrete type has to be the same for each .Store() call.
type payload struct {
	placement Placement
}

// NewPlacementsWatcher creates a new staged placement watcher.
func NewPlacementsWatcher(opts WatcherOptions) Watcher {
	watcher := &placementsWatcher{
		onPlacementChanged: opts.OnPlacementChangedFn(),
	}

	valueOpts := runtime.NewOptions().
		SetInstrumentOptions(opts.InstrumentOptions()).
		SetInitWatchTimeout(opts.InitWatchTimeout()).
		SetKVStore(opts.StagedPlacementStore()).
		SetUnmarshalFn(watcher.unmarshalAsPlacementSnapshots).
		SetProcessFn(watcher.process)
	watcher.value = runtime.NewValue(opts.StagedPlacementKey(), valueOpts)
	return watcher
}

func (t *placementsWatcher) Watch() error {
	if !t.watching.CAS(false, true) {
		return errWatcherIsWatching
	}

	return t.value.Watch()
}

func (t *placementsWatcher) Get() (Placement, error) {
	if !t.watching.Load() {
		return nil, errWatcherIsNotWatching
	}

	vp := t.valuePayload.Load()
	pl, ok := vp.(payload)
	if !ok {
		return nil, errWatcherCastError
	}

	return pl.placement, nil
}

func (t *placementsWatcher) Unwatch() error {
	if !t.watching.CAS(true, false) {
		return errWatcherIsNotWatching
	}

	t.value.Unwatch()
	return nil
}

func (t *placementsWatcher) unmarshalAsPlacementSnapshots(value kv.Value) (interface{}, error) {
	if !t.watching.Load() {
		return nil, errWatcherIsNotWatching
	}
	if value == nil {
		return nil, errNilValue
	}

	var proto placementpb.PlacementSnapshots
	if err := value.Unmarshal(&proto); err != nil {
		return nil, err
	}

	snapshots, err := NewPlacementsFromProto(&proto)
	if err != nil {
		return nil, err
	}

	latest := snapshots.Latest()
	latest.SetVersion(value.Version())

	return latest, nil
}

// process is called upon update of value, the value is already unmarshalled.
func (t *placementsWatcher) process(newValue interface{}) error {
	t.mtx.Lock() // serialize value processing
	defer t.mtx.Unlock()

	if !t.watching.Load() {
		return errWatcherIsNotWatching
	}

	newPlacement := newValue.(Placement)
	var oldPlacement Placement
	vp := t.valuePayload.Load()
	old, ok := vp.(payload)
	if ok && old.placement != nil {
		oldPlacement = old.placement
	}

	t.valuePayload.Store(payload{placement: newPlacement})

	if t.onPlacementChanged != nil {
		t.onPlacementChanged(oldPlacement, newPlacement)
	}

	return nil
}
