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

package runtime

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3metrics/generated/proto/schema"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	testValueKey = "testValue"
)

func TestValueWatchAlreadyWatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	rv.status = valueWatching
	require.NoError(t, rv.Watch())
}

func TestValueWatchCreateWatchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	errWatch := errors.New("error creating watch")
	store.EXPECT().Watch(rv.key).Return(nil, errWatch)

	require.Equal(t, CreateWatchError{innerError: errWatch}, rv.Watch())
	require.Equal(t, valueNotWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchWatchTimeout(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	notifyCh := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	require.Equal(t, InitValueError{innerError: errInitWatchTimeout}, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUpdateError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	errUpdate := errors.New("error updating")
	rv.updateWithLockFn = func(kv.Value) error { return errUpdate }
	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Get().Return(nil)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	require.Equal(t, InitValueError{innerError: errUpdate}, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	rv.updateWithLockFn = func(kv.Value) error { return nil }
	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Get().Return(nil)
	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	require.NoError(t, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueUnwatchNotWatching(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	rv.status = valueNotWatching
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUnWatchMultipleTimes(t *testing.T) {
	store, rv := testValueWithMemStore()
	rv.updateWithLockFn = func(kv.Value) error { return nil }
	_, err := store.SetIfNotExists(testValueKey, &schema.RuleSet{})
	require.NoError(t, err)

	iter := 10
	for i := 0; i < iter; i++ {
		require.NoError(t, rv.Watch())
		rv.Unwatch()
	}
}

func TestValueWatchUpdatesError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errUpdate := errors.New("error updating")
	rv.updateWithLockFn = func(kv.Value) error { return errUpdate }
	ch := make(chan struct{})
	watch := kv.NewMockValueWatch(ctrl)
	watch.EXPECT().C().Return(ch).AnyTimes()
	watch.EXPECT().Get().Return(nil)
	watch.EXPECT().Close().Do(func() { close(ch) })
	rv.watch = watch
	rv.status = valueWatching
	go rv.watchUpdates(watch)

	ch <- struct{}{}
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchValueUnwatched(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	var updated int32
	rv.updateWithLockFn = func(kv.Value) error { atomic.AddInt32(&updated, 1); return nil }
	ch := make(chan struct{})
	watch := kv.NewMockValueWatch(ctrl)
	watch.EXPECT().C().Return(ch).AnyTimes()
	watch.EXPECT().Get().Return(nil).AnyTimes()
	watch.EXPECT().Close().Do(func() { close(ch) }).AnyTimes()
	rv.watch = watch
	rv.status = valueNotWatching
	go rv.watchUpdates(watch)

	ch <- struct{}{}
	// Given the update goroutine a chance to run.
	time.Sleep(100 * time.Millisecond)
	close(ch)
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueWatchValueDifferentWatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	var updated int32
	rv.updateWithLockFn = func(kv.Value) error { atomic.AddInt32(&updated, 1); return nil }
	ch := make(chan struct{})
	watch := kv.NewMockValueWatch(ctrl)
	watch.EXPECT().C().Return(ch).AnyTimes()
	watch.EXPECT().Get().Return(nil).AnyTimes()
	watch.EXPECT().Close().Do(func() { close(ch) }).AnyTimes()
	rv.watch = kv.NewMockValueWatch(ctrl)
	rv.status = valueWatching
	go rv.watchUpdates(watch)

	ch <- struct{}{}
	// Given the update goroutine a chance to run.
	time.Sleep(100 * time.Millisecond)
	close(ch)
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueUpdateNilValueError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	require.Equal(t, errNilValue, rv.updateWithLockFn(nil))
}

func TestValueUpdateUnmarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errUnmarshal := errors.New("error unmarshaling")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, errUnmarshal }

	require.Error(t, rv.updateWithLockFn(mockValue{version: 3}))
}

func TestValueUpdateProcessError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errProcess := errors.New("error processing")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, nil }
	rv.processFn = func(v interface{}) error { return errProcess }

	require.Error(t, rv.updateWithLockFn(mockValue{version: 3}))
}

func TestValueUpdateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var outputs []mockValue
	_, rv := testValueWithMockStore(ctrl)
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return v, nil }
	rv.processFn = func(v interface{}) error {
		outputs = append(outputs, v.(mockValue))
		return nil
	}

	input := mockValue{version: 3}
	require.NoError(t, rv.updateWithLock(input))
	require.Equal(t, []mockValue{input}, outputs)
	require.Equal(t, 3, rv.version)
}

func TestValueUpdateStaleUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	rv.version = 3
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return v, nil }

	require.NoError(t, rv.updateWithLock(mockValue{version: 2}))
	require.Equal(t, 3, rv.version)
}

type mockValue struct {
	version int
}

func (v mockValue) Unmarshal(proto.Message) error { return errors.New("unimplemented") }
func (v mockValue) Version() int                  { return v.version }
func (v mockValue) IsNewer(other kv.Value) bool   { return v.version > other.Version() }

func testValueOptions(store kv.Store) Options {
	return NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetInitWatchTimeout(100 * time.Millisecond).
		SetKVStore(store).
		SetUnmarshalFn(nil).
		SetProcessFn(nil)
}

func testValueWithMockStore(ctrl *gomock.Controller) (*kv.MockStore, *value) {
	store := kv.NewMockStore(ctrl)
	opts := testValueOptions(store)
	return store, NewValue(testValueKey, opts).(*value)
}

func testValueWithMemStore() (kv.Store, *value) {
	store := mem.NewStore()
	opts := testValueOptions(store)
	return store, NewValue(testValueKey, opts).(*value)
}
