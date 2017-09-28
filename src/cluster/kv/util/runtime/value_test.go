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

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
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
	_, err := store.SetIfNotExists(testValueKey, &commonpb.BoolProto{})
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

	watchCh := make(chan struct{})
	doneCh := make(chan struct{})
	_, rv := testValueWithMockStore(ctrl)
	errUpdate := errors.New("error updating")
	rv.updateWithLockFn = func(kv.Value) error {
		close(doneCh)
		return errUpdate
	}
	watch := kv.NewMockValueWatch(ctrl)
	watch.EXPECT().C().Return(watchCh).AnyTimes()
	watch.EXPECT().Get().Return(nil)
	watch.EXPECT().Close().Do(func() { close(watchCh) })
	rv.watch = watch
	rv.status = valueWatching
	go rv.watchUpdates(watch)

	watchCh <- struct{}{}
	<-doneCh
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

func TestValueUpdateStaleUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	currValue := mem.NewValue(3, nil)
	rv.currValue = currValue
	newValue := mem.NewValue(3, nil)
	require.NoError(t, rv.updateWithLockFn(newValue))
	require.Equal(t, currValue, rv.currValue)
}

func TestValueUpdateUnmarshalError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errUnmarshal := errors.New("error unmarshaling")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, errUnmarshal }

	require.Error(t, rv.updateWithLockFn(mem.NewValue(3, nil)))
	require.Nil(t, rv.currValue)
}

func TestValueUpdateProcessError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errProcess := errors.New("error processing")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, nil }
	rv.processFn = func(v interface{}) error { return errProcess }

	require.Error(t, rv.updateWithLockFn(mem.NewValue(3, nil)))
	require.Nil(t, rv.currValue)
}

func TestValueUpdateSuccess(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var outputs []kv.Value
	_, rv := testValueWithMockStore(ctrl)
	rv.currValue = mem.NewValue(2, nil)
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return v, nil }
	rv.processFn = func(v interface{}) error {
		outputs = append(outputs, v.(kv.Value))
		return nil
	}

	input := mem.NewValue(3, nil)
	require.NoError(t, rv.updateWithLock(input))
	require.Equal(t, []kv.Value{input}, outputs)
	require.Equal(t, input, rv.currValue)
}

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
