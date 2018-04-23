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
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/watch"

	"github.com/fortytw2/leaktest"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

const (
	testValueKey = "testValue"
)

func TestValueWatchAlreadyWatching(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	notifyCh := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)

	mockStore, rv := testValueWithMockStore(ctrl)
	mockStore.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	err := rv.Watch()
	require.Error(t, err)

	_, ok := err.(watch.InitValueError)
	require.True(t, ok)
	require.NoError(t, rv.Watch())

	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	rv.Unwatch()
}

func TestValueWatchCreateWatchError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	errWatch := errors.New("error creating watch")
	store.EXPECT().Watch(rv.key).Return(nil, errWatch)

	err := rv.Watch()
	require.Error(t, err)

	_, ok := err.(watch.CreateWatchError)
	require.True(t, ok)
	store.EXPECT().Watch(rv.key).Return(nil, errWatch)

	err = rv.Watch()
	require.Error(t, err)
	_, ok = err.(watch.CreateWatchError)
	require.True(t, ok)

	rv.Unwatch()
}

func TestValueWatchWatchTimeout(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	notifyCh := make(chan struct{})
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	err := rv.Watch()
	require.Error(t, err)
	_, ok := err.(watch.InitValueError)
	require.True(t, ok)

	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	rv.Unwatch()
}

func TestValueWatchUpdateError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	errUpdate := errors.New("error updating")
	rv.updateFn = func(interface{}) error {
		return errUpdate
	}
	rv.initValue()
	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Get().Return(mem.NewValue(1, nil))
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	err := rv.Watch()
	require.Error(t, err)
	_, ok := err.(watch.InitValueError)
	require.True(t, ok)
	require.Contains(t, err.Error(), errUpdate.Error())

	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	rv.Unwatch()
}

func TestValueWatchSuccess(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	store, rv := testValueWithMockStore(ctrl)
	rv.updateFn = func(interface{}) error { return nil }
	rv.initValue()
	notifyCh := make(chan struct{}, 1)
	notifyCh <- struct{}{}
	mockWatch := kv.NewMockValueWatch(ctrl)
	mockWatch.EXPECT().C().Return(notifyCh).MinTimes(1)
	mockWatch.EXPECT().Get().Return(mem.NewValue(1, nil))
	store.EXPECT().Watch(rv.key).Return(mockWatch, nil)

	require.NoError(t, rv.Watch())

	mockWatch.EXPECT().Close().Do(func() { close(notifyCh) })
	rv.Unwatch()
}

func TestValueWatchUnWatchMultipleTimes(t *testing.T) {
	defer leaktest.Check(t)()

	store, rv := testValueWithMemStore()
	rv.updateFn = func(interface{}) error { return nil }
	rv.initValue()
	_, err := store.SetIfNotExists(testValueKey, &commonpb.BoolProto{})
	require.NoError(t, err)

	iter := 10
	for i := 0; i < iter; i++ {
		require.NoError(t, rv.Watch())
		rv.Unwatch()
	}
}

func TestValueUpdateStaleUpdate(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	currValue := mem.NewValue(3, nil)
	rv.currValue = currValue
	newValue := mem.NewValue(3, nil)
	require.NoError(t, rv.updateFn(newValue))
	require.Equal(t, currValue, rv.currValue)
}

func TestValueUpdateUnmarshalError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errUnmarshal := errors.New("error unmarshaling")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, errUnmarshal }

	require.Error(t, rv.updateFn(mem.NewValue(3, nil)))
	require.Nil(t, rv.currValue)
}

func TestValueUpdateProcessError(t *testing.T) {
	defer leaktest.Check(t)()

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	_, rv := testValueWithMockStore(ctrl)
	errProcess := errors.New("error processing")
	rv.unmarshalFn = func(v kv.Value) (interface{}, error) { return nil, nil }
	rv.processFn = func(v interface{}) error { return errProcess }

	require.Error(t, rv.updateFn(mem.NewValue(3, nil)))
	require.Nil(t, rv.currValue)
}

func TestValueUpdateSuccess(t *testing.T) {
	defer leaktest.Check(t)()

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
	require.NoError(t, rv.update(input))
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
