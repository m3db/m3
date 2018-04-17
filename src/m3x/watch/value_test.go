// Copyright (c) 2018 Uber Technologies, Inc.
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

package watch

import (
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestValueWatchAlreadyWatching(t *testing.T) {
	rv := NewValue(testValueOptions()).(*value)
	rv.status = valueWatching
	require.NoError(t, rv.Watch())
}

func TestValueWatchCreateWatchError(t *testing.T) {
	errWatch := errors.New("error creating watch")
	updatableFn := func() (Updatable, error) {
		return nil, errWatch
	}
	rv := NewValue(testValueOptions().SetNewUpdatableFn(updatableFn)).(*value)

	err := rv.Watch()
	require.Equal(t, CreateWatchError{innerError: errWatch}, err)
	require.Equal(t, valueNotWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchWatchTimeout(t *testing.T) {
	_, rv := testWatchableAndValue()
	err := rv.Watch()
	require.Equal(t, InitValueError{innerError: errInitWatchTimeout}, err)
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUpdateError(t *testing.T) {
	errUpdate := errors.New("error updating")
	wa, rv := testWatchableAndValue()
	require.NoError(t, wa.Update(1))
	rv.processWithLockFn = func(interface{}) error { return errUpdate }

	require.Equal(t, InitValueError{innerError: errUpdate}, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchSuccess(t *testing.T) {
	wa, rv := testWatchableAndValue()
	rv.processWithLockFn = func(interface{}) error {
		return nil
	}
	require.NoError(t, wa.Update(1))

	require.NoError(t, rv.Watch())
	require.Equal(t, valueWatching, rv.status)

	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueUnwatchNotWatching(t *testing.T) {
	_, rv := testWatchableAndValue()
	rv.status = valueNotWatching
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchUnWatchMultipleTimes(t *testing.T) {
	wa, rv := testWatchableAndValue()
	rv.processWithLockFn = func(interface{}) error {
		return nil
	}
	require.NoError(t, wa.Update(1))

	iter := 10
	for i := 0; i < iter; i++ {
		require.NoError(t, rv.Watch())
		rv.Unwatch()
	}
}

func TestValueWatchUpdatesError(t *testing.T) {
	wa, rv := testWatchableAndValue()
	wa.Update(1)

	doneCh := make(chan struct{})
	errUpdate := errors.New("error updating")
	rv.processWithLockFn = func(interface{}) error {
		close(doneCh)
		return errUpdate
	}
	_, w, err := wa.Watch()
	require.NoError(t, err)

	rv.updatable = w
	rv.getUpdateFn = func(Updatable) (interface{}, error) { return 1, nil }
	rv.status = valueWatching
	go rv.watchUpdates(w)
	<-doneCh
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueGetFnError(t *testing.T) {
	wa, rv := testWatchableAndValue()
	wa.Update(1)

	doneCh := make(chan struct{})
	errGet := errors.New("error get")

	rv.getUpdateFn = func(Updatable) (interface{}, error) { close(doneCh); return nil, errGet }
	require.Error(t, rv.Watch())
	<-doneCh
	rv.Unwatch()
	require.Equal(t, valueNotWatching, rv.status)
}

func TestValueWatchValueUnwatched(t *testing.T) {
	wa, rv := testWatchableAndValue()
	wa.Update(1)

	require.False(t, wa.IsClosed())
	var updated int32
	rv.processWithLockFn = func(interface{}) error { atomic.AddInt32(&updated, 1); return nil }
	_, w, err := wa.Watch()
	require.NoError(t, err)
	require.Equal(t, 1, len(w.C()))

	rv.updatable = w
	rv.status = valueNotWatching
	go rv.watchUpdates(w)

	for {
		if 0 == len(w.C()) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueWatchValueDifferentWatch(t *testing.T) {
	wa, rv := testWatchableAndValue()
	wa.Update(1)
	var updated int32
	rv.processWithLockFn = func(interface{}) error { atomic.AddInt32(&updated, 1); return nil }
	_, w1, err := wa.Watch()
	require.NoError(t, err)

	rv.updatable = w1
	rv.status = valueWatching
	_, w2, err := wa.Watch()
	require.NoError(t, err)
	go rv.watchUpdates(w2)

	for {
		if 0 == len(w2.C()) {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	require.Equal(t, int32(0), atomic.LoadInt32(&updated))
}

func TestValueUpdateNilValueError(t *testing.T) {
	rv := NewValue(testValueOptions()).(*value)
	require.Equal(t, errNilValue, rv.processWithLockFn(nil))
}

func TestValueUpdateProcessError(t *testing.T) {
	rv := NewValue(testValueOptions()).(*value)
	errProcess := errors.New("error processing")
	rv.processFn = func(interface{}) error { return errProcess }

	require.Error(t, rv.processWithLockFn(3))
}

func TestValueUpdateSuccess(t *testing.T) {
	var outputs []interface{}
	rv := NewValue(testValueOptions()).(*value)
	rv.processFn = func(v interface{}) error {
		outputs = append(outputs, v)
		return nil
	}

	require.NoError(t, rv.processWithLock(3))
	require.Equal(t, []interface{}{3}, outputs)
}

func testValueOptions() Options {
	return NewOptions().
		SetInstrumentOptions(instrument.NewOptions()).
		SetInitWatchTimeout(100 * time.Millisecond).
		SetProcessFn(nil)
}

func testWatchableAndValue() (Watchable, *value) {
	wa := NewWatchable()
	opts := testValueOptions().
		SetNewUpdatableFn(testUpdatableFn(wa)).
		SetGetUpdateFn(func(updatable Updatable) (interface{}, error) {
			return updatable.(Watch).Get(), nil
		})
	return wa, NewValue(opts).(*value)
}

func testUpdatableFn(wa Watchable) NewUpdatableFn {
	return func() (Updatable, error) {
		_, w, err := wa.Watch()
		return w, err
	}
}
