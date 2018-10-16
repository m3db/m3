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

package util

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
	"go.uber.org/atomic"
)

func TestWatchAndAtomicUpdateBool(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *atomic.Bool
	}{
		v: atomic.NewBool(false),
	}

	valueFn := func() bool {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v.Load()
	}

	var (
		store        = mem.NewStore()
		defaultValue = false
	)

	watch, err := WatchAndUpdateAtomicBool(
		store, "foo", testConfig.v, defaultValue, nil,
	)
	require.NoError(t, err)

	// Valid update.
	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if !valueFn() {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 20})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.False(t, valueFn())

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if !valueFn() {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.True(t, valueFn())

	leaktest.Check(t)()
}

func TestWatchAndUpdateAtomicFloat64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *atomic.Float64
	}{
		v: atomic.NewFloat64(0),
	}

	valueFn := func() float64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v.Load()
	}

	var (
		store        = mem.NewStore()
		defaultValue = 1.35
	)

	watch, err := WatchAndUpdateAtomicFloat64(
		store, "foo", testConfig.v, defaultValue, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 3.7})
	require.NoError(t, err)
	for {
		if valueFn() == 3.7 {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 3.7, valueFn())

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1.2})
	require.NoError(t, err)
	for {
		if valueFn() == 1.2 {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == defaultValue {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 6.2})
	require.NoError(t, err)
	for {
		if valueFn() == 6.2 {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 7.2})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 6.2, valueFn())

	leaktest.Check(t)
}

func TestWatchAndUpdateAtomicInt64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *atomic.Int64
	}{
		v: atomic.NewInt64(0),
	}

	valueFn := func() int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v.Load()
	}

	var (
		store              = mem.NewStore()
		defaultValue int64 = 3
	)

	watch, err := WatchAndUpdateAtomicInt64(
		store, "foo", testConfig.v, defaultValue, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 1 {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(1), valueFn())

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 7})
	require.NoError(t, err)
	for {
		if valueFn() == 7 {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == defaultValue {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 21})
	require.NoError(t, err)
	for {
		if valueFn() == 21 {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 13})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(21), valueFn())

	leaktest.Check(t)
}

func TestWatchAndUpdateAtomicString(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *atomic.String
	}{
		v: atomic.NewString(""),
	}

	valueFn := func() string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v.Load()
	}

	var (
		store        = mem.NewStore()
		defaultValue = "abc"
	)

	watch, err := WatchAndUpdateAtomicString(
		store, "foo", testConfig.v, defaultValue, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringProto{Value: "fizz"})
	require.NoError(t, err)
	for {
		if valueFn() == "fizz" {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "fizz", valueFn())

	_, err = store.Set("foo", &commonpb.StringProto{Value: "buzz"})
	require.NoError(t, err)
	for {
		if valueFn() == "buzz" {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == defaultValue {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringProto{Value: "lol"})
	require.NoError(t, err)
	for {
		if valueFn() == "lol" {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.StringProto{Value: "abc"})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "lol", valueFn())

	leaktest.Check(t)
}
