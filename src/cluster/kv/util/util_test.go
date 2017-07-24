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

package util

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/stretchr/testify/require"
)

func TestWatchAndUpdateBool(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v bool
	}{}

	valueFn := func() bool {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateBool(store, "foo", &testConfig.v, &testConfig.RWMutex, true, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() == true {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if valueFn() == false {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 20})
	require.NoError(t, err)
	for {
		if valueFn() == true {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if valueFn() == false {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == true {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)

	// no longer receives update
	time.Sleep(100 * time.Millisecond)
	require.True(t, valueFn())
}

func TestWatchAndUpdateFloat64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v float64
	}{}

	valueFn := func() float64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateFloat64(store, "foo", &testConfig.v, &testConfig.RWMutex, 12.3, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 12.3 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1.2})
	require.NoError(t, err)
	for {
		if valueFn() == 1.2 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 12.3 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1.2})
	require.NoError(t, err)

	// no longer receives update
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, 12.3, valueFn())
}

func TestWatchAndUpdateInt64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v int64
	}{}

	valueFn := func() int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateInt64(store, "foo", &testConfig.v, &testConfig.RWMutex, 12, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == 12 {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 1 {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == 12 {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)

	// no longer receives update
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(12), valueFn())
}

func TestWatchAndUpdateString(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v string
	}{}

	valueFn := func() string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()

	w, err := WatchAndUpdateString(store, "foo", &testConfig.v, &testConfig.RWMutex, "bar", nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringProto{Value: "baz"})
	require.NoError(t, err)
	for {
		if valueFn() == "baz" {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.StringProto{Value: "lol"})
	require.NoError(t, err)

	// no longer receives update
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "bar", valueFn())
}

func TestWatchAndUpdateTime(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v time.Time
	}{}

	valueFn := func() time.Time {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	store := mem.NewStore()
	now := time.Now()
	defaultTime := now.Add(time.Hour)

	w, err := WatchAndUpdateTime(store, "foo", &testConfig.v, &testConfig.RWMutex, defaultTime, nil)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	for {
		if valueFn() == defaultTime {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: now.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == now.Unix() {
			break
		}
	}

	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == defaultTime {
			break
		}
	}

	w.Close()

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: now.Unix()})
	require.NoError(t, err)

	// no longer receives update
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, defaultTime, valueFn())
}

func TestBoolFromValue(t *testing.T) {
	require.True(t, BoolFromValue(mem.NewValue(0, &commonpb.BoolProto{Value: true}), "key", false, nil))
	require.False(t, BoolFromValue(mem.NewValue(0, &commonpb.BoolProto{Value: false}), "key", true, nil))

	require.True(t, BoolFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", true, nil))
	require.False(t, BoolFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", false, nil))

	require.True(t, BoolFromValue(nil, "key", true, nil))
	require.False(t, BoolFromValue(nil, "key", false, nil))
}

func TestFloat64FromValue(t *testing.T) {
	require.Equal(t, 20.5, Float64FromValue(mem.NewValue(0, &commonpb.Int64Proto{Value: 200}), "key", 20.5, nil))
	require.Equal(t, 123.3, Float64FromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123.3}), "key", 20, nil))
	require.Equal(t, 20.1, Float64FromValue(nil, "key", 20.1, nil))
}

func TestInt64FromValue(t *testing.T) {
	require.Equal(t, int64(200), Int64FromValue(mem.NewValue(0, &commonpb.Int64Proto{Value: 200}), "key", 20, nil))
	require.Equal(t, int64(20), Int64FromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", 20, nil))
	require.Equal(t, int64(20), Int64FromValue(nil, "key", 20, nil))
}

func TestStringArrayFromValue(t *testing.T) {
	defaultValue := []string{"d1", "d2"}
	v1 := []string{"s1", "s2"}

	require.Equal(t, v1, StringArrayFromValue(mem.NewValue(0, &commonpb.StringArrayProto{Values: v1}), "key", defaultValue, nil))
	require.Equal(t, defaultValue, StringArrayFromValue(mem.NewValue(0, &commonpb.Float64Proto{Value: 123}), "key", defaultValue, nil))
	require.Equal(t, defaultValue, StringArrayFromValue(nil, "key", defaultValue, nil))
}
