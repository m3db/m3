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
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/generated/proto/testpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/kv/mem"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/require"
)

var (
	testNow = time.Now()
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

	var (
		store        = mem.NewStore()
		defaultValue = false
	)

	watch, err := WatchAndUpdateBool(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil,
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

	var (
		store        = mem.NewStore()
		defaultValue = 1.35
	)

	watch, err := WatchAndUpdateFloat64(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil,
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

	var (
		store              = mem.NewStore()
		defaultValue int64 = 3
	)

	watch, err := WatchAndUpdateInt64(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil,
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

	var (
		store        = mem.NewStore()
		defaultValue = "abc"
	)

	watch, err := WatchAndUpdateString(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil,
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

func TestWatchAndUpdateStringArray(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v []string
	}{}

	valueFn := func() []string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store        = mem.NewStore()
		defaultValue = []string{"abc", "def"}
	)

	watch, err := WatchAndUpdateStringArray(
		store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"fizz", "buzz"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(valueFn(), []string{"fizz", "buzz"}) {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 12.3})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"fizz", "buzz"}, valueFn())

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"foo", "bar"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(valueFn(), []string{"foo", "bar"}) {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if stringSliceEquals(valueFn(), defaultValue) {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"jim", "jam"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(valueFn(), []string{"jim", "jam"}) {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"abc", "def"}})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"jim", "jam"}, valueFn())

	leaktest.Check(t)
}

func TestWatchAndUpdateStringArrayPointer(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *[]string
	}{}

	valueFn := func() *[]string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store        = mem.NewStore()
		defaultValue = []string{"abc", "def"}
	)

	watch, err := WatchAndUpdateStringArrayPointer(
		store, "foo", &testConfig.v, &testConfig.RWMutex, &defaultValue, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"fizz", "buzz"}})
	require.NoError(t, err)
	for {
		res := valueFn()
		if res != nil && stringSliceEquals(*res, []string{"fizz", "buzz"}) {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 12.3})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"fizz", "buzz"}, *valueFn())

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"foo", "bar"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(*valueFn(), []string{"foo", "bar"}) {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if stringSliceEquals(*valueFn(), defaultValue) {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"jim", "jam"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(*valueFn(), []string{"jim", "jam"}) {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"abc", "def"}})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"jim", "jam"}, *valueFn())

	leaktest.Check(t)
}

func TestWatchAndUpdateStringArrayPointerWithNilDefault(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v *[]string
	}{}

	valueFn := func() *[]string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var store = mem.NewStore()
	watch, err := WatchAndUpdateStringArrayPointer(
		store, "foo", &testConfig.v, &testConfig.RWMutex, nil, nil,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"fizz", "buzz"}})
	require.NoError(t, err)
	for {
		res := valueFn()
		if res != nil && stringSliceEquals(*res, []string{"fizz", "buzz"}) {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 12.3})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"fizz", "buzz"}, *valueFn())

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"foo", "bar"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals(*valueFn(), []string{"foo", "bar"}) {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn() == nil {
			break
		}
	}

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"jim", "jam"}})
	require.NoError(t, err)
	for {
		res := valueFn()
		if res != nil && stringSliceEquals(*res, []string{"jim", "jam"}) {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"abc", "def"}})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"jim", "jam"}, *valueFn())

	leaktest.Check(t)
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

	var (
		store        = mem.NewStore()
		defaultValue = time.Now()
	)

	watch, err := WatchAndUpdateTime(store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil)
	require.NoError(t, err)

	newTime := defaultValue.Add(time.Minute)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, newTime.Unix(), valueFn().Unix())

	newTime = newTime.Add(time.Minute)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	// Nil updates should apply the default value.
	_, err = store.Delete("foo")
	require.NoError(t, err)
	for {
		if valueFn().Unix() == defaultValue.Unix() {
			break
		}
	}

	newTime = newTime.Add(time.Minute)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: defaultValue.Unix()})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, newTime.Unix(), valueFn().Unix())

	leaktest.Check(t)
}

func TestWatchAndUpdateDuration(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v time.Duration
	}{}

	valueFn := func() time.Duration {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store        = mem.NewStore()
		defaultValue = time.Duration(rand.Int63())
	)

	watch, err := WatchAndUpdateDuration(store, "foo", &testConfig.v, &testConfig.RWMutex, defaultValue, nil)
	require.NoError(t, err)

	newDuration := time.Duration(rand.Int63())
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: int64(newDuration)})
	require.NoError(t, err)
	for {
		if valueFn() == newDuration {
			break
		}
	}

	// Malformed updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 100})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, newDuration, valueFn())

	newDuration = time.Duration(rand.Int63())
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: int64(newDuration)})
	require.NoError(t, err)
	for {
		if valueFn() == newDuration {
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

	newDuration = time.Duration(rand.Int63())
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: int64(newDuration)})
	require.NoError(t, err)
	for {
		if valueFn() == newDuration {
			break
		}
	}

	// Updates should not be applied after the watch is closed and there should not
	// be any goroutines still running.
	watch.Close()
	time.Sleep(100 * time.Millisecond)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: int64(defaultValue)})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, newDuration, valueFn())

	leaktest.Check(t)
}

func TestWatchAndUpdateWithValidationBool(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v bool
	}{}

	valueFn := func() bool {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateBoolFn)
	)

	_, err := WatchAndUpdateBool(store, "foo", &testConfig.v, &testConfig.RWMutex, false, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.BoolProto{Value: true})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &commonpb.BoolProto{Value: false})
	require.NoError(t, err)
	for {
		if valueFn() {
			break
		}
	}
}

func TestWatchAndUpdateWithValidationFloat64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v float64
	}{}

	valueFn := func() float64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateFloat64Fn)
	)

	_, err := WatchAndUpdateFloat64(store, "foo", &testConfig.v, &testConfig.RWMutex, 1.2, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 17})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 22})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, float64(17), valueFn())

	_, err = store.Set("foo", &commonpb.Float64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 1 {
			break
		}
	}
}

func TestWatchAndUpdateWithValidationInt64(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v int64
	}{}

	valueFn := func() int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateInt64Fn)
	)

	_, err := WatchAndUpdateInt64(store, "foo", &testConfig.v, &testConfig.RWMutex, 16, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 17})
	require.NoError(t, err)
	for {
		if valueFn() == 17 {
			break
		}
	}

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 22})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, int64(17), valueFn())

	_, err = store.Set("foo", &commonpb.Int64Proto{Value: 1})
	require.NoError(t, err)
	for {
		if valueFn() == 1 {
			break
		}
	}
}

func TestWatchAndUpdateWithValidationString(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v string
	}{}

	valueFn := func() string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateStringFn)
	)

	_, err := WatchAndUpdateString(store, "foo", &testConfig.v, &testConfig.RWMutex, "bcd", opts)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringProto{Value: "bar"})
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &commonpb.StringProto{Value: "cat"})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, "bar", valueFn())

	_, err = store.Set("foo", &commonpb.StringProto{Value: "baz"})
	require.NoError(t, err)
	for {
		if valueFn() == "bar" {
			break
		}
	}
}

func TestWatchAndUpdateWithValidationStringArray(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v []string
	}{}

	valueFn := func() []string {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateStringArrayFn)
	)

	_, err := WatchAndUpdateStringArray(
		store, "foo", &testConfig.v, &testConfig.RWMutex, []string{"a", "b"}, opts,
	)
	require.NoError(t, err)

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"fizz", "buzz"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals([]string{"fizz", "buzz"}, valueFn()) {
			break
		}
	}

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"cat"}})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, []string{"fizz", "buzz"}, valueFn())

	_, err = store.Set("foo", &commonpb.StringArrayProto{Values: []string{"jim", "jam"}})
	require.NoError(t, err)
	for {
		if stringSliceEquals([]string{"jim", "jam"}, valueFn()) {
			break
		}
	}
}

func TestWatchAndUpdateWithValidationTime(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v time.Time
	}{}

	valueFn := func() time.Time {
		testConfig.RLock()
		defer testConfig.RUnlock()

		return testConfig.v
	}

	var (
		store = mem.NewStore()
		opts  = NewOptions().SetValidateFn(testValidateTimeFn)
	)

	_, err := WatchAndUpdateTime(store, "foo", &testConfig.v, &testConfig.RWMutex, testNow, opts)
	require.NoError(t, err)

	newTime := testNow.Add(30 * time.Second)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}

	// Invalid updates should not be applied.
	invalidTime := testNow.Add(2 * time.Minute)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: invalidTime.Unix()})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)
	require.Equal(t, newTime.Unix(), valueFn().Unix())

	newTime = testNow.Add(45 * time.Second)
	_, err = store.Set("foo", &commonpb.Int64Proto{Value: newTime.Unix()})
	require.NoError(t, err)
	for {
		if valueFn().Unix() == newTime.Unix() {
			break
		}
	}
}

// NB: uses a map[string]int64 as a standin for a generic type
func TestWatchAndUpdateWithValidationGeneric(t *testing.T) {
	testConfig := struct {
		sync.RWMutex
		v map[string]int64
	}{}

	valueFn := func() map[string]int64 {
		testConfig.RLock()
		defer testConfig.RUnlock()

		clonedMap := make(map[string]int64, len(testConfig.v))
		for k, v := range testConfig.v {
			clonedMap[k] = v
		}

		return clonedMap
	}

	var (
		store      = mem.NewStore()
		opts       = NewOptions().SetValidateFn(testValidateIntMapFn)
		invalidMap = map[string]int64{"1": 1, "2": 2}
		defaultMap = map[string]int64{"1": 1, "2": 2, "3": 3}
		newMap     = map[string]int64{"1": 1, "2": 2, "5": 5}
		newMap2    = map[string]int64{"1": 1, "2": 2, "100": 100}
	)

	genericGetFn := func(v kv.Value) (interface{}, error) {
		var mapProto testpb.MapProto
		if err := v.Unmarshal(&mapProto); err != nil {
			return nil, err
		}

		return mapProto.GetValue(), nil
	}

	genericUpdateFn := func(i interface{}) {
		testConfig.v = i.(map[string]int64)
	}

	waitForExpectedValue := func(m map[string]int64) {
		start := time.Now()
		for {
			value := valueFn()
			if time.Since(start) >= time.Second*5 {
				require.FailNow(t, fmt.Sprintf("Exceeded timeout while waiting for "+
					"generic update result; expected %v, got %v", m, value))
			}

			if len(value) != len(m) {
				continue
			}

			for k, v := range m {
				if xv, found := value[k]; !found || xv != v {
					continue
				}
			}

			break
		}
	}

	_, err := WatchAndUpdateGeneric(store, "foo", genericGetFn, genericUpdateFn,
		&testConfig.RWMutex, defaultMap, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", &testpb.MapProto{Value: newMap})
	require.NoError(t, err)

	waitForExpectedValue(newMap)

	// Invalid updates should not be applied.
	_, err = store.Set("foo", &testpb.MapProto{Value: invalidMap})
	require.NoError(t, err)
	time.Sleep(100 * time.Millisecond)

	// NB: should still be old value.
	waitForExpectedValue(newMap)

	_, err = store.Set("foo", &testpb.MapProto{Value: newMap2})
	require.NoError(t, err)

	waitForExpectedValue(newMap2)

	// Check default values with a different kv key.
	_, err = WatchAndUpdateGeneric(store, "bar", genericGetFn, genericUpdateFn,
		&testConfig.RWMutex, defaultMap, opts)
	require.NoError(t, err)
	waitForExpectedValue(defaultMap)
}

func testValidateBoolFn(val interface{}) error {
	v, ok := val.(bool)
	if !ok {
		return fmt.Errorf("invalid type for val, expected bool, received %T", val)
	}

	if !v {
		return errors.New("value of update is false, must be true")
	}

	return nil
}

func testValidateFloat64Fn(val interface{}) error {
	v, ok := val.(float64)
	if !ok {
		return fmt.Errorf("invalid type for val, expected float64, received %T", val)
	}

	if v > 20 {
		return fmt.Errorf("val must be < 20, is %v", v)
	}

	return nil
}

func testValidateInt64Fn(val interface{}) error {
	v, ok := val.(int64)
	if !ok {
		return fmt.Errorf("invalid type for val, expected int64, received %T", val)
	}

	if v > 20 {
		return fmt.Errorf("val must be < 20, is %v", v)
	}

	return nil
}

func testValidateStringFn(val interface{}) error {
	v, ok := val.(string)
	if !ok {
		return fmt.Errorf("invalid type for val, expected string, received %T", val)
	}

	if !strings.HasPrefix(v, "b") {
		return fmt.Errorf("val must start with 'b', is %v", v)
	}

	return nil
}

func testValidateStringArrayFn(val interface{}) error {
	v, ok := val.([]string)
	if !ok {
		return fmt.Errorf("invalid type for val, expected string, received %T", val)
	}

	if len(v) != 2 {
		return fmt.Errorf("val must contain 2 entries, is %v", v)
	}

	return nil
}

func testValidateIntMapFn(val interface{}) error {
	v, ok := val.(map[string]int64)
	if !ok {
		return fmt.Errorf("invalid type for val, expected int map, received %T", val)
	}

	// NB: for the purpose of this test, valid maps must have 3 values.
	if len(v) != 3 {
		return fmt.Errorf("val must contain 3 entries, has %v", v)
	}

	return nil
}

func testValidateTimeFn(val interface{}) error {
	v, ok := val.(time.Time)
	if !ok {
		return fmt.Errorf("invalid type for val, expected time.Time, received %T", val)
	}

	bound := testNow.Add(time.Minute)
	if v.After(bound) {
		return fmt.Errorf("val must be before %v, is %v", bound, v)
	}

	return nil
}

func stringSliceEquals(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}

	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
