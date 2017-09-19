package util

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"
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
		if !valueFn() {
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
