package util

import (
	"sync"
	"time"

	"github.com/m3db/m3cluster/kv"
)

func lockedUpdate(fn updateFn, lock sync.Locker) updateFn {
	return func(i interface{}) {
		if lock != nil {
			lock.Lock()
		}

		fn(i)

		if lock != nil {
			lock.Unlock()
		}
	}
}

// WatchAndUpdateBool sets up a watch with validation for a bool property. Any
// malformed or invalid updates are not applied. The default value is applied
// when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateBool(
	store kv.Store,
	key string,
	property *bool,
	lock sync.Locker,
	defaultValue bool,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(bool) }, lock)

	return watchAndUpdate(
		store, key, getBool, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateFloat64 sets up a watch with validation for a float64 property.
// Any malformed or invalid updates are not applied. The default value is applied
// when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateFloat64(
	store kv.Store,
	key string,
	property *float64,
	lock sync.Locker,
	defaultValue float64,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(float64) }, lock)

	return watchAndUpdate(
		store, key, getFloat64, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateInt64 sets up a watch with validation for an int64 property. Any
// malformed or invalid updates are not applied. The default value is applied when
// the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateInt64(
	store kv.Store,
	key string,
	property *int64,
	lock sync.Locker,
	defaultValue int64,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(int64) }, lock)

	return watchAndUpdate(
		store, key, getInt64, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateString sets up a watch with validation for a string property. Any
// malformed or invalid updates are not applied. The default value is applied when
// the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateString(
	store kv.Store,
	key string,
	property *string,
	lock sync.Locker,
	defaultValue string,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(string) }, lock)

	return watchAndUpdate(
		store, key, getString, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateStringArray sets up a watch with validation for a string array
// property. Any malformed, or invalid updates are not applied. The default value
// is applied when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateStringArray(
	store kv.Store,
	key string,
	property *[]string,
	lock sync.Locker,
	defaultValue []string,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.([]string) }, lock)

	return watchAndUpdate(
		store, key, getStringArray, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateTime sets up a watch with validation for a time property. Any
// malformed, or invalid updates are not applied. The default value is applied
// when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateTime(
	store kv.Store,
	key string,
	property *time.Time,
	lock sync.Locker,
	defaultValue time.Time,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(time.Time) }, lock)

	return watchAndUpdate(
		store, key, getTime, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}
