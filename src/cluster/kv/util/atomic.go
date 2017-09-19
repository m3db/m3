package util

import (
	"github.com/m3db/m3cluster/kv"
	"go.uber.org/atomic"
)

// WatchAndUpdateAtomicBool sets up a watch with validation for an atomic bool
// property. Any malformed or invalid updates are not applied. The default value
// is applied when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateAtomicBool(
	store kv.Store,
	key string,
	property *atomic.Bool,
	defaultValue bool,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := func(i interface{}) { property.Store(i.(bool)) }

	return watchAndUpdate(
		store, key, getBool, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateAtomicFloat64 sets up a watch with validation for an atomic float64
// property. Any malformed or invalid updates are not applied. The default value is
// applied when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateAtomicFloat64(
	store kv.Store,
	key string,
	property *atomic.Float64,
	defaultValue float64,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := func(i interface{}) { property.Store(i.(float64)) }

	return watchAndUpdate(
		store, key, getFloat64, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateAtomicInt64 sets up a watch with validation for an atomic int64
// property. Anymalformed or invalid updates are not applied. The default value
// is applied when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateAtomicInt64(
	store kv.Store,
	key string,
	property *atomic.Int64,
	defaultValue int64,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := func(i interface{}) { property.Store(i.(int64)) }

	return watchAndUpdate(
		store, key, getInt64, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// WatchAndUpdateAtomicString sets up a watch with validation for an atomic string
// property. Any malformed or invalid updates are not applied. The default value
// is applied when the key does not exist in KV. The watch on the value is returned.
func WatchAndUpdateAtomicString(
	store kv.Store,
	key string,
	property *atomic.String,
	defaultValue string,
	opts Options,
) (kv.ValueWatch, error) {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := func(i interface{}) { property.Store(i.(string)) }

	return watchAndUpdate(
		store, key, getString, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}
