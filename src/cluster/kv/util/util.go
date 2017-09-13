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
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

var (
	errNilStore = errors.New("kv store is nil")
)

// WatchAndUpdateBool sets up a watch with validation for a bool property. Any
// malformed or invalid updates are not applied. The default value is applied
// when the key does not exist in KV.
func WatchAndUpdateBool(
	store kv.Store,
	key string,
	property *bool,
	lock sync.Locker,
	defaultValue bool,
	opts Options,
) error {
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
// when the key does not exist in KV.
func WatchAndUpdateFloat64(
	store kv.Store,
	key string,
	property *float64,
	lock sync.Locker,
	defaultValue float64,
	opts Options,
) error {
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
// the key does not exist in KV.
func WatchAndUpdateInt64(
	store kv.Store,
	key string,
	property *int64,
	lock sync.Locker,
	defaultValue int64,
	opts Options,
) error {
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
// the key does not exist in KV.
func WatchAndUpdateString(
	store kv.Store,
	key string,
	property *string,
	lock sync.Locker,
	defaultValue string,
	opts Options,
) error {
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
// is applied when the key does not exist in KV.
func WatchAndUpdateStringArray(
	store kv.Store,
	key string,
	property *[]string,
	lock sync.Locker,
	defaultValue []string,
	opts Options,
) error {
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
// when the key does not exist in KV.
func WatchAndUpdateTime(
	store kv.Store,
	key string,
	property *time.Time,
	lock sync.Locker,
	defaultValue time.Time,
	opts Options,
) error {
	if opts == nil {
		opts = NewOptions()
	}
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(time.Time) }, lock)

	return watchAndUpdate(
		store, key, getTime, updateFn, opts.ValidateFn(), defaultValue, opts.Logger(),
	)
}

// BoolFromValue get a bool from kv.Value. If the value is nil, the default value
// is returned.
func BoolFromValue(v kv.Value, key string, defaultValue bool, opts Options) (bool, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res bool
	updateFn := func(i interface{}) { res = i.(bool) }

	if err := updateWithKV(
		getBool, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return false, err
	}

	return res, nil
}

// Float64FromValue gets a float64 from kv.Value. If the value is nil, the default
// value is returned.
func Float64FromValue(v kv.Value, key string, defaultValue float64, opts Options) (float64, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res float64
	updateFn := func(i interface{}) { res = i.(float64) }

	if err := updateWithKV(
		getFloat64, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return 0, err
	}

	return res, nil
}

// Int64FromValue gets an int64 from kv.Value. If the value is nil, the default
// value is returned.
func Int64FromValue(v kv.Value, key string, defaultValue int64, opts Options) (int64, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res int64
	updateFn := func(i interface{}) { res = i.(int64) }

	if err := updateWithKV(
		getInt64, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return 0, err
	}

	return res, nil
}

// StringFromValue gets a string from kv.Value. If the value is nil, the default
// value is returned.
func StringFromValue(v kv.Value, key string, defaultValue string, opts Options) (string, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res string
	updateFn := func(i interface{}) { res = i.(string) }

	if err := updateWithKV(
		getString, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return "", err
	}

	return res, nil
}

// StringArrayFromValue gets a string array from kv.Value. If the value is nil,
// the default value is returned.
func StringArrayFromValue(
	v kv.Value, key string, defaultValue []string, opts Options,
) ([]string, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res []string
	updateFn := func(i interface{}) { res = i.([]string) }

	if err := updateWithKV(
		getStringArray, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return nil, err
	}

	return res, nil
}

// TimeFromValue gets a time.Time from kv.Value. If the value is nil, the
// default value is returned.
func TimeFromValue(
	v kv.Value, key string, defaultValue time.Time, opts Options,
) (time.Time, error) {
	if opts == nil {
		opts = NewOptions()
	}

	var res time.Time
	updateFn := func(i interface{}) { res = i.(time.Time) }

	if err := updateWithKV(
		getTime, updateFn, opts.ValidateFn(), key, v, defaultValue, opts.Logger(),
	); err != nil {
		return time.Time{}, err
	}

	return res, nil
}

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

func getBool(v kv.Value) (interface{}, error) {
	var boolProto commonpb.BoolProto
	err := v.Unmarshal(&boolProto)
	return boolProto.Value, err
}

func getFloat64(v kv.Value) (interface{}, error) {
	var float64proto commonpb.Float64Proto
	err := v.Unmarshal(&float64proto)
	return float64proto.Value, err
}

func getInt64(v kv.Value) (interface{}, error) {
	var int64Proto commonpb.Int64Proto
	if err := v.Unmarshal(&int64Proto); err != nil {
		return 0, err
	}

	return int64Proto.Value, nil
}

func getString(v kv.Value) (interface{}, error) {
	var stringProto commonpb.StringProto
	if err := v.Unmarshal(&stringProto); err != nil {
		return 0, err
	}

	return stringProto.Value, nil
}

func getStringArray(v kv.Value) (interface{}, error) {
	var stringArrProto commonpb.StringArrayProto
	if err := v.Unmarshal(&stringArrProto); err != nil {
		return nil, err
	}

	return stringArrProto.Values, nil
}

func getTime(v kv.Value) (interface{}, error) {
	var int64Proto commonpb.Int64Proto
	if err := v.Unmarshal(&int64Proto); err != nil {
		return nil, err
	}

	return time.Unix(int64Proto.Value, 0), nil
}

func watchAndUpdate(
	store kv.Store,
	key string,
	getValue getValueFn,
	update updateFn,
	validate ValidateFn,
	defaultValue interface{},
	logger xlog.Logger,
) error {
	if store == nil {
		return errNilStore
	}

	var (
		watch kv.ValueWatch
		err   error
	)

	watch, err = store.Watch(key)
	if err != nil {
		return fmt.Errorf("could not establish initial watch: %v", err)
	}

	go func() {
		for range watch.C() {
			updateWithKV(getValue, update, validate, key, watch.Get(), defaultValue, logger)
		}
		// The channel for a ValueWatch should never close.
		getLogger(logger).
			WithFields(xlog.NewLogField("key", key)).
			Error("watch unexpectedly closed")
	}()

	return err
}

func updateWithKV(
	getValue getValueFn,
	update updateFn,
	validate ValidateFn,
	key string,
	v kv.Value,
	defaultValue interface{},
	logger xlog.Logger,
) error {
	if v == nil {
		// The key is deleted from kv, use the default value.
		update(defaultValue)
		logNilUpdate(logger, key, defaultValue)
		return nil
	}

	newValue, err := getValue(v)
	if err != nil {
		logMalformedUpdate(logger, key, v.Version(), newValue, err)
		return err
	}

	if validate != nil {
		if err := validate(newValue); err != nil {
			logInvalidUpdate(logger, key, v.Version(), newValue, err)
			return err
		}
	}

	update(newValue)
	logUpdateSuccess(logger, key, v.Version(), newValue)
	return nil
}

func logNilUpdate(logger xlog.Logger, k string, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("default-value", v),
	).Warn("nil value from kv store, applying default value")
}

func logMalformedUpdate(logger xlog.Logger, k string, ver int, v interface{}, err error) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("malformed-value", v),
		xlog.NewLogField("version", ver),
		xlog.NewLogField("error", err),
	).Warn("malformed value from kv store, not applying update")
}

func logInvalidUpdate(logger xlog.Logger, k string, ver int, v interface{}, err error) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("invalid-value", v),
		xlog.NewLogField("version", ver),
		xlog.NewLogField("error", err),
	).Warn("invalid value from kv store, not applying update")
}

func logUpdateSuccess(logger xlog.Logger, k string, ver int, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
		xlog.NewLogField("version", ver),
	).Info("value update success")
}

func getLogger(logger xlog.Logger) xlog.Logger {
	if logger == nil {
		return xlog.NullLogger
	}
	return logger
}
