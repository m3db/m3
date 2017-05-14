package util

import (
	"errors"
	"sync"
	"time"

	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/log"
)

var errNilStore = errors.New("kv store is nil")

type getValueFn func(kv.Value) (interface{}, error)
type updateFn func(interface{})

// WatchAndUpdateBool sets up a watch for a bool property.
func WatchAndUpdateBool(store kv.Store, key string, property *bool, lock sync.Locker,
	defaultValue bool, logger xlog.Logger) (kv.ValueWatch, error) {
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(bool) }, lock)

	return watchAndUpdate(store, key, getBool, updateFn, defaultValue, logger)
}

// WatchAndUpdateFloat64 sets up a watch for an float64 property.
func WatchAndUpdateFloat64(store kv.Store, key string, property *float64, lock sync.Locker,
	defaultValue float64, logger xlog.Logger) (kv.ValueWatch, error) {
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(float64) }, lock)

	return watchAndUpdate(store, key, getFloat64, updateFn, defaultValue, logger)
}

// WatchAndUpdateInt64 sets up a watch for an int64 property.
func WatchAndUpdateInt64(store kv.Store, key string, property *int64, lock sync.Locker,
	defaultValue int64, logger xlog.Logger) (kv.ValueWatch, error) {
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(int64) }, lock)

	return watchAndUpdate(store, key, getInt64, updateFn, defaultValue, logger)
}

// WatchAndUpdateString sets up a watch for an string property.
func WatchAndUpdateString(store kv.Store, key string, property *string, lock sync.Locker,
	defaultValue string, logger xlog.Logger) (kv.ValueWatch, error) {
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(string) }, lock)

	return watchAndUpdate(store, key, getString, updateFn, defaultValue, logger)
}

// WatchAndUpdateTime sets up a watch for a time property.
func WatchAndUpdateTime(store kv.Store, key string, property *time.Time, lock sync.Locker,
	defaultValue time.Time, logger xlog.Logger) (kv.ValueWatch, error) {
	updateFn := lockedUpdate(func(i interface{}) { *property = i.(time.Time) }, lock)

	return watchAndUpdate(store, key, getTime, updateFn, defaultValue, logger)
}

// BoolFromValue get a bool from kv.Value
func BoolFromValue(v kv.Value, key string, defaultValue bool, logger xlog.Logger) bool {
	var res bool
	updateFn := func(i interface{}) { res = i.(bool) }

	updateWithKV(getBool, updateFn, key, v, defaultValue, logger)

	return res
}

// Float64FromValue gets an float64 from kv.Value
func Float64FromValue(v kv.Value, key string, defaultValue float64, logger xlog.Logger) float64 {
	var res float64
	updateFn := func(i interface{}) { res = i.(float64) }

	updateWithKV(getFloat64, updateFn, key, v, defaultValue, logger)

	return res
}

// Int64FromValue gets an int64 from kv.Value
func Int64FromValue(v kv.Value, key string, defaultValue int64, logger xlog.Logger) int64 {
	var res int64
	updateFn := func(i interface{}) { res = i.(int64) }

	updateWithKV(getInt64, updateFn, key, v, defaultValue, logger)

	return res
}

// StringFromValue gets an string from kv.Value
func StringFromValue(v kv.Value, key string, defaultValue string, logger xlog.Logger) string {
	var res string
	updateFn := func(i interface{}) { res = i.(string) }

	updateWithKV(getString, updateFn, key, v, defaultValue, logger)

	return res
}

// StringArrayFromValue gets a string array from kv.Value
func StringArrayFromValue(v kv.Value, key string, defaultValue []string, logger xlog.Logger) []string {
	var res []string
	updateFn := func(i interface{}) { res = i.([]string) }

	updateWithKV(getStringArray, updateFn, key, v, defaultValue, logger)

	return res
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

func watchAndUpdate(store kv.Store, key string, getValue getValueFn, update updateFn,
	defaultValue interface{}, logger xlog.Logger) (kv.ValueWatch, error) {
	if store == nil {
		return nil, errNilStore
	}

	watch, err := store.Watch(key)
	if err != nil {
		return nil, err
	}

	go func() {
		for {
			_, ok := <-watch.C()
			if !ok {
				return
			}
			updateWithKV(getValue, update, key, watch.Get(), defaultValue, logger)
		}
	}()

	return watch, err
}

func updateWithKV(getValue getValueFn, update updateFn, key string, v kv.Value,
	defaultValue interface{}, logger xlog.Logger) error {
	if v == nil {
		// the key is deleted from kv, use the default value
		update(defaultValue)
		logSetDefault(logger, key, defaultValue)
		return nil
	}

	newValue, err := getValue(v)
	if err != nil {
		update(defaultValue)
		logInvalidUpdate(logger, key, v.Version(), defaultValue, err)
		return err
	}

	update(newValue)
	logUpdateSuccess(logger, key, v.Version(), newValue)
	return nil
}

func logSetDefault(logger xlog.Logger, k string, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
	).Infof("nil value from kv store, use default value")
}

func logUpdateSuccess(logger xlog.Logger, k string, ver int, v interface{}) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
		xlog.NewLogField("version", ver),
	).Infof("value update success")
}

func logInvalidUpdate(logger xlog.Logger, k string, ver int, v interface{}, err error) {
	getLogger(logger).WithFields(
		xlog.NewLogField("key", k),
		xlog.NewLogField("value", v),
		xlog.NewLogField("version", ver),
		xlog.NewLogField("error", err),
	).Infof("invalid value from kv store, use default value")
}

func getLogger(logger xlog.Logger) xlog.Logger {
	if logger == nil {
		return xlog.NullLogger
	}
	return logger
}
