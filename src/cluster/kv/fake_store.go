// Copyright (c) 2016 Uber Technologies, Inc.
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

package kv

import (
	"sync"

	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3x/watch"
)

// NewFakeStore returns a new in-process store that can be used for testing
func NewFakeStore() Store {
	return &fakeStore{
		values:     make(map[string]*fakeValue),
		watchables: make(map[string]xwatch.Watchable),
	}
}

// NewFakeValue returns a new fake Value around the given proto
func NewFakeValue(vers int, msg proto.Message) Value {
	data, _ := proto.Marshal(msg)
	return &fakeValue{
		version: vers,
		data:    data,
	}
}

// NewFakeValueWithData returns a new fake Value around the given data
func NewFakeValueWithData(vers int, data []byte) Value {
	return &fakeValue{
		version: vers,
		data:    data,
	}
}

type fakeValue struct {
	version int
	data    []byte
}

func (v fakeValue) Version() int                      { return v.version }
func (v fakeValue) Unmarshal(msg proto.Message) error { return proto.Unmarshal(v.data, msg) }

type fakeStore struct {
	sync.RWMutex
	values     map[string]*fakeValue
	watchables map[string]xwatch.Watchable
}

func (kv *fakeStore) Get(key string) (Value, error) {
	kv.RLock()
	defer kv.RUnlock()

	if val := kv.values[key]; val != nil {
		return val, nil
	}

	return nil, ErrNotFound
}

func (kv *fakeStore) Watch(key string) (ValueWatch, error) {
	kv.Lock()
	val := kv.values[key]

	watchable, ok := kv.watchables[key]
	if !ok {
		watchable = xwatch.NewWatchable()
		kv.watchables[key] = watchable
	}
	kv.Unlock()

	if !ok && val != nil {
		watchable.Update(val)
	}

	_, watch, _ := watchable.Watch()
	return NewValueWatch(watch), nil
}

func (kv *fakeStore) Set(key string, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	lastVersion := 0
	if val := kv.values[key]; val != nil {
		lastVersion = val.version
	}

	newVersion := lastVersion + 1
	fv := &fakeValue{
		version: newVersion,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateWatchable(key, fv)

	return newVersion, nil
}

func (kv *fakeStore) SetIfNotExists(key string, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	if _, exists := kv.values[key]; exists {
		return 0, ErrAlreadyExists
	}

	fv := &fakeValue{
		version: 1,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateWatchable(key, fv)

	return 1, nil
}

func (kv *fakeStore) CheckAndSet(key string, version int, val proto.Message) (int, error) {
	data, err := proto.Marshal(val)
	if err != nil {
		return 0, err
	}

	kv.Lock()
	defer kv.Unlock()

	if val, exists := kv.values[key]; exists {
		if val.version != version {
			return 0, ErrVersionMismatch
		}
	}

	newVersion := version + 1
	fv := &fakeValue{
		version: newVersion,
		data:    data,
	}
	kv.values[key] = fv
	kv.updateWatchable(key, fv)

	return newVersion, nil
}

// updateWatchable updates all subscriptions for the given key. It assumes
// the fakeStore write lock is acquired outside of this call
func (kv *fakeStore) updateWatchable(key string, newVal Value) {
	if watchable, ok := kv.watchables[key]; ok {
		watchable.Update(newVal)
	}
}
