// Copyright (c) 2020  Uber Technologies, Inc.
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

package fake

import (
	"errors"

	"github.com/m3db/m3/src/cluster/kv"

	"github.com/gogo/protobuf/proto"
)

// NewStore returns a fakeStore adhering to the kv.Store interface.
// All methods except Watch are implemented. Implementation is not threadsafe
// and should only be used for tests.
func NewStore() kv.Store {
	return &fakeStore{
		store: make(map[string][]kv.Value),
	}
}

type fakeStore struct {
	store map[string][]kv.Value
}

func (f *fakeStore) Get(key string) (kv.Value, error) {
	value, ok := f.store[key]
	if !ok {
		return nil, kv.ErrNotFound
	}

	return value[len(value)-1], nil
}

func (f *fakeStore) Watch(_ string) (kv.ValueWatch, error) {
	panic("implement me")
}

func (f *fakeStore) Set(key string, v proto.Message) (int, error) {
	oldVal, err := f.Get(key)
	if err != nil && err != kv.ErrNotFound {
		return 0, err
	}

	data, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	var newVer int
	if oldVal == nil {
		f.store[key] = []kv.Value{newValue(data, int64(newVer))}
	} else {
		newVer = oldVal.Version() + 1
		f.store[key] = append(f.store[key], newValue(data, int64(newVer)))
	}

	return newVer, nil
}

func (f *fakeStore) SetIfNotExists(key string, v proto.Message) (int, error) {
	_, err := f.Get(key)
	if err == kv.ErrNotFound {
		return f.Set(key, v)
	} else {
		return 0, kv.ErrAlreadyExists
	}
}

func (f *fakeStore) CheckAndSet(key string, version int, v proto.Message) (int, error) {
	val, err := f.Get(key)
	if err != nil && err != kv.ErrNotFound {
		return 0, err
	} else if err != nil && err == kv.ErrNotFound && version == 0 {
		return f.Set(key, v)
	} else if val == nil {
		return 0, kv.ErrVersionMismatch
	} else if val.Version() == version {
		return f.Set(key, v)
	} else {
		return 0, kv.ErrVersionMismatch
	}
}

func (f *fakeStore) Delete(key string) (kv.Value, error) {
	val, err := f.Get(key)
	if err != nil {
		return nil, err
	}
	delete(f.store, key)

	return val, nil
}

func (f *fakeStore) History(key string, from, to int) ([]kv.Value, error) {
	if from > to || from < 0 || to < 0 {
		return nil, errors.New("invalid history range")
	}

	if from == to {
		return nil, nil
	}

	vals, ok := f.store[key]
	if !ok {
		return nil, kv.ErrNotFound
	}
	if to > len(vals) {
		return nil, errors.New("invalid history range")
	}

	return vals[from:to], nil
}

type value struct {
	Val []byte
	Ver int64
}

func newValue(val []byte, ver int64) *value {
	return &value{
		Val: val,
		Ver: ver,
	}
}

func (c *value) IsNewer(other kv.Value) bool {
	return c.Version() > other.Version()
}

func (c *value) Unmarshal(v proto.Message) error {
	err := proto.Unmarshal(c.Val, v)
	return err
}

func (c *value) Version() int {
	return int(c.Ver)
}
