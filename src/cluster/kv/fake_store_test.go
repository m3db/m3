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
	"testing"

	"github.com/m3db/m3cluster/generated/proto/kvtest"
	"github.com/stretchr/testify/require"
)

func TestFakeStore(t *testing.T) {
	kv := NewFakeStore()

	// Should start without a value
	val, err := kv.Get("foo")
	require.Equal(t, ErrNotFound, err)
	require.Nil(t, val)

	// Should be able to set to non-existent value
	version, err := kv.SetIfNotExists("foo", &kvtest.Foo{
		Msg: "first",
	})
	require.NoError(t, err)
	require.Equal(t, 1, version)

	// And have that value stored
	val, err = kv.Get("foo")
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, 1, val.Version())

	var read kvtest.Foo
	require.NoError(t, val.Unmarshal(&read))
	require.Equal(t, "first", read.Msg)

	// Should not be able to SetIfNotExists to that value again
	_, err = kv.SetIfNotExists("foo", &kvtest.Foo{
		Msg: "update",
	})
	require.Equal(t, ErrAlreadyExists, err)

	read.Reset()
	val, err = kv.Get("foo")
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, 1, val.Version())
	require.NoError(t, val.Unmarshal(&read))
	require.Equal(t, "first", read.Msg)

	// Should be able to Set unconditionally and get a new version
	version2, err := kv.Set("foo", &kvtest.Foo{
		Msg: "update",
	})
	require.Nil(t, nil)
	require.Equal(t, 2, version2)

	read.Reset()
	val, err = kv.Get("foo")
	require.NoError(t, err)
	require.Equal(t, 2, val.Version())
	require.NoError(t, val.Unmarshal(&read))
	require.Equal(t, "update", read.Msg)

	// Should not be able to set at an old version
	_, err = kv.CheckAndSet("foo", version, &kvtest.Foo{
		Msg: "update2",
	})
	require.Equal(t, ErrVersionMismatch, err)

	read.Reset()
	val, err = kv.Get("foo")
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, 2, val.Version())
	require.NoError(t, val.Unmarshal(&read))
	require.Equal(t, "update", read.Msg)

	// Should be able to set at the specific version
	version3, err := kv.CheckAndSet("foo", val.Version(), &kvtest.Foo{
		Msg: "update3",
	})
	require.NoError(t, err)
	require.Equal(t, 3, version3)

	read.Reset()
	val, err = kv.Get("foo")
	require.NoError(t, err)
	require.NotNil(t, val)
	require.Equal(t, 3, val.Version())
	require.NoError(t, val.Unmarshal(&read))
	require.Equal(t, "update3", read.Msg)
}

func TestFakeStoreWatch(t *testing.T) {
	kv := NewFakeStore()

	fooWatch1, err := kv.Watch("foo")
	require.NoError(t, err)
	require.NotNil(t, fooWatch1)
	require.Nil(t, fooWatch1.Get())

	version, err := kv.SetIfNotExists("foo", &kvtest.Foo{
		Msg: "first",
	})
	require.NoError(t, err)
	require.Equal(t, 1, version)

	<-fooWatch1.C()
	var foo kvtest.Foo
	require.NoError(t, fooWatch1.Get().Unmarshal(&foo))
	require.Equal(t, "first", foo.Msg)

	fooWatch2, err := kv.Watch("foo")
	<-fooWatch2.C()
	require.NoError(t, fooWatch2.Get().Unmarshal(&foo))
	require.Equal(t, "first", foo.Msg)

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		<-fooWatch1.C()
		var foo kvtest.Foo
		require.NoError(t, fooWatch1.Get().Unmarshal(&foo))
		require.Equal(t, "second", foo.Msg)
	}()

	go func() {
		defer wg.Done()
		<-fooWatch2.C()
		var foo kvtest.Foo
		require.NoError(t, fooWatch2.Get().Unmarshal(&foo))
		require.Equal(t, "second", foo.Msg)
	}()

	version, err = kv.Set("foo", &kvtest.Foo{
		Msg: "second",
	})
	require.NoError(t, err)
	require.Equal(t, 2, version)
	wg.Wait()

	fooWatch1.Close()
	version, err = kv.Set("foo", &kvtest.Foo{
		Msg: "third",
	})
	require.NoError(t, err)
	require.Equal(t, 3, version)
	require.NoError(t, fooWatch2.Get().Unmarshal(&foo))
	require.Equal(t, "third", foo.Msg)
}

func TestFakeStoreErrors(t *testing.T) {
	kv := NewFakeStore()

	_, err := kv.Set("foo", nil)
	require.Error(t, err)

	_, err = kv.SetIfNotExists("foo", nil)
	require.Error(t, err)

	_, err = kv.CheckAndSet("foo", 1, nil)
	require.Error(t, err)
}
