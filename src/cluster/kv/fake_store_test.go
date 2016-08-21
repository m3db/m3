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
