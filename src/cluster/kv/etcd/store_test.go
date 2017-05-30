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

package etcd

import (
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/coreos/etcd/integration"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/generated/proto/kvtest"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3cluster/mocks"
	"github.com/stretchr/testify/require"
)

func TestValue(t *testing.T) {
	v1 := newValue(nil, 2, 100)
	require.Equal(t, 2, v1.Version())

	v2 := newValue(nil, 1, 200)
	require.Equal(t, 1, v2.Version())

	require.True(t, v2.IsNewer(v1))
	require.False(t, v1.IsNewer(v1))
	require.False(t, v1.IsNewer(v2))
}

func TestGetAndSet(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	value, err := store.Get("foo")
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)
	require.Nil(t, value)

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	version, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar2", 2)
}

func TestNoCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	// the will send a notification but won't trigger a sync
	// because no cache file is set
	require.Equal(t, 1, len(store.(*client).cacheUpdatedCh))

	closeFn()

	// from cache
	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	// new store but no cache file set
	store, err = NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.Error(t, err)

	_, err = store.Get("foo")
	require.Error(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)

	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	opts = opts.SetCacheFileFn(func(string) string {
		return f.Name()
	})

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	for {
		// the notification should be picked up and trigger a sync
		if len(store.(*client).cacheUpdatedCh) == 0 {
			break
		}
	}
	closeFn()

	// from cache
	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	// new store but with cache file
	store, err = NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Set("key", genProto("bar1"))
	require.Error(t, err)

	_, err = store.Get("key")
	require.Error(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))

	value, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestSetIfNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	version, err := store.SetIfNotExists("foo", genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrAlreadyExists, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 1)
}

func TestCheckAndSet(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	version, err := store.CheckAndSet("foo", 1, genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	version, err = store.SetIfNotExists("foo", genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.Error(t, err)
	require.Equal(t, kv.ErrVersionMismatch, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 2)
}

func TestWatchClose(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	w1, err := store.Watch("foo")
	require.NoError(t, err)
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	c := store.(*client)
	_, ok := c.watchables["test/foo"]
	require.True(t, ok)

	// closing w1 will close the go routine for the watch updates
	w1.Close()

	// waits until the original watchable is cleaned up
	for {
		c.RLock()
		_, ok = c.watchables["test/foo"]
		c.RUnlock()
		if !ok {
			break
		}
	}

	// getting a new watch will create a new watchale and thread to watch for updates
	w2, err := store.Watch("foo")
	require.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar1", 1)

	// verify that w1 will no longer be updated because the original watchable is closed
	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	<-w2.C()
	verifyValue(t, w2.Get(), "bar2", 2)
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
	w2.Close()
}

func TestWatchLastVersion(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	w, err := store.Watch("foo")
	require.NoError(t, err)
	require.Nil(t, w.Get())

	lastVersion := 100
	go func() {
		for i := 1; i <= lastVersion; i++ {
			_, err := store.Set("foo", genProto(fmt.Sprintf("bar%d", i)))
			require.NoError(t, err)
		}
	}()

	for {
		<-w.C()
		value := w.Get()
		if value.Version() == lastVersion {
			break
		}
	}
	verifyValue(t, w.Get(), fmt.Sprintf("bar%d", lastVersion), lastVersion)

	w.Close()
}

func TestWatchFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar1", 1)

	w, err := store.Watch("foo")
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar3", 3)

	w.Close()
}

func TestWatchFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	w, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w.C()))
	require.Nil(t, w.Get())

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w.C()
	require.Equal(t, 0, len(w.C()))
	verifyValue(t, w.Get(), "bar2", 2)

	w.Close()
}

func TestGetFromKvNotFound(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()
	store, err := NewStore(ec, ec, opts)
	c := store.(*client)
	_, err = c.Set("foo", genProto("bar1"))

	val, err := c.getFromKVStore("foo2")
	require.Nil(t, val)
	require.Nil(t, err)
}

func TestMultipleWatchesFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	w1, err := store.Watch("foo")
	require.NoError(t, err)

	w2, err := store.Watch("foo")
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	_, err = store.Set("foo", genProto("bar3"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar3", 3)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar3", 3)

	w1.Close()
	w2.Close()
}

func TestMultipleWatchesFromNotExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)
	w1, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))
	require.Nil(t, w1.Get())

	w2, err := store.Watch("foo")
	require.NoError(t, err)
	require.Equal(t, 0, len(w2.C()))
	require.Nil(t, w2.Get())

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar1", 1)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-w1.C()
	require.Equal(t, 0, len(w1.C()))
	verifyValue(t, w1.Get(), "bar2", 2)

	<-w2.C()
	require.Equal(t, 0, len(w2.C()))
	verifyValue(t, w2.Get(), "bar2", 2)

	w1.Close()
	w2.Close()
}

func TestWatchNonBlocking(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	opts = opts.SetWatchChanResetInterval(200 * time.Millisecond).SetWatchChanInitTimeout(200 * time.Millisecond)

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)
	c := store.(*client)

	failTotal := 3
	mw := mocks.NewBlackholeWatcher(ec, failTotal, func() { time.Sleep(time.Minute) })
	c.watcher = mw

	_, err = c.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	before := time.Now()
	w1, err := c.Watch("foo")
	require.WithinDuration(t, time.Now(), before, 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))

	// watch channel will error out, but Get() will be tried
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
}

func TestHistory(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	res, err := store.History("k1", 10, 5)
	require.Error(t, err)

	res, err = store.History("k1", 0, 5)
	require.Error(t, err)

	res, err = store.History("k1", -5, 0)
	require.Error(t, err)

	totalVersion := 10
	for i := 1; i <= totalVersion; i++ {
		store.Set("k1", genProto(fmt.Sprintf("bar%d", i)))
		store.Set("k2", genProto(fmt.Sprintf("bar%d", i)))
	}

	res, err = store.History("k1", 5, 5)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))

	res, err = store.History("k1", 15, 20)
	require.NoError(t, err)
	require.Equal(t, 0, len(res))

	res, err = store.History("k1", 6, 10)
	require.NoError(t, err)
	require.Equal(t, 4, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 6
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}

	res, err = store.History("k1", 3, 7)
	require.NoError(t, err)
	require.Equal(t, 4, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 3
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}

	res, err = store.History("k1", 5, 15)
	require.NoError(t, err)
	require.Equal(t, totalVersion-5+1, len(res))
	for i := 0; i < len(res); i++ {
		version := i + 5
		value := res[i]
		verifyValue(t, value, fmt.Sprintf("bar%d", version), version)
	}
}

func TestDelete(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	v, err := store.Delete("foo")
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	v, err = store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar2", 2)

	v, err = store.Delete("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar2", 2)

	v, err = store.Delete("foo")
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	v, err = store.Get("foo")
	require.Error(t, err)
	require.Equal(t, kv.ErrNotFound, err)

	version, err = store.SetIfNotExists("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)
}

func TestDelete_UpdateCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	c, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	store := c.(*client)
	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	v, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar1", 1)
	require.Equal(t, 1, len(store.cache.Values))
	require.Equal(t, 1, len(store.cacheUpdatedCh))

	// drain the notification
	<-store.cacheUpdatedCh

	v, err = store.Delete("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar1", 1)
	// make sure the cache is cleaned up
	require.Equal(t, 0, len(store.cache.Values))
	require.Equal(t, 1, len(store.cacheUpdatedCh))
}

func TestDelete_TriggerWatch(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	vw, err := store.Watch("foo")
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	<-vw.C()
	verifyValue(t, vw.Get(), "bar1", 1)

	_, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)

	<-vw.C()
	verifyValue(t, vw.Get(), "bar2", 2)

	_, err = store.Delete("foo")
	require.NoError(t, err)

	<-vw.C()
	require.Nil(t, vw.Get())

	_, err = store.Set("foo", genProto("bar3"))
	require.NoError(t, err)

	<-vw.C()
	verifyValue(t, vw.Get(), "bar3", 1)
}

func TestTxn(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	r, err := store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("foo").
				SetValue(0),
		},
		[]kv.Op{kv.NewSetOp("foo", genProto("bar1"))},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(r.Responses()))
	require.Equal(t, "foo", r.Responses()[0].Key())
	require.Equal(t, kv.OpSet, r.Responses()[0].Type())
	require.Equal(t, 1, r.Responses()[0].Value())

	v, err := store.Set("key", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, v)

	r, err = store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("key").
				SetValue(1),
		},
		[]kv.Op{kv.NewSetOp("foo", genProto("bar1"))},
	)
	require.NoError(t, err)
	require.Equal(t, 1, len(r.Responses()))
	require.Equal(t, "foo", r.Responses()[0].Key())
	require.Equal(t, kv.OpSet, r.Responses()[0].Type())
	require.Equal(t, 2, r.Responses()[0].Value())

	r, err = store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("foo").
				SetValue(2),
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("key").
				SetValue(1),
		},
		[]kv.Op{
			kv.NewSetOp("key", genProto("bar1")),
			kv.NewSetOp("foo", genProto("bar2")),
		},
	)
	require.NoError(t, err)
	require.Equal(t, 2, len(r.Responses()))
	require.Equal(t, "key", r.Responses()[0].Key())
	require.Equal(t, kv.OpSet, r.Responses()[0].Type())
	require.Equal(t, 2, r.Responses()[0].Value())
	require.Equal(t, "foo", r.Responses()[1].Key())
	require.Equal(t, kv.OpSet, r.Responses()[1].Type())
	require.Equal(t, 3, r.Responses()[1].Value())
}

func TestTxn_ConditionFail(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("foo").
				SetValue(1),
		},
		[]kv.Op{kv.NewSetOp("foo", genProto("bar1"))},
	)
	require.Error(t, err)

	store.Set("key1", genProto("v1"))
	store.Set("key2", genProto("v2"))
	_, err = store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("key1").
				SetValue(1),
			kv.NewCondition().
				SetCompareType(kv.CompareEqual).
				SetTargetType(kv.TargetVersion).
				SetKey("key2").
				SetValue(2),
		},
		[]kv.Op{kv.NewSetOp("foo", genProto("bar1"))},
	)
	require.Error(t, err)
}

func TestTxn_UnknownType(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, ec, opts)
	require.NoError(t, err)

	_, err = store.Commit(
		[]kv.Condition{
			kv.NewCondition().
				SetTargetType(kv.TargetVersion).
				SetKey("foo").
				SetValue(1),
		},
		[]kv.Op{kv.NewSetOp("foo", genProto("bar1"))},
	)
	require.Equal(t, kv.ErrUnknownCompareType, err)
}

func verifyValue(t *testing.T, v kv.Value, value string, version int) {
	var testMsg kvtest.Foo
	err := v.Unmarshal(&testMsg)
	require.NoError(t, err)
	require.Equal(t, value, testMsg.Msg)
	require.Equal(t, version, v.Version())
}

func genProto(msg string) proto.Message {
	return &kvtest.Foo{Msg: msg}
}

func testStore(t *testing.T) (*clientv3.Client, Options, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	ec := ecluster.RandClient()

	closer := func() {
		ecluster.Terminate(t)
	}

	opts := NewOptions().
		SetWatchChanCheckInterval(10 * time.Millisecond).
		SetPrefix("test")

	return ec, opts, closer
}
