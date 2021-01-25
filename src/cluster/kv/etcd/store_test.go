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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"testing"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/kvtest"
	"github.com/m3db/m3/src/cluster/kv"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/retry"

	"github.com/golang/protobuf/proto"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/integration"
	"golang.org/x/net/context"
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

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	value, err := store.Get("foo")
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

	store, err := NewStore(ec, opts)
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
	store, err = NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Set("foo", genProto("bar1"))
	require.Error(t, err)

	_, err = store.Get("foo")
	require.Error(t, err)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestCacheDirCreation(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	tdir, err := ioutil.TempDir("", "m3tests")
	require.NoError(t, err)
	defer os.RemoveAll(tdir)

	cdir := path.Join(tdir, "testCache")
	opts = opts.SetCacheFileFn(func(string) string {
		return path.Join(cdir, opts.Prefix())
	})

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	info, err := os.Stat(cdir)
	require.NoError(t, err)
	require.Equal(t, info.IsDir(), true)

	_, err = store.Set("foo", genProto("bar"))
	require.NoError(t, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 1)
	require.Equal(t, 0, len(store.(*client).cacheUpdatedCh))
}

func TestCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)

	f, err := ioutil.TempFile("", "")
	require.NoError(t, err)

	opts = opts.SetCacheFileFn(func(string) string {
		return f.Name()
	})

	store, err := NewStore(ec, opts)
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
	store, err = NewStore(ec, opts)
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

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	version, err := store.SetIfNotExists("foo", genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	_, err = store.SetIfNotExists("foo", genProto("bar"))
	require.Equal(t, kv.ErrAlreadyExists, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 1)
}

func TestCheckAndSet(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.Equal(t, kv.ErrVersionMismatch, err)

	version, err := store.CheckAndSet("foo", 0, genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	_, err = store.CheckAndSet("foo", 1, genProto("bar"))
	require.Equal(t, kv.ErrVersionMismatch, err)

	value, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, value, "bar", 2)
}

func TestWatchClose(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
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
		time.Sleep(1 * time.Millisecond)
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

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	w, err := store.Watch("foo")
	require.NoError(t, err)
	require.Nil(t, w.Get())

	var (
		doneCh      = make(chan struct{})
		lastVersion = 50
	)

	go func() {
		for i := 1; i <= lastVersion; i++ {
			_, err := store.Set("foo", genProto(fmt.Sprintf("bar%d", i)))
			assert.NoError(t, err)
		}
	}()

	go func() {
		defer close(doneCh)
		for {
			<-w.C()
			value := w.Get()
			if value.Version() == lastVersion {
				return
			}
		}
	}()

	select {
	case <-time.After(5 * time.Second):
		t.Fatal("test timed out")
	case <-doneCh:
	}
	verifyValue(t, w.Get(), fmt.Sprintf("bar%d", lastVersion), lastVersion)
}

func TestWatchFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
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

	store, err := NewStore(ec, opts)
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
	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	c := store.(*client)
	_, err = c.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	val, err := c.getFromKVStore("foo2")
	require.NoError(t, err)
	require.Nil(t, val)
}

func TestMultipleWatchesFromExist(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
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

	store, err := NewStore(ec, opts)
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
	ecluster, opts, closeFn := testCluster(t)
	defer closeFn()

	ec := ecluster.Client(0)

	opts = opts.SetWatchChanResetInterval(200 * time.Millisecond).SetWatchChanInitTimeout(500 * time.Millisecond)

	store, err := NewStore(ec, opts)
	require.NoError(t, err)
	c := store.(*client)

	_, err = c.Set("foo", genProto("bar1"))
	require.NoError(t, err)

	before := time.Now()
	ecluster.Members[0].Blackhole()
	w1, err := c.Watch("foo")
	require.WithinDuration(t, time.Now(), before, 100*time.Millisecond)
	require.NoError(t, err)
	require.Equal(t, 0, len(w1.C()))
	ecluster.Members[0].Unblackhole()
	ecluster.Members[0].DropConnections()

	// watch channel will error out, but Get() will be tried
	<-w1.C()
	verifyValue(t, w1.Get(), "bar1", 1)

	w1.Close()
}

func TestHistory(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.History("k1", 10, 5)
	require.Error(t, err)

	_, err = store.History("k1", 0, 5)
	require.Error(t, err)

	_, err = store.History("k1", -5, 0)
	require.Error(t, err)

	totalVersion := 10
	for i := 1; i <= totalVersion; i++ {
		store.Set("k1", genProto(fmt.Sprintf("bar%d", i)))
		store.Set("k2", genProto(fmt.Sprintf("bar%d", i)))
	}

	res, err := store.History("k1", 5, 5)
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

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	_, err = store.Delete("foo")
	require.Equal(t, kv.ErrNotFound, err)

	version, err := store.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	version, err = store.Set("foo", genProto("bar2"))
	require.NoError(t, err)
	require.Equal(t, 2, version)

	v, err := store.Get("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar2", 2)

	v, err = store.Delete("foo")
	require.NoError(t, err)
	verifyValue(t, v, "bar2", 2)

	_, err = store.Delete("foo")
	require.Equal(t, kv.ErrNotFound, err)

	_, err = store.Get("foo")
	require.Equal(t, kv.ErrNotFound, err)

	version, err = store.SetIfNotExists("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)
}

func TestDelete_UpdateCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	c, err := NewStore(ec, opts)
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

func TestDelete_UpdateWatcherCache(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	setStore, err := NewStore(ec, opts)
	require.NoError(t, err)

	setClient := setStore.(*client)
	version, err := setClient.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	setV, err := setClient.Get("foo")
	require.NoError(t, err)
	verifyValue(t, setV, "bar1", 1)
	require.Equal(t, 1, len(setClient.cache.Values))
	require.Equal(t, 1, len(setClient.cacheUpdatedCh))

	// drain the notification to ensure set received update
	<-setClient.cacheUpdatedCh

	// make a custom cache path for the get client
	clientCachePath, err := ioutil.TempDir("", "client-cache-dir")
	require.NoError(t, err)
	defer os.RemoveAll(clientCachePath)

	getStore, err := NewStore(ec, opts.SetCacheFileFn(func(ns string) string {
		nsFile := path.Join(clientCachePath, fmt.Sprintf("%s.json", ns))
		return nsFile
	}))
	require.NoError(t, err)
	getClient := getStore.(*client)

	getW, err := getClient.Watch("foo")
	require.NoError(t, err)
	<-getW.C()
	verifyValue(t, getW.Get(), "bar1", 1)
	require.True(t, xclock.WaitUntil(func() bool {
		getClient.cache.RLock()
		defer getClient.cache.RUnlock()
		return 1 == len(getClient.cache.Values)
	}, time.Minute))

	var (
		originalBytes []byte
	)
	require.True(t, xclock.WaitUntil(func() bool {
		originalBytes, err = readCacheJSON(clientCachePath)
		return err == nil
	}, time.Minute))

	setV, err = setClient.Delete("foo")
	require.NoError(t, err)
	verifyValue(t, setV, "bar1", 1)

	// make sure the cache is cleaned up on set client
	require.Equal(t, 0, len(setClient.cache.Values))
	require.Equal(t, 1, len(setClient.cacheUpdatedCh))

	// make sure the cache is cleaned up on get client (mem and disk)
	var (
		updatedBytes []byte
	)
	require.True(t, xclock.WaitUntil(func() bool {
		getClient.cache.RLock()
		defer getClient.cache.RUnlock()
		if len(getClient.cache.Values) != 0 {
			return false
		}
		updatedBytes, err = readCacheJSON(clientCachePath)
		if err != nil {
			return false
		}
		return !bytes.Equal(updatedBytes, originalBytes)
	}, time.Minute), "did not observe any invalidation of the cache file")
}

func TestDelete_TriggerWatch(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
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

func TestStaleDelete__FromGet(t *testing.T) {
	// in this test we ensure clients who did not receive a delete for a key in
	// their caches, evict the value in their cache the next time they communicate
	// with an etcd which is unaware of the key (e.g. it's been compacted).

	// first, we find the bytes required to be created in the cache file
	serverCachePath, err := ioutil.TempDir("", "server-cache-dir")
	require.NoError(t, err)
	defer os.RemoveAll(serverCachePath)
	ec, opts, closeFn := testStore(t)

	setStore, err := NewStore(ec, opts.SetCacheFileFn(func(ns string) string {
		return path.Join(serverCachePath, fmt.Sprintf("%s.json", ns))
	}))
	require.NoError(t, err)

	setClient := setStore.(*client)
	version, err := setClient.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	setV, err := setClient.Get("foo")
	require.NoError(t, err)
	verifyValue(t, setV, "bar1", 1)
	require.Equal(t, 1, len(setClient.cache.Values))

	// drain the notification to ensure set received update
	var (
		fileName   string
		cacheBytes []byte
	)
	require.True(t, xclock.WaitUntil(func() bool {
		fileName, cacheBytes, err = readCacheJSONAndFilename(serverCachePath)
		return err == nil && isValidJSON(cacheBytes)
	}, time.Minute), "timed out waiting to read cache file")
	closeFn()

	// make a new etcd cluster (to mimic the case where etcd has deleted and compacted
	// the value).
	newServerCachePath, err := ioutil.TempDir("", "new-server-cache-dir")
	require.NoError(t, err)
	defer os.RemoveAll(newServerCachePath)

	// write the cache file with correct contents in the new location
	f, err := os.Create(path.Join(newServerCachePath, fileName))
	require.NoError(t, err)
	_, err = f.Write(cacheBytes)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	require.True(t, xclock.WaitUntil(func() bool {
		_, newBytes, err := readCacheJSONAndFilename(newServerCachePath)
		return err == nil && bytes.Equal(cacheBytes, newBytes)
	}, time.Minute), "timed out waiting to flush new cache file")

	// create new etcd cluster
	ec2, opts, closeFn2 := testStore(t)
	defer closeFn2()
	getStore, err := NewStore(ec2, opts.SetCacheFileFn(func(ns string) string {
		nsFile := path.Join(newServerCachePath, fmt.Sprintf("%s.json", ns))
		return nsFile
	}))
	require.NoError(t, err)
	getClient := getStore.(*client)

	require.True(t, xclock.WaitUntil(func() bool {
		getClient.cache.RLock()
		defer getClient.cache.RUnlock()
		return 1 == len(getClient.cache.Values)
	}, time.Minute), "timed out waiting for client to read values from cache")

	// get value and ensure it's not able to serve the read
	v, err := getClient.Get("foo")
	require.Error(t, kv.ErrNotFound)
	require.Nil(t, v)

	require.True(t, xclock.WaitUntil(func() bool {
		_, updatedBytes, err := readCacheJSONAndFilename(newServerCachePath)
		return err == nil && !bytes.Equal(cacheBytes, updatedBytes)
	}, time.Minute), "timed out waiting to flush cache file delete")
}

func TestStaleDelete__FromWatch(t *testing.T) {
	// in this test we ensure clients who did not receive a delete for a key in
	// their caches, evict the value in their cache the next time they communicate
	// with an etcd which is unaware of the key (e.g. it's been compacted).
	// first, we find the bytes required to be created in the cache file
	serverCachePath, err := ioutil.TempDir("", "server-cache-dir")
	require.NoError(t, err)
	defer os.RemoveAll(serverCachePath)
	ec, opts, closeFn := testStore(t)

	setStore, err := NewStore(ec, opts.SetCacheFileFn(func(ns string) string {
		return path.Join(serverCachePath, fmt.Sprintf("%s.json", ns))
	}))
	require.NoError(t, err)

	setClient := setStore.(*client)
	version, err := setClient.Set("foo", genProto("bar1"))
	require.NoError(t, err)
	require.Equal(t, 1, version)

	setV, err := setClient.Get("foo")
	require.NoError(t, err)
	verifyValue(t, setV, "bar1", 1)
	require.Equal(t, 1, len(setClient.cache.Values))

	// drain the notification to ensure set received update
	var (
		fileName   string
		cacheBytes []byte
	)
	require.True(t, xclock.WaitUntil(func() bool {
		fileName, cacheBytes, err = readCacheJSONAndFilename(serverCachePath)
		// Need to make sure it is valid JSON to ensure we're not reading the
		// bytes before they've been completely written out.
		return err == nil && isValidJSON(cacheBytes)
	}, time.Minute), "timed out waiting to read cache file")
	closeFn()

	// make a new etcd cluster (to mimic the case where etcd has deleted and compacted
	// the value).
	newServerCachePath, err := ioutil.TempDir("", "new-server-cache-dir")
	require.NoError(t, err)
	defer os.RemoveAll(newServerCachePath)

	// write the cache file with correct contents in the new location
	f, err := os.Create(path.Join(newServerCachePath, fileName))
	require.NoError(t, err)
	_, err = f.Write(cacheBytes)
	require.NoError(t, err)
	require.NoError(t, f.Sync())
	require.NoError(t, f.Close())

	require.True(t, xclock.WaitUntil(func() bool {
		_, newBytes, err := readCacheJSONAndFilename(newServerCachePath)
		return err == nil && bytes.Equal(cacheBytes, newBytes) && isValidJSON(newBytes)
	}, time.Minute), "timed out waiting to flush new cache file")

	// create new etcd cluster
	ec2, opts, closeFn2 := testStore(t)
	defer closeFn2()
	getStore, err := NewStore(ec2, opts.SetCacheFileFn(func(ns string) string {
		nsFile := path.Join(newServerCachePath, fmt.Sprintf("%s.json", ns))
		return nsFile
	}))
	require.NoError(t, err)
	getClient := getStore.(*client)

	require.True(t, xclock.WaitUntil(func() bool {
		getClient.cache.RLock()
		defer getClient.cache.RUnlock()
		return 1 == len(getClient.cache.Values)
	}, time.Minute), "timed out waiting for client to read values from cache")
	require.Equal(t, 1, len(getClient.cache.Values))

	// get value and ensure it's not able to serve the read
	w, err := getClient.Watch("foo")
	require.NoError(t, err)
	require.NotNil(t, w)
	require.Nil(t, w.Get())

	require.True(t, xclock.WaitUntil(func() bool {
		_, updatedBytes, err := readCacheJSONAndFilename(newServerCachePath)
		return err == nil && !bytes.Equal(cacheBytes, updatedBytes)
	}, time.Minute), "timed out waiting to flush cache file delete")
	require.Equal(t, 0, len(getClient.cache.Values))
	require.Nil(t, w.Get())
}

func isValidJSON(b []byte) bool {
	var j interface{}
	return json.Unmarshal(b, &j) == nil
}

func TestTxn(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	store, err := NewStore(ec, opts)
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

	store, err := NewStore(ec, opts)
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

	store, err := NewStore(ec, opts)
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

// TestWatchWithStartRevision that watching from 1) an old compacted start
// revision and 2) a start revision in the future are both safe
func TestWatchWithStartRevision(t *testing.T) {
	tests := map[string]int64{
		"old_revision":    1,
		"future_revision": 100000,
	}

	for name, rev := range tests {
		t.Run(name, func(t *testing.T) {
			ec, opts, closeFn := testStore(t)
			defer closeFn()

			opts = opts.SetWatchWithRevision(rev)

			store, err := NewStore(ec, opts)
			require.NoError(t, err)

			for i := 1; i <= 50; i++ {
				_, err = store.Set("foo", genProto(fmt.Sprintf("bar-%d", i)))
				require.NoError(t, err)
			}

			cl := store.(*client).kv

			resp, err := cl.Get(context.Background(), "foo")
			require.NoError(t, err)
			compactRev := resp.Header.Revision

			_, err = cl.Compact(context.Background(), compactRev-1)
			require.NoError(t, err)

			w1, err := store.Watch("foo")
			require.NoError(t, err)
			<-w1.C()
			verifyValue(t, w1.Get(), "bar-50", 50)
		})
	}
}

func TestSerializedGets(t *testing.T) {
	ec, opts, closeFn := testStore(t)
	defer closeFn()

	opts = opts.SetEnableFastGets(true)
	require.NoError(t, opts.Validate())

	store, err := NewStore(ec, opts)
	require.NoError(t, err)

	v, err := store.Set("foo", genProto("bar"))
	require.EqualValues(t, 1, v)
	require.NoError(t, err)

	val, err := store.Get("foo")
	verifyValue(t, val, "bar", 1)
	require.NoError(t, err)

	v, err = store.Set("foo", genProto("42"))
	require.EqualValues(t, 2, v)
	require.NoError(t, err)

	val, err = store.Get("foo")
	verifyValue(t, val, "42", 2)
	require.NoError(t, err)
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

func testCluster(t *testing.T) (*integration.ClusterV3, Options, func()) {
	ecluster := integration.NewClusterV3(t, &integration.ClusterConfig{Size: 1})
	closer := func() {
		ecluster.Terminate(t)
	}

	opts := NewOptions().
		SetWatchChanCheckInterval(100 * time.Millisecond).
		SetWatchChanResetInterval(200 * time.Millisecond).
		SetWatchChanInitTimeout(200 * time.Millisecond).
		SetRequestTimeout(200 * time.Millisecond).
		SetRetryOptions(retry.NewOptions().SetMaxRetries(1).SetMaxBackoff(0)).
		SetPrefix("test")

	return ecluster, opts, closer
}

func testStore(t *testing.T) (*clientv3.Client, Options, func()) {
	ecluster, opts, closer := testCluster(t)
	return ecluster.RandClient(), opts, closer
}

func readCacheJSONAndFilename(dirPath string) (string, []byte, error) {
	files, err := ioutil.ReadDir(dirPath)
	if err != nil {
		return "", nil, err
	}
	if len(files) != 1 {
		return "", nil, fmt.Errorf("expected 1 file, found files: %+v", files)
	}
	fileName := files[0].Name()
	filepath := path.Join(dirPath, fileName)
	f, err := os.Open(filepath)
	if err != nil {
		return "", nil, err
	}
	b, err := ioutil.ReadAll(f)
	if err != nil {
		return "", nil, err
	}
	return fileName, b, nil
}

func readCacheJSON(dirPath string) ([]byte, error) {
	_, b, err := readCacheJSONAndFilename(dirPath)
	return b, err
}
