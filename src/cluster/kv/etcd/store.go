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
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"sync"

	"github.com/coreos/etcd/clientv3"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3cluster/etcd/watchmanager"
	"github.com/m3db/m3cluster/kv"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/retry"
	"github.com/uber-go/tally"
	"golang.org/x/net/context"
)

const etcdVersionZero = 0

var (
	noopCancel func()

	errInvalidHistoryVersion = errors.New("invalid version range")
)

// NewStore creates a kv store based on etcd
func NewStore(c *clientv3.Client, opts Options) (kv.Store, error) {
	scope := opts.InstrumentsOptions().MetricsScope()

	store := &client{
		opts:           opts,
		kv:             c.KV,
		watcher:        c.Watcher,
		watchables:     map[string]kv.ValueWatchable{},
		retrier:        xretry.NewRetrier(opts.RetryOptions()),
		logger:         opts.InstrumentsOptions().Logger(),
		cacheFile:      opts.CacheFilePath(),
		cache:          newCache(),
		cacheUpdatedCh: make(chan struct{}, 1),
		m: clientMetrics{
			etcdGetError:   scope.Counter("etcd-get-error"),
			etcdPutError:   scope.Counter("etcd-put-error"),
			etcdTnxError:   scope.Counter("etcd-tnx-error"),
			diskWriteError: scope.Counter("disk-write-error"),
			diskReadError:  scope.Counter("disk-read-error"),
		},
	}
	wOpts := watchmanager.NewOptions().
		SetWatcher(c.Watcher).
		SetUpdateFn(store.update).
		SetTickAndStopFn(store.tickAndStop).
		SetWatchOptions([]clientv3.OpOption{
			// periodically (appx every 10 mins) checks for the latest data
			// with or without any update notification
			clientv3.WithProgressNotify(),
			// receive initial notification once the watch channel is created
			clientv3.WithCreatedNotify(),
		}).
		SetWatchChanCheckInterval(opts.WatchChanCheckInterval()).
		SetWatchChanInitTimeout(opts.WatchChanInitTimeout()).
		SetWatchChanResetInterval(opts.WatchChanResetInterval()).
		SetInstrumentsOptions(opts.InstrumentsOptions())

	wm, err := watchmanager.NewWatchManager(wOpts)
	if err != nil {
		return nil, err
	}

	store.wm = wm

	if store.cacheFile != "" {
		if err := store.initCache(); err != nil {
			store.logger.Warnf("could not load cache from file %s: %v", store.cacheFile, err)
		} else {
			store.logger.Infof("successfully loaded cache from file %s", store.cacheFile)
		}

		go func() {
			for range store.cacheUpdatedCh {
				store.writeCacheToFile()
			}
		}()
	}
	return store, nil
}

type client struct {
	sync.RWMutex

	opts           Options
	kv             clientv3.KV
	watcher        clientv3.Watcher
	watchables     map[string]kv.ValueWatchable
	retrier        xretry.Retrier
	logger         xlog.Logger
	m              clientMetrics
	cache          *valueCache
	cacheFile      string
	cacheUpdatedCh chan struct{}

	wm watchmanager.WatchManager
}

type clientMetrics struct {
	etcdGetError   tally.Counter
	etcdPutError   tally.Counter
	etcdTnxError   tally.Counter
	diskWriteError tally.Counter
	diskReadError  tally.Counter
}

// Get returns the latest value from etcd store and only fall back to
// in-memory cache if the remote store is unavailable
func (c *client) Get(key string) (kv.Value, error) {
	return c.get(c.opts.KeyFn()(key))
}

func (c *client) get(key string) (kv.Value, error) {
	ctx, cancel := c.context()
	defer cancel()

	r, err := c.kv.Get(ctx, key)
	if err != nil {
		c.m.etcdGetError.Inc(1)
		cachedV, ok := c.getCache(key)
		if ok {
			return cachedV, nil
		}
		return nil, err
	}

	if r.Count == 0 {
		return nil, kv.ErrNotFound
	}

	v := newValue(r.Kvs[0].Value, r.Kvs[0].Version, r.Kvs[0].ModRevision)

	c.mergeCache(key, v)

	return v, nil
}

func (c *client) History(key string, from, to int) ([]kv.Value, error) {
	if from > to || from < 0 || to < 0 {
		return nil, errInvalidHistoryVersion
	}

	if from == to {
		return nil, nil
	}

	newKey := c.opts.KeyFn()(key)

	ctx, cancel := c.context()
	defer cancel()

	r, err := c.kv.Get(ctx, newKey)
	if err != nil {
		return nil, err
	}

	if r.Count == 0 {
		return nil, kv.ErrNotFound
	}

	numValue := to - from

	latestKV := r.Kvs[0]
	version := int(latestKV.Version)
	modRev := latestKV.ModRevision

	if version < from {
		// no value available in the requested version range
		return nil, nil
	}

	if version-from+1 < numValue {
		// get the correct size of the result slice
		numValue = version - from + 1
	}

	res := make([]kv.Value, numValue)

	if version < to {
		// put it in the last element of the result
		res[version-from] = newValue(latestKV.Value, latestKV.Version, modRev)
	}

	for version > from {
		ctx, cancel := c.context()
		defer cancel()

		r, err = c.kv.Get(ctx, newKey, clientv3.WithRev(modRev-1))
		if err != nil {
			return nil, err
		}

		if r.Count == 0 {
			// unexpected
			return nil, fmt.Errorf("could not find version %d for key %s", version-1, key)
		}

		v := r.Kvs[0]
		modRev = v.ModRevision
		version = int(v.Version)
		if version < to {
			res[version-from] = newValue(v.Value, v.Version, v.ModRevision)
		}
	}

	return res, nil
}

func (c *client) Watch(key string) (kv.ValueWatch, error) {
	newKey := c.opts.KeyFn()(key)
	c.Lock()
	watchable, ok := c.watchables[newKey]
	if !ok {
		watchable = kv.NewValueWatchable()
		c.watchables[newKey] = watchable

		go c.wm.Watch(newKey)

	}
	c.Unlock()
	_, w, err := watchable.Watch()
	return w, err
}

func (c *client) update(key string) error {
	var (
		newValue kv.Value
		err      error
	)
	// we need retry here because if Get() failed on an watch update,
	// it has to wait 10 mins to be notified to try again
	if execErr := c.retrier.Attempt(func() error {
		newValue, err = c.get(key)
		if err == kv.ErrNotFound {
			// do not retry on ErrNotFound
			return xretry.NonRetryableError(err)
		}
		return err
	}); execErr != nil && xerrors.GetInnerNonRetryableError(execErr) != kv.ErrNotFound {
		return execErr
	}

	c.RLock()
	w, ok := c.watchables[key]
	c.RUnlock()
	if !ok {
		return fmt.Errorf("unexpected: no watchable found for key: %s", key)
	}

	curValue := w.Get()
	if curValue == nil && newValue == nil {
		// nothing to update
		return nil
	}

	if curValue == nil || newValue == nil {
		// got the first value or received a delete update
		return w.Update(newValue)
	}

	// now both curValue and newValue are valid, compare version
	if newValue.(*value).isNewer(curValue.(*value)) {
		return w.Update(newValue)
	}

	return nil
}

func (c *client) tickAndStop(key string) bool {
	// fast path
	c.RLock()
	watchable, ok := c.watchables[key]
	c.RUnlock()
	if !ok {
		c.logger.Warnf("unexpected: key %s is already cleaned up", key)
		return true
	}

	if watchable.NumWatches() != 0 {
		return false
	}

	// slow path
	c.Lock()
	defer c.Unlock()
	watchable, ok = c.watchables[key]
	if !ok {
		// not expect this to happen
		c.logger.Warnf("unexpected: key %s is already cleaned up", key)
		return true
	}

	if watchable.NumWatches() != 0 {
		// a new watch has subscribed to the watchable, do not clean up
		return false
	}

	watchable.Close()
	delete(c.watchables, key)
	return true
}

func (c *client) Set(key string, v proto.Message) (int, error) {
	ctx, cancel := c.context()
	defer cancel()

	value, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	r, err := c.kv.Put(ctx, c.opts.KeyFn()(key), string(value), clientv3.WithPrevKV())
	if err != nil {
		c.m.etcdPutError.Inc(1)
		return 0, err
	}

	// if there is no prev kv, means this is the first version of the key
	if r.PrevKv == nil {
		return etcdVersionZero + 1, nil
	}

	return int(r.PrevKv.Version + 1), nil
}

func (c *client) SetIfNotExists(key string, v proto.Message) (int, error) {
	version, err := c.CheckAndSet(key, etcdVersionZero, v)
	if err == kv.ErrVersionMismatch {
		err = kv.ErrAlreadyExists
	}
	return version, err
}

func (c *client) CheckAndSet(key string, version int, v proto.Message) (int, error) {
	ctx, cancel := c.context()
	defer cancel()

	value, err := proto.Marshal(v)
	if err != nil {
		return 0, err
	}

	key = c.opts.KeyFn()(key)
	r, err := c.kv.Txn(ctx).
		If(clientv3.Compare(clientv3.Version(key), "=", version)).
		Then(clientv3.OpPut(key, string(value))).
		Commit()
	if err != nil {
		c.m.etcdTnxError.Inc(1)
		return 0, err
	}
	if !r.Succeeded {
		return 0, kv.ErrVersionMismatch
	}

	return version + 1, nil
}

func (c *client) Delete(key string) (kv.Value, error) {
	ctx, cancel := c.context()
	defer cancel()

	key = c.opts.KeyFn()(key)

	r, err := c.kv.Delete(ctx, key, clientv3.WithPrevKV())
	if err != nil {
		return nil, err
	}

	if r.Deleted == 0 {
		return nil, kv.ErrNotFound
	}

	prevKV := newValue(r.PrevKvs[0].Value, r.PrevKvs[0].Version, r.PrevKvs[0].ModRevision)

	c.deleteCache(key)

	return prevKV, nil
}

func (c *client) deleteCache(key string) {
	c.cache.Lock()

	delete(c.cache.Values, key)
	c.notifyCacheUpdate()

	c.cache.Unlock()
}

func (c *client) getCache(key string) (kv.Value, bool) {
	c.cache.RLock()
	v, ok := c.cache.Values[key]
	c.cache.RUnlock()

	return v, ok
}

func (c *client) mergeCache(key string, v *value) {
	c.cache.Lock()

	cur, ok := c.cache.Values[key]
	if !ok || v.isNewer(cur) {
		c.cache.Values[key] = v
		c.notifyCacheUpdate()
	}

	c.cache.Unlock()
}

func (c *client) notifyCacheUpdate() {
	// notify that cached data is updated
	select {
	case c.cacheUpdatedCh <- struct{}{}:
	default:
	}
}

func (c *client) writeCacheToFile() error {
	file, err := os.Create(c.cacheFile)
	if err != nil {
		c.m.diskWriteError.Inc(1)
		c.logger.Warnf("error creating cache file %s: %v", c.cacheFile, err)
		return fmt.Errorf("invalid cache file: %s", c.cacheFile)
	}

	encoder := json.NewEncoder(file)
	c.cache.RLock()
	err = encoder.Encode(c.cache)
	c.cache.RUnlock()

	if err != nil {
		c.m.diskWriteError.Inc(1)
		c.logger.Warnf("error encoding values: %v", err)
		return err
	}

	if err = file.Close(); err != nil {
		c.m.diskWriteError.Inc(1)
		c.logger.Warnf("error closing cache file %s: %v", c.cacheFile, err)
	}

	return nil
}

func (c *client) initCache() error {
	file, err := os.Open(c.opts.CacheFilePath())
	if err != nil {
		c.m.diskReadError.Inc(1)
		return fmt.Errorf("error opening cache file %s: %v", c.cacheFile, err)
	}

	// Read bootstrap file
	decoder := json.NewDecoder(file)

	if err := decoder.Decode(c.cache); err != nil {
		c.m.diskReadError.Inc(1)
		return fmt.Errorf("error reading cache file %s: %v", c.cacheFile, err)
	}

	return nil
}

func (c *client) context() (context.Context, context.CancelFunc) {
	ctx := context.Background()
	cancel := noopCancel
	if c.opts.RequestTimeout() > 0 {
		ctx, cancel = context.WithTimeout(ctx, c.opts.RequestTimeout())
	}

	return ctx, cancel
}

type valueCache struct {
	sync.RWMutex

	Values map[string]*value `json:"values"`
}

func newCache() *valueCache {
	return &valueCache{Values: make(map[string]*value)}
}

type value struct {
	Val []byte `json:"value"`
	Ver int64  `json:"version"`
	Rev int64  `json:"revision"`
}

func newValue(val []byte, ver, rev int64) *value {
	return &value{
		Val: val,
		Ver: ver,
		Rev: rev,
	}
}

func (c *value) isNewer(other *value) bool {
	return c.Rev > other.Rev
}

func (c *value) Unmarshal(v proto.Message) error {
	err := proto.Unmarshal(c.Val, v)

	return err
}

func (c *value) Version() int {
	return int(c.Ver)
}
