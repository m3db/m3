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

package watchmanager

import (
	"context"
	"fmt"
	"math/rand"
	"time"

	"github.com/uber-go/tally"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.uber.org/zap"
)

// NewWatchManager creates a new watch manager
func NewWatchManager(opts Options) (WatchManager, error) {
	if err := opts.Validate(); err != nil {
		return nil, err
	}

	scope := opts.InstrumentsOptions().MetricsScope()
	return &manager{
		opts:   opts,
		logger: opts.InstrumentsOptions().Logger(),
		m: metrics{
			etcdWatchCreate: scope.Counter("etcd-watch-create"),
			etcdWatchError:  scope.Counter("etcd-watch-error"),
			etcdWatchReset:  scope.Counter("etcd-watch-reset"),
		},
		updateFn:      opts.UpdateFn(),
		tickAndStopFn: opts.TickAndStopFn(),
	}, nil
}

type manager struct {
	opts   Options
	logger *zap.Logger
	m      metrics

	updateFn      UpdateFn
	tickAndStopFn TickAndStopFn
}

type metrics struct {
	etcdWatchCreate tally.Counter
	etcdWatchError  tally.Counter
	etcdWatchReset  tally.Counter
}

func (w *manager) watchChanWithTimeout(key string, rev int64) (clientv3.WatchChan, context.CancelFunc, error) {
	doneCh := make(chan struct{})

	ctx, cancelFn := context.WithCancel(clientv3.WithRequireLeader(context.Background()))

	var (
		watcher   = clientv3.NewWatcher(w.opts.Client())
		watchChan clientv3.WatchChan
	)
	go func() {
		wOpts := w.opts.WatchOptions()
		if rev > 0 {
			wOpts = append(wOpts, clientv3.WithRev(rev))
		}

		watchChan = watcher.Watch(
			ctx,
			key,
			wOpts...,
		)
		close(doneCh)
	}()

	var (
		timeout       = w.opts.WatchChanInitTimeout()
		cancelWatchFn = func() {
			// we *must* both cancel the context and call .Close() on watch to
			// properly free resources, and not end up with weird issues due to stale
			// grpc streams or bad internal etcd watch state.
			cancelFn()
			if err := watcher.Close(); err != nil {
				// however, there's nothing we can do about an error on watch close,
				// and it shouldn't happen in practice - unless we end up
				// closing an already closed grpc stream or smth.
				w.logger.Info("error closing watcher", zap.Error(err))
			}
		}
	)

	select {
	case <-doneCh:
		return watchChan, cancelWatchFn, nil
	case <-time.After(timeout):
		cancelWatchFn()
		err := fmt.Errorf("etcd watch create timed out after %s for key: %s", timeout.String(), key)
		return nil, cancelWatchFn, err
	}
}

func (w *manager) Watch(key string) {
	var (
		ticker = time.NewTicker(w.opts.WatchChanCheckInterval())
		logger = w.logger.With(zap.String("watch_key", key))
		rnd    = rand.New(rand.NewSource(time.Now().UnixNano())) //nolint:gosec

		revOverride          int64
		firstUpdateSucceeded bool
		watchChan            clientv3.WatchChan
		cancelFn             context.CancelFunc
		err                  error
	)

	defer ticker.Stop()

	resetWatchWithSleep := func() {
		w.m.etcdWatchReset.Inc(1)

		cancelFn()
		// set it to nil so it will be recreated
		watchChan = nil
		// avoid recreating watch channel too frequently
		dur := w.opts.WatchChanResetInterval()
		dur += time.Duration(rnd.Int63n(int64(dur)))
		time.Sleep(dur)
	}

	for {
		if watchChan == nil {
			w.m.etcdWatchCreate.Inc(1)
			logger.Info("creating etcd watch at revision", zap.Int64("revision", revOverride))
			watchChan, cancelFn, err = w.watchChanWithTimeout(key, revOverride)
			if err != nil {
				logger.Error("could not create etcd watch", zap.Error(err))

				// NB(cw) when we failed to create a etcd watch channel
				// we do a get for now and will try to recreate the watch chan later
				if !firstUpdateSucceeded {
					if err = w.updateFn(key, nil); err != nil {
						logger.Error("failed to get value for key", zap.Error(err))
					} else {
						// NB(vytenis): only try initializing once, otherwise there's
						// get request amplification, especially for non-existent keys.
						firstUpdateSucceeded = true
					}
				}
				resetWatchWithSleep()
				continue
			}
		}

		select {
		case r, ok := <-watchChan:
			if !ok {
				resetWatchWithSleep()
				logger.Warn("etcd watch channel closed on key, recreating a watch channel")
				continue
			}

			// handle the update
			if err = r.Err(); err != nil {
				logger.Error(
					"received error on watch channel",
					zap.Uint64("etcd_cluster_id", r.Header.ClusterId),
					zap.Uint64("etcd_member_id", r.Header.MemberId),
					zap.Bool("etcd_watch_is_canceled", r.Canceled),
					zap.Error(err),
				)
				w.m.etcdWatchError.Inc(1)
				if err == rpctypes.ErrCompacted {
					revOverride = r.CompactRevision
					logger.Warn("compacted; recreating watch at revision",
						zap.Int64("revision", revOverride))
				} else {
					logger.Warn("recreating watch due to an error", zap.Error(err))
				}

				resetWatchWithSleep()
				continue
			} else if r.IsProgressNotify() {
				if r.CompactRevision > revOverride {
					// we only care about last event as this watchmanager implementation does not support
					// watching key ranges, only single keys.
					// set revOverride to minimum non-compacted revision if watch was
					// initialized with an older rev., since we really don't care about history.
					// this may help recover faster (one less retry) on connection loss/leader change
					// around compaction, if we were watching on a revision that's already compacted.
					revOverride = r.CompactRevision
				}
				// Do not call updateFn on ProgressNotify as it happens periodically with no update events
				continue
			}

			if err = w.updateFn(key, r.Events); err != nil {
				logger.Error("received notification for key, but failed to get value", zap.Error(err))
			}
		case <-ticker.C:
			if w.tickAndStopFn(key) {
				logger.Info("watch on key ended")
				return
			}
		}
	}
}
