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
	"time"

	"github.com/uber-go/tally"
	"go.etcd.io/etcd/clientv3"
	"go.etcd.io/etcd/etcdserver/api/v3rpc/rpctypes"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/x/unsafe"
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
			etcdWatchCreate:     scope.Counter("etcd-watch-create"),
			etcdWatchError:      scope.Counter("etcd-watch-error"),
			etcdWatchReset:      scope.Counter("etcd-watch-reset"),
			etcdWatchUpdateRcvd: scope.Counter("etcd-watch-update-rcvd"),
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
	etcdWatchCreate     tally.Counter
	etcdWatchError      tally.Counter
	etcdWatchReset      tally.Counter
	etcdWatchUpdateRcvd tally.Counter
}

func (w *manager) watchChanWithTimeout(key string, rev int64) (clientv3.WatchChan, context.CancelFunc, error) {
	doneCh := make(chan struct{})

	ctx, cancelFn := context.WithCancel(clientv3.WithRequireLeader(context.Background()))

	var watchChan clientv3.WatchChan
	go func() {
		wOpts := w.opts.WatchOptions()
		if rev > 0 {
			wOpts = append(wOpts, clientv3.WithRev(rev))
		}
		watchChan = w.opts.Watcher().Watch(
			ctx,
			key,
			wOpts...,
		)
		close(doneCh)
	}()

	timeout := w.opts.WatchChanInitTimeout()
	select {
	case <-doneCh:
		return watchChan, cancelFn, nil
	case <-time.After(timeout):
		cancelFn()
		return nil, nil, fmt.Errorf("etcd watch create timed out after %s for key: %s", timeout.String(), key)
	}
}

func (w *manager) Watch(key string) {
	var (
		ticker = time.NewTicker(w.opts.WatchChanCheckInterval())
		logger = w.logger.With(zap.String("watch_key", key))

		revOverride int64
		watchChan   clientv3.WatchChan
		cancelFn    context.CancelFunc
		err         error
	)

	defer ticker.Stop()

	for {
		if watchChan == nil {
			w.m.etcdWatchCreate.Inc(1)
			logger.Info("creating etcd watch at revision", zap.Int64("revision", revOverride))
			watchChan, cancelFn, err = w.watchChanWithTimeout(key, revOverride)
			if err != nil {
				logger.Error("could not create etcd watch", zap.Error(err))

				// NB(cw) when we failed to create a etcd watch channel
				// we do a get for now and will try to recreate the watch chan later
				if err = w.updateFn(key, nil); err != nil {
					logger.Error("failed to get value for key", zap.Error(err))
				}
				// avoid recreating watch channel too frequently
				time.Sleep(w.opts.WatchChanResetInterval())
				continue
			}
		}

		processWatchUpdate := func(r clientv3.WatchResponse) bool {
			// handle the update
			if err = r.Err(); err == nil {
				w.m.etcdWatchUpdateRcvd.Inc(1)
				if r.IsProgressNotify() {
					// Do not call updateFn on ProgressNotify as it happens periodically with no update events
					return true
				}

				if err = w.updateFn(key, r.Events); err != nil {
					logger.Error("received notification for key, but failed to get value", zap.Error(err))
				}

				return true
			}

			// Unhappy path: watch failed/was cancelled
			w.m.etcdWatchError.Inc(1)
			logger.Error(
				"received error on watch channel, recreating watch",
				zap.Uint64("etcd_cluster_id", r.Header.ClusterId),
				zap.Uint64("etcd_member_id", r.Header.MemberId),
				zap.Bool("etcd_watch_is_canceled", r.Canceled),
				zap.Error(err),
			)

			// If the current revision has been compacted, set watchChan to
			// nil so the watch is recreated with a valid start revision
			if err == rpctypes.ErrCompacted {
				logger.Warn("recreating watch at revision", zap.Int64("revision", r.CompactRevision))
				revOverride = r.CompactRevision
			}

			// Attempt to get the latest version
			if err = w.updateFn(key, nil); err != nil {
				logger.Error("failed to get value after watch was compacted", zap.Error(err))
			}

			return false
		}

		select {
		case r, ok := <-watchChan:
			if !ok || !processWatchUpdate(r) {
				// the watch encountered an error, set it to nil so it will be recreated
				logger.Debug("resetting watch channel")
				w.m.etcdWatchReset.Inc(1)

				cancelFn()
				watchChan = nil
				// avoid recreating watch channel too frequently
				// this is different than the watch creation error case:
				// we use jitter on server-side cancellations, compations, or leader loss to reduce
				// thundering herd effect.
				max := uint32(w.opts.WatchChanResetInterval())
				sleep := time.Duration(unsafe.Fastrandn(max))
				time.Sleep(sleep)
			}
		case <-ticker.C:
			if w.tickAndStopFn(key) {
				logger.Info("watch on key ended")
				return
			}
		}
	}
}
