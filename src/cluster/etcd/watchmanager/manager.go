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

	"github.com/m3db/m3x/log"

	"github.com/coreos/etcd/clientv3"
	"github.com/uber-go/tally"
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
	logger xlog.Logger
	m      metrics

	updateFn      UpdateFn
	tickAndStopFn TickAndStopFn
}

type metrics struct {
	etcdWatchCreate tally.Counter
	etcdWatchError  tally.Counter
	etcdWatchReset  tally.Counter
}

func (w *manager) watchChanWithTimeout(key string) (clientv3.WatchChan, error) {
	doneCh := make(chan struct{})

	ctx, cancelFn := context.WithCancel(clientv3.WithRequireLeader(context.Background()))

	var watchChan clientv3.WatchChan
	go func() {
		watchChan = w.opts.Watcher().Watch(
			ctx,
			key,
			w.opts.WatchOptions()...,
		)
		close(doneCh)
	}()

	timeout := w.opts.WatchChanInitTimeout()
	select {
	case <-doneCh:
		return watchChan, nil
	case <-time.After(timeout):
		cancelFn()
		return nil, fmt.Errorf("etcd watch create timed out after %s for key: %s", timeout.String(), key)
	}
}

func (w *manager) Watch(key string) {
	ticker := time.Tick(w.opts.WatchChanCheckInterval())

	var (
		watchChan clientv3.WatchChan
		err       error
	)
	for {
		if watchChan == nil {
			w.m.etcdWatchCreate.Inc(1)
			watchChan, err = w.watchChanWithTimeout(key)
			if err != nil {
				w.logger.Errorf("could not create etcd watch: %v", err)

				// NB(cw) when we failed to create a etcd watch channel
				// we do a get for now and will try to recreate the watch chan later
				if err = w.updateFn(key); err != nil {
					w.logger.Errorf("failed to get value for key %s: %v", key, err)
				}
				// avoid recreating watch channel too frequently
				time.Sleep(w.opts.WatchChanResetInterval())
				continue
			}
		}

		select {
		case r, ok := <-watchChan:
			if !ok {
				// the watch chan is closed, set it to nil so it will be recreated
				// this is unlikely to happen but just to be defensive
				watchChan = nil
				w.logger.Warnf("etcd watch channel closed on key %s, recreating a watch channel", key)

				// avoid recreating watch channel too frequently
				time.Sleep(w.opts.WatchChanResetInterval())
				w.m.etcdWatchReset.Inc(1)

				continue
			}

			// handle the update
			if err = r.Err(); err != nil {
				w.logger.Errorf("received error on watch channel: %v", err)
				w.m.etcdWatchError.Inc(1)
				// do not stop here, even though the update contains an error
				// we still take this chance to attemp a Get() for the latest value
			}

			if err = w.updateFn(key); err != nil {
				w.logger.Errorf("received notification for key %s, but failed to get value: %v", key, err)
			}
		case <-ticker:
			if w.tickAndStopFn(key) {
				w.logger.Infof("watch on key %s ended", key)
				return
			}
		}
	}
}
