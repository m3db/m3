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

package peers

import (
	"sync"

	"github.com/m3db/m3db/clock"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/sync"
	"github.com/m3db/m3x/time"
)

type peersSource struct {
	opts  Options
	log   xlog.Logger
	nowFn clock.NowFn
}

func newPeersSource(opts Options) bootstrap.Source {
	return &peersSource{
		opts:  opts,
		log:   opts.ResultOptions().InstrumentOptions().Logger(),
		nowFn: opts.ResultOptions().ClockOptions().NowFn(),
	}
}

func (s *peersSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *peersSource) Available(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Peers should be able to fulfill all data
	return shardsTimeRanges
}

func (s *peersSource) Read(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	result := result.NewBootstrapResult()
	session, err := s.opts.AdminClient().DefaultAdminSession()
	if err != nil {
		s.log.Errorf("peers bootstrapper cannot get default admin session: %v", err)
		result.SetUnfulfilled(shardsTimeRanges)
		return result, nil
	}

	var (
		bopts       = s.opts.ResultOptions()
		concurrency = s.opts.BootstrapShardConcurrency()
		count       = len(shardsTimeRanges)
		lock        sync.Mutex
		wg          sync.WaitGroup
	)
	s.log.WithFields(
		xlog.NewLogField("shards", count),
		xlog.NewLogField("concurrency", concurrency),
	).Infof("peers bootstrapper bootstrapping shards for ranges")
	workers := xsync.NewWorkerPool(concurrency)
	workers.Init()
	for shard, ranges := range shardsTimeRanges {
		shard, ranges := shard, ranges
		wg.Add(1)
		workers.Go(func() {
			defer wg.Done()

			it := ranges.Iter()
			for it.Next() {
				currRange := it.Value()

				shardResult, err := session.FetchBootstrapBlocksFromPeers(namespace,
					shard, currRange.Start, currRange.End, bopts)

				lock.Lock()
				if err == nil {
					result.Add(shard, shardResult, nil)
				} else {
					result.Add(shard, nil, xtime.NewRanges().AddRange(currRange))
				}
				lock.Unlock()
			}
		})
	}

	wg.Wait()

	return result, nil
}
