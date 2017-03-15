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
	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/storage/block"
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

type incrementalFlush struct {
	namespace         ts.ID
	shard             uint32
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
	shardResult       result.ShardResult
	timeRange         xtime.Range
}

type incrementalFlushedBlock struct {
	id    ts.ID
	block block.DatabaseBlock
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
	opts bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	var (
		blockRetriever    block.DatabaseBlockRetriever
		shardRetrieverMgr block.DatabaseShardBlockRetrieverManager
		persistFlush      persist.Flush
		incremental       = false
	)
	if opts.Incremental() {
		retrieverMgr := s.opts.DatabaseBlockRetrieverManager()
		persistManager := s.opts.PersistManager()
		if retrieverMgr != nil && persistManager != nil {
			s.log.WithFields(
				xlog.NewLogField("namespace", namespace.String()),
			).Infof("peers bootstrapper resolving block retriever")

			r, err := retrieverMgr.Retriever(namespace)
			if err != nil {
				return nil, err
			}

			flush, err := persistManager.StartFlush()
			if err != nil {
				return nil, err
			}

			defer flush.Done()

			incremental = true
			blockRetriever = r
			shardRetrieverMgr = block.NewDatabaseShardBlockRetrieverManager(r)
			persistFlush = flush
		}
	}

	result := result.NewBootstrapResult()
	session, err := s.opts.AdminClient().DefaultAdminSession()
	if err != nil {
		s.log.Errorf("peers bootstrapper cannot get default admin session: %v", err)
		result.SetUnfulfilled(shardsTimeRanges)
		return nil, err
	}

	var (
		lock                sync.Mutex
		wg                  sync.WaitGroup
		incrementalWg       sync.WaitGroup
		incrementalMaxQueue = s.opts.IncrementalPersistMaxQueueSize()
		incrementalQueue    = make(chan incrementalFlush, incrementalMaxQueue)
		bopts               = s.opts.ResultOptions()
		count               = len(shardsTimeRanges)
		concurrency         = s.opts.DefaultShardConcurrency()
	)
	if incremental {
		concurrency = s.opts.IncrementalShardConcurrency()
	}

	s.log.WithFields(
		xlog.NewLogField("shards", count),
		xlog.NewLogField("concurrency", concurrency),
		xlog.NewLogField("incremental", incremental),
	).Infof("peers bootstrapper bootstrapping shards for ranges")
	if incremental {
		// If performing an incremental bootstrap then flush one
		// at a time as shard results are gathered
		incrementalWg.Add(1)
		go func() {
			defer incrementalWg.Done()

			for flush := range incrementalQueue {
				err := s.incrementalFlush(persistFlush, flush.namespace,
					flush.shard, flush.shardRetrieverMgr, flush.shardResult, flush.timeRange)
				if err != nil {
					s.log.WithFields(
						xlog.NewLogField("error", err.Error()),
					).Infof("peers bootstrapper incremental flush encountered error")
				}
			}
		}()
	}

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

				if err == nil && incremental {
					incrementalQueue <- incrementalFlush{
						namespace:         namespace,
						shard:             shard,
						shardRetrieverMgr: shardRetrieverMgr,
						shardResult:       shardResult,
						timeRange:         currRange,
					}
				}

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

	close(incrementalQueue)
	incrementalWg.Wait()

	if incremental {
		// Now cache the incremental results
		shards := make([]uint32, 0, len(shardsTimeRanges))
		for shard := range shardsTimeRanges {
			shards = append(shards, shard)
		}

		if err := blockRetriever.CacheShardIndices(shards); err != nil {
			return nil, err
		}
	}

	return result, nil
}

func (s *peersSource) incrementalFlush(
	flush persist.Flush,
	namespace ts.ID,
	shard uint32,
	shardRetrieverMgr block.DatabaseShardBlockRetrieverManager,
	shardResult result.ShardResult,
	tr xtime.Range,
) error {
	var (
		ropts          = s.opts.ResultOptions().RetentionOptions()
		blockSize      = ropts.BlockSize()
		shardRetriever = shardRetrieverMgr.ShardRetriever(shard)
		tmpCtx         = context.NewContext()
	)
	for start := tr.Start; start.Before(tr.End); start = start.Add(blockSize) {
		prepared, err := flush.Prepare(namespace, shard, start)
		if err != nil {
			return err
		}

		var (
			blockErr          error
			shardResultSeries = shardResult.AllSeries()
		)
		for _, series := range shardResultSeries {
			bl, ok := series.Blocks.BlockAt(start)
			if !ok {
				continue
			}

			tmpCtx.Reset()
			stream, err := bl.Stream(tmpCtx)
			if err != nil {
				tmpCtx.BlockingClose()
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			segment, err := stream.Segment()
			if err != nil {
				tmpCtx.BlockingClose()
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}

			err = prepared.Persist(series.ID, segment, bl.Checksum())
			tmpCtx.BlockingClose()
			if err != nil {
				blockErr = err // Need to call prepared.Close, avoid return
				break
			}
		}

		// Always close before attempting to check if block error occurred,
		// avoid using a defer here as this needs to be done for each inner loop
		err = prepared.Close()
		if blockErr != nil {
			// A block error is more interesting to bubble up than a close error
			err = blockErr
		}
		if err != nil {
			return err
		}

		// NB(r): We can now make the flushed blocks retrievable, note that we
		// explicitly perform another loop here and lookup the block again
		// to avoid a large expensive allocation to hold onto the blocks
		// that we just flushed that would have to be pooled.
		// We are explicitly trading CPU time here for lower GC pressure.
		for _, series := range shardResultSeries {
			bl, ok := series.Blocks.BlockAt(start)
			if !ok {
				continue
			}
			metadata := block.RetrievableBlockMetadata{
				ID:       series.ID,
				Length:   bl.Len(),
				Checksum: bl.Checksum(),
			}
			bl.ResetRetrievable(start, shardRetriever, metadata)
		}
	}

	return nil
}
