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

package commitlog

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	xlog "github.com/m3db/m3x/log"
	xtime "github.com/m3db/m3x/time"
)

const encoderChanBufSize = 1000

type newIteratorFn func(
	opts commitlog.Options,
	commitLogPred commitlog.ReadEntryPredicate,
) (commitlog.Iterator, error)

type commitLogSource struct {
	opts          Options
	log           xlog.Logger
	newIteratorFn newIteratorFn
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

type encodersByTime struct {
	id ts.ID
	// int64 instead of time.Time because there is an optimized map access pattern
	// for i64's
	encoders map[int64][]encoder
}

// encoderArg contains all the information a worker go-routine needs to encode
// a data point as M3TSZ
type encoderArg struct {
	series     commitlog.Series
	dp         ts.Datapoint
	unit       xtime.Unit
	annotation ts.Annotation
	blockStart time.Time
}

func newCommitLogSource(opts Options) bootstrap.Source {
	return &commitLogSource{
		opts:          opts,
		log:           opts.ResultOptions().InstrumentOptions().Logger(),
		newIteratorFn: commitlog.NewIterator,
	}
}
func (s *commitLogSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *commitLogSource) Available(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

func (s *commitLogSource) Read(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	readCommitLogPredicate := newReadCommitLogPredicate(ns, shardsTimeRanges, s.opts)
	iter, err := s.newIteratorFn(s.opts.CommitLogOptions(), readCommitLogPredicate)
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	defer iter.Close()

	var (
		namespace    = ns.ID()
		numShards    = len(shardsTimeRanges)
		unmerged     = []map[ts.Hash]encodersByTime{}
		unmergedLock = &sync.RWMutex{}
		// TODO: Add to opts
		numConc     = uint32(4)
		bopts       = s.opts.ResultOptions()
		blopts      = bopts.DatabaseBlockOptions()
		blockSize   = ns.Options().RetentionOptions().BlockSize()
		encoderPool = bopts.DatabaseBlockOptions().EncoderPool()
		workerErrs  = make([]int, numConc, numConc)
	)

	encoderChans := []chan encoderArg{}
	for i := uint32(0); i < numConc; i++ {
		encoderChans = append(encoderChans, make(chan encoderArg, encoderChanBufSize))
	}

	// Spin up numConc background go-routines to handle M3TSZ encoding. This must
	// happen before we start reading to prevent infinitely blocking writes to
	// the encoderChans.
	wg := &sync.WaitGroup{}
	for workerNum, encoderChan := range encoderChans {
		wg.Add(1)
		go s.startM3TSZEncodingWorker(
			workerNum,
			encoderChan,
			&unmerged,
			unmergedLock,
			encoderPool,
			workerErrs,
			blopts,
			wg,
		)
	}

	for iter.Next() {
		series, dp, unit, annotation := iter.Current()

		// check if the series belongs to current namespace being bootstrapped
		if !namespace.Equal(series.Namespace) {
			continue
		}

		ranges, ok := shardsTimeRanges[series.Shard]
		if !ok {
			// Not bootstrapping this shard
			continue
		}

		blockStart := dp.Timestamp.Truncate(blockSize)
		blockEnd := blockStart.Add(blockSize)
		blockRange := xtime.Range{
			Start: blockStart,
			End:   blockEnd,
		}
		if !ranges.Overlaps(blockRange) {
			// Data in this block does not match the requested ranges
			continue
		}

		// TODO: Factor out into function?
		// We use the unmerged slice for lookup because its faster than a map.
		// Shard ID's map directly to indices, but we don't know the highest shard
		// number upfront so we resize when necessary.
		if len(unmerged) < int(series.Shard+1) {
			// Resize shard
			unmergedLock.Lock()
			oldUnmerged := unmerged
			unmerged = make([]map[ts.Hash]encodersByTime, series.Shard+1)
			for i, hashToEncoderMap := range oldUnmerged {
				unmerged[i] = hashToEncoderMap
			}
			unmergedLock.Unlock()
		}

		// Distribute work such that each encoder goroutine is responsible for
		// approximately numShards / numConc shards. This also means that all
		// datapoints for a given shard/series will be processed in a serialized
		// manner.
		encoderChans[series.Shard%numConc] <- struct {
			series     commitlog.Series
			dp         ts.Datapoint
			unit       xtime.Unit
			annotation ts.Annotation
			blockStart time.Time
		}{
			series:     series,
			dp:         dp,
			unit:       unit,
			annotation: annotation,
			blockStart: blockStart,
		}
	}
	for _, encoderChan := range encoderChans {
		close(encoderChan)
	}

	// Block until all data has been read and encoded by the worker goroutines
	wg.Wait()
	errSum := 0
	for _, numErrs := range workerErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d block encode errors", errSum)
	}
	if err := iter.Err(); err != nil {
		s.log.Errorf("error reading commit log: %v", err)
	}

	shardErrs := make([]int, numShards, numShards)
	shardEmptyErrs := make([]int, numShards, numShards)
	bootstrapResult := result.NewBootstrapResult()
	blocksPool := bopts.DatabaseBlockOptions().DatabaseBlockPool()
	multiReaderIteratorPool := blopts.MultiReaderIteratorPool()
	// Controls how many shards can be merged in parallel
	mergeSemaphore := make(chan bool, 4)
	bootstrapResultLock := sync.Mutex{}
	wg = &sync.WaitGroup{}

	for shard, unmergedShard := range unmerged {
		mergeSemaphore <- true
		wg.Add(1)
		// TODO: Factor this into subfunctions
		go func(shard int, unmergedShard map[ts.Hash]encodersByTime) {
			shardResult := result.NewShardResult(len(unmergedShard), s.opts.ResultOptions())
			for _, unmergedBlocks := range unmergedShard {
				blocks := block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders))
				for start, unmergedBlock := range unmergedBlocks.encoders {
					startInNano := time.Unix(0, start)
					block := blocksPool.Get()
					if len(unmergedBlock) == 0 {
						shardEmptyErrs[shard]++
						continue
					} else if len(unmergedBlock) == 1 {
						block.Reset(startInNano, unmergedBlock[0].enc.Discard())
					} else {
						readers := make([]io.Reader, len(unmergedBlock))
						for i := range unmergedBlock {
							stream := unmergedBlock[i].enc.Stream()
							readers[i] = stream
							defer stream.Finalize()
						}

						iter := multiReaderIteratorPool.Get()
						iter.Reset(readers)

						var err error
						enc := encoderPool.Get()
						enc.Reset(startInNano, blopts.DatabaseBlockAllocSize())
						for iter.Next() {
							dp, unit, annotation := iter.Current()
							encodeErr := enc.Encode(dp, unit, annotation)
							if encodeErr != nil {
								err = encodeErr
								shardErrs[shard]++
								break
							}
						}
						if iterErr := iter.Err(); iterErr != nil {
							if err == nil {
								err = iter.Err()
							}
							shardErrs[shard]++
						}

						iter.Close()
						for i := range unmergedBlock {
							unmergedBlock[i].enc.Close()
						}

						if err != nil {
							continue
						}

						block.Reset(startInNano, enc.Discard())
					}
					blocks.AddBlock(block)
				}
				if blocks.Len() > 0 {
					shardResult.AddSeries(unmergedBlocks.id, blocks)
				}
			}
			if len(shardResult.AllSeries()) > 0 {
				// Prevent race conditions while updating bootstrapResult from multiple go-routines
				bootstrapResultLock.Lock()
				// Shard is a slice index so conversion to uint32 is safe
				bootstrapResult.Add(uint32(shard), shardResult, nil)
				bootstrapResultLock.Unlock()
			}
			<-mergeSemaphore
			wg.Done()
		}(shard, unmergedShard)
	}
	// Wait for all merge goroutines to complete
	wg.Wait()

	errSum = 0
	for _, numErrs := range shardErrs {
		errSum += numErrs
	}
	if errSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d merge out of order errors", errSum)
	}

	emptyErrSum := 0
	for _, numEmptyErr := range shardEmptyErrs {
		emptyErrSum += numEmptyErr
	}
	if emptyErrSum > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d empty unmerged blocks errors", emptyErrSum)
	}

	return bootstrapResult, nil
}

func (s *commitLogSource) startM3TSZEncodingWorker(
	workerNum int,
	ec <-chan encoderArg,
	// Pass as pointer so slice resizing is reflected
	unmergedPtr *[]map[ts.Hash]encodersByTime,
	unmergedLock *sync.RWMutex,
	encoderPool encoding.EncoderPool,
	workerErrs []int,
	blockOpts block.Options,
	wg *sync.WaitGroup,
) {
	for arg := range ec {
		var (
			series     = arg.series
			dp         = arg.dp
			unit       = arg.unit
			annotation = arg.annotation
			blockStart = arg.blockStart
		)

		unmergedLock.RLock()
		// Dereference the pointer inside the lock so we're certain that the slice
		// has been resized already
		unmerged := *unmergedPtr
		unmergedShard := unmerged[series.Shard]
		unmergedLock.RUnlock()
		if unmergedShard == nil {
			unmergedShard = make(map[ts.Hash]encodersByTime)
			unmerged[series.Shard] = unmergedShard
		}

		unmergedSeries, ok := unmergedShard[series.ID.Hash()]
		if !ok {
			unmergedSeries = encodersByTime{
				id:       series.ID,
				encoders: make(map[int64][]encoder)}
			unmergedShard[series.ID.Hash()] = unmergedSeries
		}

		var (
			err           error
			unmergedBlock = unmergedSeries.encoders[blockStart.UnixNano()]
			wroteExisting = false
		)
		for i := range unmergedBlock {
			if unmergedBlock[i].lastWriteAt.Before(dp.Timestamp) {
				unmergedBlock[i].lastWriteAt = dp.Timestamp
				err = unmergedBlock[i].enc.Encode(dp, unit, annotation)
				wroteExisting = true
				break
			}
		}
		if !wroteExisting {
			enc := encoderPool.Get()
			enc.Reset(blockStart, blockOpts.DatabaseBlockAllocSize())

			err = enc.Encode(dp, unit, annotation)
			if err == nil {
				unmergedBlock = append(unmergedBlock, encoder{
					lastWriteAt: dp.Timestamp,
					enc:         enc,
				})
				unmergedSeries.encoders[blockStart.UnixNano()] = unmergedBlock
			}
		}
		if err != nil {
			workerErrs[workerNum]++
		}
	}
	wg.Done()
}

func newReadCommitLogPredicate(
	ns namespace.Metadata,
	shardsTimeRanges result.ShardTimeRanges,
	opts Options,
) commitlog.ReadEntryPredicate {
	// Minimum and maximum times for which we want to bootstrap
	shardMin, shardMax := shardsTimeRanges.MinMax()
	shardRange := xtime.Range{
		Start: shardMin,
		End:   shardMax,
	}

	// How far into the past or future a commitlog might contain a write for a
	// previous or future block
	bufferPast := ns.Options().RetentionOptions().BufferPast()
	bufferFuture := ns.Options().RetentionOptions().BufferFuture()

	return func(entryTime time.Time, entryDuration time.Duration) bool {
		// If there is any amount of overlap between the commitlog range and the
		// shardRange then we need to read the commitlog file
		return xtime.Range{
			Start: entryTime.Add(-bufferPast),
			End:   entryTime.Add(entryDuration).Add(bufferFuture),
		}.Overlaps(shardRange)
	}
}
