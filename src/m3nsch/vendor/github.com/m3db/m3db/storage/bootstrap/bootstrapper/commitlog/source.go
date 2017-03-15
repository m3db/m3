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
	"time"

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

type newIteratorFn func(opts commitlog.Options) (commitlog.Iterator, error)

type commitLogSource struct {
	opts          Options
	log           xlog.Logger
	newIteratorFn newIteratorFn
}

type encoder struct {
	lastWriteAt time.Time
	enc         encoding.Encoder
}

type encoderMap struct {
	id       ts.ID
	encoders map[time.Time][]encoder
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
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
) result.ShardTimeRanges {
	// Commit log bootstrapper is a last ditch effort, so fulfill all
	// time ranges requested even if not enough data, just to succeed
	// the bootstrap
	return shardsTimeRanges
}

func (s *commitLogSource) Read(
	namespace ts.ID,
	shardsTimeRanges result.ShardTimeRanges,
	_ bootstrap.RunOptions,
) (result.BootstrapResult, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	iter, err := s.newIteratorFn(s.opts.CommitLogOptions())
	if err != nil {
		return nil, fmt.Errorf("unable to create commit log iterator: %v", err)
	}

	defer iter.Close()

	var (
		unmerged    = make(map[uint32]map[ts.Hash]encoderMap)
		bopts       = s.opts.ResultOptions()
		blopts      = bopts.DatabaseBlockOptions()
		blockSize   = bopts.RetentionOptions().BlockSize()
		encoderPool = bopts.DatabaseBlockOptions().EncoderPool()
		errs        = 0
	)
	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
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

		unmergedShard, ok := unmerged[series.Shard]
		if !ok {
			unmergedShard = make(map[ts.Hash]encoderMap)
			unmerged[series.Shard] = unmergedShard
		}

		unmergedSeries, ok := unmergedShard[series.ID.Hash()]
		if !ok {
			unmergedSeries = encoderMap{
				id:       series.ID,
				encoders: make(map[time.Time][]encoder)}
			unmergedShard[series.ID.Hash()] = unmergedSeries
		}

		var (
			err           error
			unmergedBlock = unmergedSeries.encoders[blockStart]
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
			enc.Reset(blockStart, blopts.DatabaseBlockAllocSize())

			err = enc.Encode(dp, unit, annotation)
			if err == nil {
				unmergedBlock = append(unmergedBlock, encoder{
					lastWriteAt: dp.Timestamp,
					enc:         enc,
				})
				unmergedSeries.encoders[blockStart] = unmergedBlock
			}
		}
		if err != nil {
			errs++
		}
	}
	if errs > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d block encode errors", errs)
	}
	if err := iter.Err(); err != nil {
		s.log.Errorf("error reading commit log: %v", err)
	}

	errs = 0
	emptyErrs := 0
	bootstrapResult := result.NewBootstrapResult()
	blocksPool := bopts.DatabaseBlockOptions().DatabaseBlockPool()
	multiReaderIteratorPool := blopts.MultiReaderIteratorPool()
	for shard, unmergedShard := range unmerged {
		shardResult := result.NewShardResult(len(unmergedShard), s.opts.ResultOptions())
		for _, unmergedBlocks := range unmergedShard {
			blocks := block.NewDatabaseSeriesBlocks(len(unmergedBlocks.encoders), blopts)
			for start, unmergedBlock := range unmergedBlocks.encoders {
				block := blocksPool.Get()
				if len(unmergedBlock) == 0 {
					emptyErrs++
					continue
				} else if len(unmergedBlock) == 1 {
					block.Reset(start, unmergedBlock[0].enc.Discard())
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
					enc.Reset(start, blopts.DatabaseBlockAllocSize())
					for iter.Next() {
						dp, unit, annotation := iter.Current()
						encodeErr := enc.Encode(dp, unit, annotation)
						if encodeErr != nil {
							err = encodeErr
							errs++
							break
						}
					}
					if iterErr := iter.Err(); iterErr != nil {
						if err == nil {
							err = iter.Err()
						}
						errs++
					}

					iter.Close()
					for i := range unmergedBlock {
						unmergedBlock[i].enc.Close()
					}

					if err != nil {
						continue
					}

					block.Reset(start, enc.Discard())
				}
				blocks.AddBlock(block)
			}
			if blocks.Len() > 0 {
				shardResult.AddSeries(unmergedBlocks.id, blocks)
			}
		}
		if len(shardResult.AllSeries()) > 0 {
			bootstrapResult.Add(shard, shardResult, nil)
		}
	}
	if errs > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d merge out of order errors", errs)
	}
	if emptyErrs > 0 {
		s.log.Errorf("error bootstrapping from commit log: %d empty unmerged blocks errors", emptyErrs)
	}

	return bootstrapResult, nil
}
