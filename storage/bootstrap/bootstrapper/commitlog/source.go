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
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3x/time"
)

// commitLogSource provides information about commit log data stored on disk.
type commitLogSource struct {
	opts Options
}

// newCommitLogSource creates a new commit log based database.
func newCommitLogSource(opts Options) bootstrap.Source {
	return &commitLogSource{opts: opts}
}

// GetAvailability returns what time ranges are available for a given shard.
func (s *commitLogSource) GetAvailability(shard uint32, targetRangesForShard xtime.Ranges) xtime.Ranges {
	return targetRangesForShard
}

// ReadData returns raw series for a given shard within certain time ranges.
func (s *commitLogSource) ReadData(shard uint32, tr xtime.Ranges) (bootstrap.ShardResult, xtime.Ranges) {
	bopts := s.opts.GetBootstrapOptions()
	log := bopts.GetInstrumentOptions().GetLogger()

	if xtime.IsEmpty(tr) {
		return nil, tr
	}

	iter, err := commitlog.NewCommitLogIterator(s.opts.GetCommitLogOptions())
	if err != nil {
		log.Errorf("unable to create commit log iterator: %v", err)
		return nil, tr
	}

	defer iter.Close()

	seriesMap := bootstrap.NewShardResult(bopts)
	blockSize := bopts.GetRetentionOptions().GetBlockSize()
	errs := 0
	for iter.Next() {
		series, dp, unit, annotation := iter.Current()
		if series.Shard != shard {
			continue
		}

		blocks, ok := seriesMap.GetAllSeries()[series.ID]
		if !ok {
			blocks = block.NewDatabaseSeriesBlocks(bopts.GetDatabaseBlockOptions())
			seriesMap.AddSeries(series.ID, blocks)
		}

		block := blocks.GetBlockOrAdd(dp.Timestamp.Truncate(blockSize))
		if err := block.Write(dp.Timestamp, dp.Value, unit, annotation); err != nil {
			errs++
		}
	}
	if errs > 0 {
		log.Errorf("error bootstrapping from commit log: %d block encode errors", errs)
	}
	if err := iter.Err(); err != nil {
		log.Errorf("error reading commit log: %v", err)
	}

	return seriesMap, tr
}
