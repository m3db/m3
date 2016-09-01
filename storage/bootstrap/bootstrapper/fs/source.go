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

package fs

import (
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

type newFileSetReaderFn func(filePathPrefix string, readerBufferSize int) fs.FileSetReader

type fileSystemSource struct {
	opts             Options
	log              xlog.Logger
	filePathPrefix   string
	readerBufferSize int
	newReaderFn      newFileSetReaderFn
}

func newFileSystemSource(prefix string, opts Options) bootstrap.Source {
	return &fileSystemSource{
		opts:             opts,
		log:              opts.GetBootstrapOptions().GetInstrumentOptions().GetLogger(),
		filePathPrefix:   prefix,
		readerBufferSize: opts.GetFilesystemOptions().GetReaderBufferSize(),
		newReaderFn:      fs.NewReader,
	}
}

func (s *fileSystemSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapParallel:
		return true
	case bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *fileSystemSource) Available(shardsTimeRanges bootstrap.ShardTimeRanges) bootstrap.ShardTimeRanges {
	result := make(map[uint32]xtime.Ranges)
	for shard, ranges := range shardsTimeRanges {
		result[shard] = s.shardAvailability(shard, ranges)
	}
	return result
}

func (s *fileSystemSource) shardAvailability(shard uint32, targetRangesForShard xtime.Ranges) xtime.Ranges {
	if targetRangesForShard == nil {
		return nil
	}

	entries := fs.ReadInfoFiles(s.filePathPrefix, shard, s.readerBufferSize)
	if len(entries) == 0 {
		return nil
	}

	tr := xtime.NewRanges()
	for i := 0; i < len(entries); i++ {
		info := entries[i]
		t := xtime.FromNanoseconds(info.Start)
		w := time.Duration(info.BlockSize)
		currRange := xtime.Range{Start: t, End: t.Add(w)}
		if targetRangesForShard.Overlaps(currRange) {
			tr = tr.AddRange(currRange)
		}
	}
	return tr
}

func (s *fileSystemSource) Read(shardsTimeRanges bootstrap.ShardTimeRanges) (bootstrap.Result, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	result := bootstrap.NewResult()
	for shard, tr := range shardsTimeRanges {
		var files []string
		fs.ForEachInfoFile(s.filePathPrefix, shard, s.readerBufferSize, func(fname string, _ []byte) {
			files = append(files, fname)
		})
		if len(files) == 0 {
			result.AddShardResult(shard, nil, tr)
			continue
		}

		bopts := s.opts.GetBootstrapOptions()
		seriesMap := bootstrap.NewShardResult(bopts)
		r := s.newReaderFn(s.filePathPrefix, s.readerBufferSize)
		for i := 0; i < len(files); i++ {
			t, err := fs.TimeFromFileName(files[i])
			if err != nil {
				s.log.Errorf("unable to get time from info file name %s: %v", files[i], err)
				continue
			}
			if err := r.Open(shard, t); err != nil {
				s.log.Errorf("unable to open info file for shard %d time %v: %v", shard, t, err)
				continue
			}
			timeRange := r.Range()
			if !tr.Overlaps(timeRange) {
				r.Close()
				continue
			}
			hasError := false
			curMap := bootstrap.NewShardResult(bopts)
			for i := 0; i < r.Entries(); i++ {
				id, data, err := r.Read()
				if err != nil {
					s.log.Errorf("error reading data file for shard %d time %v: %v", shard, t, err)
					hasError = true
					break
				}
				encoder := bopts.GetDatabaseBlockOptions().GetEncoderPool().Get()
				encoder.ResetSetData(timeRange.Start, data, false)
				block := bopts.GetDatabaseBlockOptions().GetDatabaseBlockPool().Get()
				block.Reset(timeRange.Start, encoder)
				curMap.AddBlock(id, block)
			}
			if !hasError {
				if err := r.Validate(); err != nil {
					hasError = true
					s.log.Errorf("data validation failed for shard %d time %v: %v", shard, t, err)
				}
			}
			r.Close()

			if !hasError {
				seriesMap.AddResult(curMap)
				tr = tr.RemoveRange(timeRange)
			} else {
				curMap.Close()
			}
		}

		result.AddShardResult(shard, seriesMap, tr)
	}

	return result, nil
}
