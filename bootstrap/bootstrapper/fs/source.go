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

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3x/time"
)

// fileSystemSource provides information about TSDB data stored on disk.
type fileSystemSource struct {
	opts             m3db.DatabaseOptions
	filePathPrefix   string
	readerBufferSize int
	newReaderFn      m3db.NewFileSetReaderFn
}

// newFileSystemSource creates a new filesystem based database.
func newFileSystemSource(prefix string, opts m3db.DatabaseOptions) m3db.Source {
	return &fileSystemSource{
		opts:             opts,
		filePathPrefix:   prefix,
		readerBufferSize: opts.GetReaderBufferSize(),
		newReaderFn:      fs.NewReader,
	}
}

// GetAvailability returns what time ranges are available for a given shard.
func (fss *fileSystemSource) GetAvailability(shard uint32, targetRangesForShard xtime.Ranges) xtime.Ranges {
	if targetRangesForShard == nil {
		return nil
	}

	entries := fs.ReadInfoFiles(fss.filePathPrefix, shard, fss.readerBufferSize)
	if len(entries) == 0 {
		return nil
	}

	tr := xtime.NewRanges()
	for i := 0; i < len(entries); i++ {
		info := entries[i]
		t := xtime.FromNanoseconds(info.Start)
		w := time.Duration(info.BlockSize)
		curRange := xtime.Range{t, t.Add(w)}
		if targetRangesForShard.Overlaps(curRange) {
			tr = tr.AddRange(curRange)
		}
	}
	return tr
}

// ReadData returns raw series for a given shard within certain time ranges.
func (fss *fileSystemSource) ReadData(shard uint32, tr xtime.Ranges) (m3db.ShardResult, xtime.Ranges) {
	if xtime.IsEmpty(tr) {
		return nil, tr
	}

	var files []string
	fs.ForEachInfoFile(fss.filePathPrefix, shard, fss.readerBufferSize, func(fname string, _ []byte) {
		files = append(files, fname)
	})
	if len(files) == 0 {
		return nil, tr
	}

	log := fss.opts.GetLogger()
	seriesMap := bootstrap.NewShardResult(fss.opts)
	r := fss.newReaderFn(fss.filePathPrefix, fss.readerBufferSize)
	for i := 0; i < len(files); i++ {
		t, err := fs.TimeFromFileName(files[i])
		if err != nil {
			log.Errorf("unable to get time from info file name %s: %v", files[i], err)
			continue
		}
		if err := r.Open(shard, t); err != nil {
			log.Errorf("unable to open info file for shard %d time %v: %v", shard, t, err)
			continue
		}
		timeRange := r.Range()
		if !tr.Overlaps(timeRange) {
			r.Close()
			continue
		}
		hasError := false
		curMap := bootstrap.NewShardResult(fss.opts)
		for i := 0; i < r.Entries(); i++ {
			id, data, err := r.Read()
			if err != nil {
				log.Errorf("error reading data file for shard %d time %v: %v", shard, t, err)
				hasError = true
				break
			}
			encoder := fss.opts.GetEncoderPool().Get()
			encoder.ResetSetData(timeRange.Start, data, false)
			block := fss.opts.GetDatabaseBlockPool().Get()
			block.Reset(timeRange.Start, encoder)
			curMap.AddBlock(id, block)
		}
		if !hasError {
			if err := r.Validate(); err != nil {
				hasError = true
				log.Errorf("data validation failed for shard %d time %v: %v", shard, t, err)
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
	return seriesMap, tr
}
