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
	"os"
	"time"

	"github.com/m3db/m3db"
	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/storage"
	xtime "github.com/m3db/m3db/x/time"
)

// fileSystemSource provides information about TSDB data stored on disk.
type fileSystemSource struct {
	opts           memtsdb.DatabaseOptions
	filePathPrefix string
}

// newFileSystemSource creates a new filesystem based database.
func newFileSystemSource(prefix string, opts memtsdb.DatabaseOptions) memtsdb.Source {
	return &fileSystemSource{
		opts:           opts,
		filePathPrefix: prefix,
	}
}

func (fss *fileSystemSource) getInfoFiles(shard uint32) ([]string, error) {
	shardDir := fs.ShardDirPath(fss.filePathPrefix, shard)
	return fs.InfoFiles(shardDir)
}

// GetAvailability returns what time ranges are available for a given shard.
func (fss *fileSystemSource) GetAvailability(shard uint32, targetRangesForShard xtime.Ranges) xtime.Ranges {
	log := fss.opts.GetLogger()

	if targetRangesForShard == nil {
		return nil
	}
	files, err := fss.getInfoFiles(shard)
	if err != nil {
		log.Errorf("unable to get info files for shard %d: %v", shard, err)
		return nil
	}

	numFiles := len(files)
	tr := xtime.NewRanges()
	for i := 0; i < numFiles; i++ {
		f, err := os.Open(files[i])
		if err != nil {
			log.Errorf("unable to open info file %s: %v", files[i], err)
			continue
		}
		info, err := fs.ReadInfo(f)
		f.Close()
		if err != nil {
			log.Errorf("unable to read info file %s: %v", files[i], err)
			continue
		}
		t := xtime.FromNanoseconds(info.Start)
		w := time.Duration(info.BlockSize)
		curRange := xtime.Range{t, t.Add(w)}
		if targetRangesForShard.Contains(curRange) {
			tr = tr.AddRange(curRange)
		}
	}
	return tr
}

// ReadData returns raw series for a given shard within certain time ranges.
func (fss *fileSystemSource) ReadData(shard uint32, tr xtime.Ranges) (memtsdb.ShardResult, xtime.Ranges) {
	log := fss.opts.GetLogger()

	if xtime.IsEmpty(tr) {
		return nil, tr
	}
	files, err := fss.getInfoFiles(shard)
	if err != nil {
		log.Errorf("unable to get info files for shard %d: %v", shard, err)
		return nil, tr
	}
	seriesMap := bootstrap.NewShardResult(fss.opts)
	r := fs.NewReader(fss.filePathPrefix)
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
		if !tr.Contains(timeRange) {
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
			block := storage.NewDatabaseBlock(timeRange.Start, encoder, fss.opts)
			curMap.AddBlock(id, block)
		}
		r.Close()

		if !hasError {
			seriesMap.AddResult(curMap)
			tr = tr.RemoveRange(timeRange)
		}
	}
	return seriesMap, tr
}
