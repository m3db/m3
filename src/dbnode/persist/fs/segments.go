// Copyright (c) 2020 Uber Technologies, Inc
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE

package fs

import (
	"time"

	"github.com/m3db/m3/src/dbnode/generated/proto/index"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
	idxpersist "github.com/m3db/m3/src/m3ninx/persist"
	xtime "github.com/m3db/m3/src/x/time"
)

type segments struct {
	absoluteFilepaths []string
	shardRanges       result.ShardTimeRanges
	volumeType        idxpersist.IndexVolumeType
	volumeIndex       int
	blockStart        xtime.UnixNano
}

// NewSegments returns an on disk segments for an index info file.
func NewSegments(
	info index.IndexVolumeInfo,
	volumeIndex int,
	absoluteFilepaths []string,
) Segments {
	sr := result.NewShardTimeRanges()
	indexBlockStart := xtime.UnixNano(info.BlockStart)
	indexBlockRange := xtime.Range{
		Start: indexBlockStart,
		End:   indexBlockStart.Add(time.Duration(info.BlockSize)),
	}
	for _, shard := range info.Shards {
		ranges, ok := sr.Get(shard)
		if !ok {
			ranges = xtime.NewRanges()
			sr.Set(shard, ranges)
		}
		ranges.AddRange(indexBlockRange)
	}
	volumeType := idxpersist.DefaultIndexVolumeType
	if info.IndexVolumeType != nil {
		volumeType = idxpersist.IndexVolumeType(info.IndexVolumeType.Value)
	}
	return &segments{
		shardRanges:       sr,
		volumeType:        volumeType,
		volumeIndex:       volumeIndex,
		absoluteFilepaths: absoluteFilepaths,
		blockStart:        indexBlockStart,
	}
}

func (o *segments) ShardTimeRanges() result.ShardTimeRanges {
	return o.shardRanges
}

func (o *segments) VolumeType() idxpersist.IndexVolumeType {
	return o.volumeType
}

func (o *segments) AbsoluteFilePaths() []string {
	return o.absoluteFilepaths
}

func (o *segments) VolumeIndex() int {
	return o.volumeIndex
}

func (o *segments) BlockStart() xtime.UnixNano {
	return o.blockStart
}
