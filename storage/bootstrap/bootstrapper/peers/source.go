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
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3x/log"
	"github.com/m3db/m3x/time"
)

type peersSource struct {
	opts Options
	log  xlog.Logger
}

func newPeersSource(opts Options) bootstrap.Source {
	return &peersSource{
		opts: opts,
		log:  opts.GetBootstrapOptions().GetInstrumentOptions().GetLogger(),
	}
}

func (s *peersSource) Can(strategy bootstrap.Strategy) bool {
	switch strategy {
	case bootstrap.BootstrapParallel, bootstrap.BootstrapSequential:
		return true
	}
	return false
}

func (s *peersSource) Available(shardsTimeRanges bootstrap.ShardTimeRanges) bootstrap.ShardTimeRanges {
	// Peers should be able to fulfill all data
	return shardsTimeRanges
}

func (s *peersSource) Read(shardsTimeRanges bootstrap.ShardTimeRanges) (bootstrap.Result, error) {
	if shardsTimeRanges.IsEmpty() {
		return nil, nil
	}

	var (
		bopts   = s.opts.GetBootstrapOptions()
		session = s.opts.GetAdminSession()
		result  = bootstrap.NewResult()
	)
	for shard, ranges := range shardsTimeRanges {
		it := ranges.Iter()
		for it.Next() {
			currRange := it.Value()
			start := currRange.Start
			end := currRange.End
			shardResult, err := session.FetchBootstrapBlocksFromPeers(shard, start, end, bopts)
			if err == nil {
				result.Add(shard, shardResult, nil)
			} else {
				result.Add(shard, nil, xtime.NewRanges().AddRange(currRange))
			}
		}
	}

	return result, nil
}
