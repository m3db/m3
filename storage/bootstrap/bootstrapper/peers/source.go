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
	"time"

	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3x/time"
)

// peersSource is a source for bootstrap data from peers
type peersSource struct {
	opts Options
}

// newPeersSource creates a source for bootstrap data from peers
func newPeersSource(opts Options) bootstrap.Source {
	return &peersSource{opts: opts}
}

// GetAvailability returns what time ranges are available for a given shard
func (s *peersSource) GetAvailability(
	namespace string,
	shard uint32,
	targetRangesForShard xtime.Ranges,
) xtime.Ranges {
	return targetRangesForShard
}

// ReadData returns raw series for a given shard within certain time ranges
func (s *peersSource) ReadData(
	namespace string,
	shard uint32,
	tr xtime.Ranges,
) (bootstrap.ShardResult, xtime.Ranges) {
	if xtime.IsEmpty(tr) {
		return nil, tr
	}

	bopts := s.opts.GetBootstrapOptions()
	blockSize := bopts.GetRetentionOptions().GetBlockSize()
	session := s.opts.GetAdminSession()
	result, err := session.FetchBootstrapBlocksFromPeers(namespace, shard, time.Time{}, time.Now().Add(blockSize), bopts)
	if err != nil {
		return nil, tr
	}

	return result, nil
}
