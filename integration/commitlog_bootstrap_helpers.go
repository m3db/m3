// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

package integration

import (
	"testing"
	"time"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/block"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	defaultIntegrationTestFlushInterval = 10 * time.Millisecond
)

func computeMetricIndexes(timeBlocks generate.SeriesBlocksByStart) map[string]uint64 {
	var idx uint64
	indexes := make(map[string]uint64)
	for _, blks := range timeBlocks {
		for _, blk := range blks {
			id := blk.ID.String()
			if _, ok := indexes[id]; !ok {
				indexes[id] = idx
				idx++
			}
		}
	}
	return indexes
}

func writeCommitLog(
	t *testing.T,
	s *testSetup,
	data generate.SeriesBlocksByStart,
	namespace ts.ID,
) {
	indexes := computeMetricIndexes(data)
	opts := s.storageOpts.CommitLogOptions()

	// ensure commit log is flushing frequently
	require.Equal(t, defaultIntegrationTestFlushInterval, opts.FlushInterval())
	ctx := context.NewContext()

	var (
		shardSet  = s.shardSet
		points    = generate.ToPointsByTime(data) // points are sorted in chronological order
		blockSize = opts.RetentionOptions().BlockSize()
		commitLog commitlog.CommitLog
		now       time.Time
	)

	closeCommitLogFn := func() {
		if commitLog != nil {
			// wait a bit to ensure writes are done, and then close the commit log
			time.Sleep(10 * defaultIntegrationTestFlushInterval)
			require.NoError(t, commitLog.Close())
		}

	}

	for _, point := range points {
		pointTime := point.Timestamp

		// check if this point falls in the current commit log block
		if truncated := pointTime.Truncate(blockSize); truncated != now {
			// close commit log if it exists
			closeCommitLogFn()
			// move time forward
			now = truncated
			s.setNowFn(now)
			// create new commit log
			commitLog = commitlog.NewCommitLog(opts)
			require.NoError(t, commitLog.Open())
		}

		// write this point
		idx, ok := indexes[point.ID.String()]
		require.True(t, ok)
		cId := commitlog.Series{
			Namespace:   namespace,
			Shard:       shardSet.Lookup(point.ID),
			ID:          point.ID,
			UniqueIndex: idx,
		}
		require.NoError(t, commitLog.WriteBehind(ctx, cId, point.Datapoint, xtime.Second, nil))
	}

	closeCommitLogFn()
}

func testSetupMetadatas(
	t *testing.T,
	testSetup *testSetup,
	namespace ts.ID,
	start time.Time,
	end time.Time,
) map[uint32][]block.ReplicaMetadata {
	// Retrieve written data using the AdminSession APIs
	// FetchMetadataBlocksFromPeers/FetchBlocksFromPeers
	adminClient := testSetup.m3dbAdminClient
	metadatasByShard, err := m3dbClientFetchBlocksMetadata(
		adminClient, namespace, testSetup.shardSet.AllIDs(), start, end)
	require.NoError(t, err)
	return metadatasByShard
}
