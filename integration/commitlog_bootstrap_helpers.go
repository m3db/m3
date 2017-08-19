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
	defaultIntegrationTestFlushInterval = 100 * time.Millisecond
	defaultDerrangementPercent          = 0.20
)

type commitLogSeriesState struct {
	uniqueIndex uint64
}

func newCommitLogSeriesStates(
	timeBlocks generate.SeriesBlocksByStart,
) map[string]*commitLogSeriesState {
	var idx uint64
	lookup := make(map[string]*commitLogSeriesState)
	for _, blks := range timeBlocks {
		for _, blk := range blks {
			id := blk.ID.String()
			if _, ok := lookup[id]; !ok {
				lookup[id] = &commitLogSeriesState{
					uniqueIndex: idx,
				}
				idx++
			}
		}
	}
	return lookup
}

// nolint: deadcode
func writeCommitLogData(
	t *testing.T,
	s *testSetup,
	data generate.SeriesBlocksByStart,
	namespace ts.ID,
) {
	// ensure commit log is flushing frequently
	opts := s.storageOpts.CommitLogOptions()
	require.Equal(t, defaultIntegrationTestFlushInterval, opts.FlushInterval())

	var (
		seriesLookup = newCommitLogSeriesStates(data)
		shardSet     = s.shardSet
	)

	for ts, blk := range data {
		ctx := context.NewContext()
		defer ctx.Close()

		s.setNowFn(ts)
		m := map[time.Time]generate.SeriesBlock{
			ts: blk,
		}

		points := generate.
			ToPointsByTime(m).
			Dearrange(defaultDerrangementPercent)

		// create new commit log
		commitLog := commitlog.NewCommitLog(opts)
		require.NoError(t, commitLog.Open())
		defer func() {
			require.NoError(t, commitLog.Close())
		}()

		// write points
		for _, point := range points {
			series, ok := seriesLookup[point.ID.String()]
			require.True(t, ok)
			cId := commitlog.Series{
				Namespace:   namespace,
				Shard:       shardSet.Lookup(point.ID),
				ID:          point.ID,
				UniqueIndex: series.uniqueIndex,
			}
			require.NoError(t, commitLog.Write(ctx, cId, point.Datapoint, xtime.Second, nil))
		}
	}
}

// nolint: deadcode
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
