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
	"math/rand"
	"testing"
	"time"

	"github.com/m3db/m3db/src/dbnode/integration/generate"
	"github.com/m3db/m3db/src/dbnode/persist/fs"
	"github.com/m3db/m3db/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

var (
	defaultIntegrationTestFlushInterval = 100 * time.Millisecond
	defaultDerrangementPercent          = 0.20
)

type commitLogSeriesState struct {
	uniqueIndex uint64
	tags        ident.Tags
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
					tags:        blk.Tags,
				}
				idx++
			}
		}
	}
	return lookup
}

var letterRunes = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

func randStringRunes(n int) string {
	b := make([]rune, n)
	for i := range b {
		b[i] = letterRunes[rand.Intn(len(letterRunes))]
	}
	return string(b)
}

func generateSeriesMaps(numBlocks int, starts ...time.Time) generate.SeriesBlocksByStart {
	blockConfig := []generate.BlockConfig{}
	for i := 0; i < numBlocks; i++ {
		name := []string{}
		for j := 0; j < rand.Intn(10)+1; j++ {
			name = append(name, randStringRunes(100))
		}

		start := starts[rand.Intn(len(starts))]
		blockConfig = append(blockConfig, generate.BlockConfig{
			IDs:       name,
			NumPoints: rand.Intn(100) + 1,
			Start:     start,
		})
	}
	return generate.BlocksByStart(blockConfig)
}

func writeCommitLogData(
	t *testing.T,
	s *testSetup,
	opts commitlog.Options,
	data generate.SeriesBlocksByStart,
	namespace namespace.Metadata,
	genSnapshots bool,
) {
	writeCommitLogDataBase(t, s, opts, data, namespace, nil, nil)
}

func writeCommitLogDataSpecifiedTS(
	t *testing.T,
	s *testSetup,
	opts commitlog.Options,
	data generate.SeriesBlocksByStart,
	namespace namespace.Metadata,
	ts time.Time,
	genSnapshots bool,
) {
	writeCommitLogDataBase(t, s, opts, data, namespace, &ts, nil)
}

func writeCommitLogDataWithPredicate(
	t *testing.T,
	s *testSetup,
	opts commitlog.Options,
	data generate.SeriesBlocksByStart,
	namespace namespace.Metadata,
	pred generate.WriteDatapointPredicate,
) {
	writeCommitLogDataBase(t, s, opts, data, namespace, nil, pred)
}

func writeCommitLogDataBase(
	t *testing.T,
	s *testSetup,
	opts commitlog.Options,
	data generate.SeriesBlocksByStart,
	namespace namespace.Metadata,
	specifiedTS *time.Time,
	pred generate.WriteDatapointPredicate,
) {
	if pred == nil {
		pred = generate.WriteAllPredicate
	}

	// ensure commit log is flushing frequently
	require.Equal(
		t, defaultIntegrationTestFlushInterval, opts.FlushInterval())

	var (
		seriesLookup = newCommitLogSeriesStates(data)
		shardSet     = s.shardSet
	)

	// Write out commit log data
	for ts, blk := range data {
		if specifiedTS != nil {
			s.setNowFn(*specifiedTS)
		} else {
			s.setNowFn(ts.ToTime())
		}

		ctx := context.NewContext()
		defer ctx.Close()

		m := map[xtime.UnixNano]generate.SeriesBlock{
			ts: blk,
		}

		points := generate.
			ToPointsByTime(m).
			Dearrange(defaultDerrangementPercent)

		// create new commit log
		commitLog, err := commitlog.NewCommitLog(opts)
		require.NoError(t, err)
		require.NoError(t, commitLog.Open())

		// write points
		for _, point := range points {
			series, ok := seriesLookup[point.ID.String()]
			require.True(t, ok)
			cId := commitlog.Series{
				Namespace:   namespace.ID(),
				Shard:       shardSet.Lookup(point.ID),
				ID:          point.ID,
				Tags:        series.tags,
				UniqueIndex: series.uniqueIndex,
			}
			if pred(point.Datapoint) {
				require.NoError(t, commitLog.Write(ctx, cId, point.Datapoint, xtime.Second, nil))
			}
		}

		// ensure writes finished
		require.NoError(t, commitLog.Close())
	}
}

func writeSnapshotsWithPredicate(
	t *testing.T,
	s *testSetup,
	opts commitlog.Options,
	data generate.SeriesBlocksByStart,
	namespace namespace.Metadata,
	specifiedTS *time.Time,
	pred generate.WriteDatapointPredicate,
	snapshotInterval time.Duration,
) {
	// Write out snapshots
	err := writeTestSnapshotsToDiskWithPredicate(
		namespace, s, data, pred, snapshotInterval)
	require.NoError(t, err)
}

func mustInspectFilesystem(fsOpts fs.Options) fs.Inspection {
	inspection, err := fs.InspectFilesystem(fsOpts)
	if err != nil {
		panic(err)
	}

	return inspection
}
