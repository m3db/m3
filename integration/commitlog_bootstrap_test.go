// +build integration_disabled

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

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3db/context"
	"github.com/m3db/m3db/integration/generate"
	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bcl "github.com/m3db/m3db/storage/bootstrap/bootstrapper/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/time"
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
	shard uint32,
	namespace ts.ID,
) {
	indexes := computeMetricIndexes(data)
	opts := s.storageOpts.CommitLogOptions()

	// ensure commit log is flushing frequently
	require.Equal(t, defaultIntegrationTestFlushInterval, opts.FlushInterval())
	ctx := context.NewContext()

	var (
		points    = generate.ToPointsByTime(data) // points are sorted in chronological order
		blockSize = opts.RetentionOptions().BlockSize()
		commitLog commitlog.CommitLog
		err       error
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
			commitLog, err = commitlog.NewCommitLog(opts)
			require.NoError(t, err)
			require.NoError(t, commitLog.Open())
		}

		// write this point
		idx, ok := indexes[point.ID.String()]
		require.True(t, ok)
		cId := commitlog.Series{
			ID:          point.ID,
			Namespace:   namespace,
			Shard:       shard,
			UniqueIndex: idx,
		}
		// println(fmt.Sprintf("write: [ Series = %+v, datapoint = %+v ]", cId, point))
		require.NoError(t, commitLog.WriteBehind(ctx, cId, point.Datapoint, xtime.Second, nil))
	}

	closeCommitLogFn()
}

func readCommitLog(
	t *testing.T,
	s *testSetup,
) {
	iter, err := commitlog.NewIterator(s.storageOpts.CommitLogOptions())
	require.NoError(t, err)
	defer iter.Close()
	for iter.Next() {
		cId, dp, _, _ := iter.Current()
		println(fmt.Sprintf("read: [ Series = %+v, datapoint = %+v ]", cId, dp))
	}
	require.NoError(t, iter.Err())
}

func TestCommitLogBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		ropts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = ropts.BlockSize()
		ns1       = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ropts))
		opts      = newTestOptions().SetNamespaces([]namespace.Metadata{ns1})
	)
	setup, err := newTestSetup(opts)
	require.NoError(t, err)
	defer setup.close()

	commitLogOpts := setup.storageOpts.CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval).
		SetRetentionOptions(ropts)
	setup.storageOpts = setup.storageOpts.SetCommitLogOptions(commitLogOpts)

	noOpAll := bootstrapper.NewNoOpAllBootstrapper()
	bsOpts := result.NewOptions()
	bclOpts := bcl.NewOptions().
		SetResultOptions(bsOpts).
		SetCommitLogOptions(commitLogOpts)
	bs, err := bcl.NewCommitLogBootstrapper(bclOpts, noOpAll)
	require.NoError(t, err)
	process := bootstrap.NewProcess(bs, bsOpts)
	setup.storageOpts = setup.storageOpts.SetBootstrapProcess(process)

	log := setup.storageOpts.InstrumentOptions().Logger()
	log.Info("commit log bootstrap test")

	// Write test data
	log.Info("generating data")
	shard := uint32(0)
	now := setup.getNowFn()
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{[]string{"foo"}, 2 /* was 20 */, now.Add(-2 * blockSize)},
		// {[]string{"foo", "bar"}, 2 /* was 20 */, now.Add(-2 * blockSize)},
		// {[]string{"bar", "baz"}, 5 /* was 50 */, now.Add(-blockSize)},
	})
	_, err = setup.storageOpts.NamespaceRegistry().Get(testNamespaces[0])
	require.NoError(t, err)
	log.Info("writing data")
	writeCommitLog(t, setup, seriesMaps, shard, testNamespaces[0])
	log.Info("written data")

	// log.Infof("reading data")
	// readCommitLog(t, setup)
	// log.Infof("read data")

	setup.setNowFn(now)
	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.stopServer())
		log.Debug("server is now down")
	}()

	bootstrapped := waitUntil(func() bool {
		return setup.db.IsBootstrapped()
	}, time.Second*5)
	require.True(t, bootstrapped)

	// Verify in-memory data match what we expect
	verifySeriesMaps(t, setup, testNamespaces[0], seriesMaps)
	// verifySeriesMaps(t, setup, testNamespaces[1], nil) // TODO(prateek): resolves the commit log issue
}
