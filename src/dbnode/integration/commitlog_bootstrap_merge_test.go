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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"

	"github.com/stretchr/testify/require"
)

// Consider a database running with a single namespaces, and the following retention opts:
//
//           | BlockSize | retention Period
// ns1       |    2h     |      8h
// commitLog |   15m     |      8h
//
// We have a block for each of the two at each marker in the diagram below.
//
// time (flowing left --> right):
// time-label: t0  t1  t2  t3
//  ns1        .   .          [blocksize . is 2h]
//  commitlog      ,,,,,,,,,  [blocksize , is 30min]
//
// The test creates the blocks below, and verifies the bootstrappers are able to merge the data
// successfully.
// - ns1 block at t0 and t1
// - commit log blocks from [t1, t3]
func TestCommitLogAndFSMergeBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		ns1BlockSize = 2 * time.Hour
		ns1ROpts     = rOpts.SetBlockSize(ns1BlockSize)
	)
	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(ns1ROpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("commit log + fs merge bootstrap test")

	// generate and write test data
	var (
		t0 = setup.NowFn()()
		t1 = t0.Add(ns1BlockSize)
		t2 = t1.Add(ns1BlockSize)
		t3 = t2.Add(ns1BlockSize)
	)
	blockConfigs := []generate.BlockConfig{
		{IDs: []string{"foo", "bar"}, NumPoints: 20, Start: t0},
		{IDs: []string{"nah", "baz"}, NumPoints: 50, Start: t1},
		{IDs: []string{"hax", "ord"}, NumPoints: 30, Start: t2},
	}
	log.Info("generating data")
	seriesMaps := generate.BlocksByStart(blockConfigs)
	log.Info("generated data")

	log.Info("writing filesets")
	fsSeriesMaps := generate.SeriesBlocksByStart{
		t0: seriesMaps[t0],
		t1: seriesMaps[t1],
	}
	require.NoError(t, writeTestDataToDiskWithIndex(ns1, setup, fsSeriesMaps))

	log.Info("writing commit logs")
	commitlogSeriesMaps := generate.SeriesBlocksByStart{
		t1: seriesMaps[t1],
		t2: seriesMaps[t2],
	}
	writeCommitLogData(t, setup, commitLogOpts, commitlogSeriesMaps, ns1, false)

	require.NoError(t, setup.InitializeBootstrappers(InitializeBootstrappersOptions{
		CommitLogOptions: commitLogOpts,
		WithCommitLog:    true,
		WithFileSystem:   true,
	}))

	log.Info("moving time forward and starting server")
	setup.SetNowFn(t3)
	// Start the server
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	log.Info("validating bootstrapped data")
	verifySeriesMaps(t, setup, ns1.ID(), seriesMaps)
}
