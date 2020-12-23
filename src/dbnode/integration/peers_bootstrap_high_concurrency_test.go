// +build integration

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

	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

func TestPeersBootstrapHighConcurrencyBatch16Workers64(t *testing.T) {
	testPeersBootstrapHighConcurrency(t,
		testPeersBootstrapHighConcurrencyOptions{
			BatchSize:        16,
			Concurrency:      64,
			BatchesPerWorker: 8,
		})
}

func TestPeersBootstrapHighConcurrencyBatch64Workers16(t *testing.T) {
	testPeersBootstrapHighConcurrency(t,
		testPeersBootstrapHighConcurrencyOptions{
			BatchSize:        64,
			Concurrency:      16,
			BatchesPerWorker: 8,
		})
}

type testPeersBootstrapHighConcurrencyOptions struct {
	BatchSize        int
	Concurrency      int
	BatchesPerWorker int
}

func testPeersBootstrapHighConcurrency(
	t *testing.T,
	testOpts testPeersBootstrapHighConcurrencyOptions,
) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)

	blockSize := 2 * time.Hour

	idxOpts := namespace.NewIndexOptions().
		SetEnabled(true).
		SetBlockSize(blockSize)

	rOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		SetBlockSize(blockSize).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)

	nOpts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(idxOpts)

	namesp, err := namespace.NewMetadata(testNamespaces[0], nOpts)
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	batchSize := 16
	concurrency := 64
	setupOpts := []BootstrappableTestSetupOptions{
		{
			DisablePeersBootstrapper: true,
		},
		{
			DisablePeersBootstrapper:   false,
			BootstrapBlocksBatchSize:   batchSize,
			BootstrapBlocksConcurrency: concurrency,
		},
	}
	setups, closeFn := NewDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()

	// Write test data for first node
	numSeries := testOpts.BatchesPerWorker * testOpts.Concurrency * testOpts.BatchSize
	log.Sugar().Debugf("testing a total of %d IDs with %d batch size %d concurrency",
		numSeries, testOpts.BatchSize, testOpts.Concurrency)

	now := setups[0].NowFn()()
	commonTags := []ident.Tag{
		{
			Name:  ident.StringID("fruit"),
			Value: ident.StringID("apple"),
		},
	}
	numPoints := 10
	seriesMaps := generate.BlocksByStart(blockConfigs(
		generateTaggedBlockConfigs(generateTaggedBlockConfig{
			series:     numSeries,
			numPoints:  numPoints,
			commonTags: commonTags,
			blockStart: now.Add(-3 * blockSize),
		}),
		generateTaggedBlockConfigs(generateTaggedBlockConfig{
			series:     numSeries,
			numPoints:  numPoints,
			commonTags: commonTags,
			blockStart: now.Add(-2 * blockSize),
		}),
		generateTaggedBlockConfigs(generateTaggedBlockConfig{
			series:     numSeries,
			numPoints:  numPoints,
			commonTags: commonTags,
			blockStart: now.Add(-1 * blockSize),
		}),
		generateTaggedBlockConfigs(generateTaggedBlockConfig{
			series:     numSeries,
			numPoints:  numPoints,
			commonTags: commonTags,
			blockStart: now,
		}),
	))
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps, 0)
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].StartServer())

	// Start the last server with peers and filesystem bootstrappers
	bootstrapStart := time.Now()
	require.NoError(t, setups[1].StartServer())
	log.Debug("servers are now up", zap.Duration("took", time.Since(bootstrapStart)))

	// Stop the servers
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, namesp.ID(), seriesMaps)
	}

	// Issue some index queries to the second node which bootstrapped the metadata
	session, err := setups[1].M3DBClient().DefaultSession()
	require.NoError(t, err)

	start := now.Add(-rOpts.RetentionPeriod())
	end := now.Add(blockSize)
	queryOpts := index.QueryOptions{StartInclusive: start, EndExclusive: end}

	// Match on common tags
	termQuery := idx.NewTermQuery(commonTags[0].Name.Bytes(), commonTags[0].Value.Bytes())
	iter, _, err := session.FetchTaggedIDs(namesp.ID(),
		index.Query{Query: termQuery}, queryOpts)
	require.NoError(t, err)
	defer iter.Finalize()

	count := 0
	for iter.Next() {
		count++
	}
	require.Equal(t, numSeries, count)
}

type generateTaggedBlockConfig struct {
	series     int
	numPoints  int
	commonTags []ident.Tag
	blockStart time.Time
}

func generateTaggedBlockConfigs(
	cfg generateTaggedBlockConfig,
) []generate.BlockConfig {
	results := make([]generate.BlockConfig, 0, cfg.series)
	for i := 0; i < cfg.series; i++ {
		id := fmt.Sprintf("series_%d", i)
		tags := make([]ident.Tag, 0, 1+len(cfg.commonTags))
		tags = append(tags, ident.Tag{
			Name:  ident.StringID("series"),
			Value: ident.StringID(fmt.Sprintf("%d", i)),
		})
		tags = append(tags, cfg.commonTags...)
		results = append(results, generate.BlockConfig{
			IDs:       []string{id},
			Tags:      ident.NewTags(tags...),
			NumPoints: cfg.numPoints,
			Start:     cfg.blockStart,
		})
	}
	return results
}

func blockConfigs(cfgs ...[]generate.BlockConfig) []generate.BlockConfig {
	var results []generate.BlockConfig
	for _, elem := range cfgs {
		results = append(results, elem...)
	}
	return results
}
