// +build integration

// Copyright (c) 2021 Uber Technologies, Inc.
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
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/dbnode/integration/generate"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
)

func TestBootstrapRetriesDueToError(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan bool)

	setup, testScope := bootstrapRetryTestSetup(t, func(
		ctx context.Context,
		namespaces bootstrap.Namespaces,
		cache bootstrap.Cache,
	) (bootstrap.NamespaceResults, error) {
		shouldError := <-signalCh
		if shouldError {
			return bootstrap.NamespaceResults{}, errors.New("error in bootstrapper")
		}
		// Mark all as fulfilled
		bs, err := bootstrapper.NewNoOpAllBootstrapperProvider().Provide()
		require.NoError(t, err)
		return bs.Bootstrap(ctx, namespaces, cache)
	})

	go func() {
		// Wait for server to get started by the main test method.
		require.NoError(t, setup.WaitUntilServerIsUp())

		// First bootstrap pass. Bootstrapper produces an error. Check if DB is not marked bootstrapped.
		signalCh <- true
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Bootstrap retry. Bootstrapper completes persist range without errors. Check if DB isn't
		// marked as bootstrapped on the second pass.
		signalCh <- false
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Still bootstrap retry. Bootstrapper completes in-memory range without errors. DB finishes bootstrapping.
		signalCh <- false
	}()

	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete
	defer func() {
		require.NoError(t, setup.StopServer())
	}()

	assert.True(t, setup.DB().IsBootstrapped(), "database should be bootstrapped")
	assertRetryMetric(t, testScope, "other")
}

func TestBootstrapRetriesDueToObsoleteRanges(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan struct{})

	setup, testScope := bootstrapRetryTestSetup(t, func(
		ctx context.Context,
		namespaces bootstrap.Namespaces,
		cache bootstrap.Cache,
	) (bootstrap.NamespaceResults, error) {
		// read from signalCh twice so we could advance the clock exactly in between of those signals
		<-signalCh
		<-signalCh
		bs, err := bootstrapper.NewNoOpAllBootstrapperProvider().Provide()
		require.NoError(t, err)
		return bs.Bootstrap(ctx, namespaces, cache)
	})

	go assertBootstrapRetry(t, setup, signalCh)

	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete
	defer func() {
		require.NoError(t, setup.StopServer())
	}()

	assert.True(t, setup.DB().IsBootstrapped(), "database should be bootstrapped")
	assertRetryMetric(t, testScope, "obsolete-ranges")
}

func TestNoOpenFilesWhenBootstrapRetriesDueToObsoleteRanges(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan struct{})

	setup, testScope := bootstrapRetryTestSetup(t, func(
		ctx context.Context,
		namespaces bootstrap.Namespaces,
		cache bootstrap.Cache,
	) (bootstrap.NamespaceResults, error) {
		// read from signalCh twice so we could advance the clock exactly in between of those signals
		<-signalCh
		<-signalCh
		bs, err := bootstrapper.NewNoOpAllBootstrapperProvider().Provide()
		require.NoError(t, err)
		return bs.Bootstrap(ctx, namespaces, cache)
	})

	go assertBootstrapRetry(t, setup, signalCh)

	// Write test data
	now := setup.NowFn()()

	fooSeries := generate.Series{
		ID:   ident.StringID("foo"),
		Tags: ident.NewTags(ident.StringTag("city", "new_york"), ident.StringTag("foo", "foo")),
	}

	barSeries := generate.Series{
		ID:   ident.StringID("bar"),
		Tags: ident.NewTags(ident.StringTag("city", "new_jersey")),
	}

	bazSeries := generate.Series{
		ID:   ident.StringID("baz"),
		Tags: ident.NewTags(ident.StringTag("city", "seattle")),
	}

	blockSize := 2 * time.Hour

	ns1 := setup.Namespaces()[0]
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{
			IDs:       []string{fooSeries.ID.String()},
			Tags:      fooSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-1 * blockSize),
		},
		{
			IDs:       []string{barSeries.ID.String()},
			Tags:      barSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(-1 * blockSize),
		},
		{
			IDs:       []string{fooSeries.ID.String()},
			Tags:      fooSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(1 * blockSize),
		},
		{
			IDs:       []string{barSeries.ID.String()},
			Tags:      barSeries.Tags,
			NumPoints: 100,
			Start:     now.Add(1 * blockSize),
		},
		{
			IDs:       []string{fooSeries.ID.String()},
			Tags:      fooSeries.Tags,
			NumPoints: 50,
			Start:     now,
		},
		{
			IDs:       []string{bazSeries.ID.String()},
			Tags:      bazSeries.Tags,
			NumPoints: 50,
			Start:     now,
		},
	})

	require.NoError(t, writeTestDataToDiskWithIndex(ns1, setup, seriesMaps, 0))
	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete
	defer func() {
		require.NoError(t, setup.StopServerAndVerifyOpenFilesAreClosed())
		setup.Close()
	}()

	assert.True(t, setup.DB().IsBootstrapped(), "database should be bootstrapped")
	assertRetryMetric(t, testScope, "obsolete-ranges")
}

func TestBootstrapRetriesDueToUnfulfilledRanges(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan bool)

	setup, testScope := bootstrapRetryTestSetup(t, func(
		ctx context.Context,
		namespaces bootstrap.Namespaces,
		cache bootstrap.Cache,
	) (bootstrap.NamespaceResults, error) {
		var provider bootstrap.BootstrapperProvider
		shouldUnfulfill := <-signalCh
		if shouldUnfulfill {
			provider = bootstrapper.NewNoOpNoneBootstrapperProvider()
		} else {
			provider = bootstrapper.NewNoOpAllBootstrapperProvider()
		}
		bs, err := provider.Provide()
		require.NoError(t, err)
		return bs.Bootstrap(ctx, namespaces, cache)
	})

	go func() {
		// Wait for server to get started by the main test method.
		require.NoError(t, setup.WaitUntilServerIsUp())

		// First bootstrap pass. Bootstrap produces unfulfilled ranges for persist range.
		// Check if DB is not marked bootstrapped.
		signalCh <- true
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")
		// Still first bootstrap pass. Bootstrap produces unfulfilled ranges for in-memory range.
		// Check if DB is not marked bootstrapped.
		signalCh <- true
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Bootstrap retry. Bootstrapper completes persist range fulfilling everything.
		// Check if DB isn't marked as bootstrapped on the second pass.
		signalCh <- false
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Still bootstrap retry. Bootstrapper completes in-memory range fulfilling everything.
		// DB finishes bootstrapping.
		signalCh <- false
	}()

	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete
	defer func() {
		require.NoError(t, setup.StopServer())
	}()

	assert.True(t, setup.DB().IsBootstrapped(), "database should be bootstrapped")

	assertRetryMetric(t, testScope, "other")
}

func assertBootstrapRetry(t *testing.T, setup TestSetup, signalCh chan struct{}) {
	// Wait for server to get started by the main test method.
	require.NoError(t, setup.WaitUntilServerIsUp())

	// First bootstrap pass, persist ranges. Check if DB is not marked bootstrapped and advance clock.
	signalCh <- struct{}{}
	assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")
	setup.SetNowFn(setup.NowFn()().Add(2 * time.Hour))
	signalCh <- struct{}{}

	// Still first bootstrap pass, in-memory ranges. Due to advanced clock previously calculated
	// ranges are obsolete. Check if DB is not marked bootstrapped.
	signalCh <- struct{}{}
	assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")
	signalCh <- struct{}{}

	// Bootstrap retry, persist ranges. Check if DB isn't marked as bootstrapped on the second pass.
	signalCh <- struct{}{}
	assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")
	signalCh <- struct{}{}
}

type bootstrapFn = func(
	ctx context.Context,
	namespaces bootstrap.Namespaces,
	cache bootstrap.Cache,
) (bootstrap.NamespaceResults, error)

func bootstrapRetryTestSetup(t *testing.T, bootstrapFn bootstrapFn) (TestSetup, tally.TestScope) {
	testScope := tally.NewTestScope("testScope", map[string]string{})

	rOpts := retention.NewOptions().
		SetRetentionPeriod(12 * time.Hour).
		SetBufferPast(5 * time.Minute).
		SetBufferFuture(5 * time.Minute)

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1}).
		SetShardSetOptions(&TestShardSetOptions{
			// Set all shards to initializing so bootstrap is
			// retried on an obsolete range (which is not done
			// if all shards are available and hence coming from disk).
			ShardState: shard.Initializing,
		})

	setup, err := NewTestSetup(t, opts, nil, func(storageOpts storage.Options) storage.Options {
		return storageOpts.SetInstrumentOptions(storageOpts.InstrumentOptions().SetMetricsScope(testScope))
	})
	require.NoError(t, err)
	defer setup.Close()

	var (
		fsOpts = setup.StorageOpts().CommitLogOptions().FilesystemOptions()

		bootstrapOpts          = newDefaulTestResultOptions(setup.StorageOpts())
		bootstrapperSourceOpts = testBootstrapperSourceOptions{read: bootstrapFn}
		processOpts            = bootstrap.NewProcessOptions().
					SetTopologyMapProvider(setup).
					SetOrigin(setup.Origin())
	)
	bootstrapOpts.SetInstrumentOptions(bootstrapOpts.InstrumentOptions().SetMetricsScope(testScope))
	boostrapper := newTestBootstrapperSource(bootstrapperSourceOpts, bootstrapOpts, nil)

	processProvider, err := bootstrap.NewProcessProvider(
		boostrapper, processOpts, bootstrapOpts, fsOpts)
	require.NoError(t, err)
	setup.SetStorageOpts(setup.StorageOpts().SetBootstrapProcessProvider(processProvider))
	return setup, testScope
}

func assertRetryMetric(t *testing.T, testScope tally.TestScope, expectedReason string) {
	const (
		metricName = "bootstrap-retries"
		reasonTag  = "reason"
	)
	valuesByReason := make(map[string]int)
	for _, counter := range testScope.Snapshot().Counters() {
		if strings.Contains(counter.Name(), metricName) {
			reason := ""
			if r, ok := counter.Tags()[reasonTag]; ok {
				reason = r
			}
			valuesByReason[reason] = int(counter.Value())
		}
	}

	val, ok := valuesByReason[expectedReason]
	if assert.True(t, ok, "missing metric for expected reason") {
		assert.Equal(t, 1, val)
	}
	for r, val := range valuesByReason {
		if r != expectedReason {
			assert.Equal(t, 0, val)
		}
	}
}
