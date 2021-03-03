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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3/src/x/context"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapRetriesDueToError(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan bool)

	setup := bootstrapRetryTestSetup(t, func(
		ctx context.Context,
		namespaces bootstrap.Namespaces,
		cache bootstrap.Cache,
	) (bootstrap.NamespaceResults, error) {
		shouldError := <-signalCh
		if shouldError {
			return bootstrap.NamespaceResults{}, errors.New("error in bootstrapper")
		}
		// Mark all as fulfilled
		noopNone := bootstrapper.NewNoOpAllBootstrapperProvider()
		bs, err := noopNone.Provide()
		require.NoError(t, err)
		return bs.Bootstrap(ctx, namespaces, cache)
	})

	go func() {
		// Wait for server to start
		setup.WaitUntilServerIsUp()

		// First bootstrap pass. Bootstrapper produces an error. Check if DB is not marked bootstrapped.
		signalCh <- true
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Bootstrap retry. Bootstrapper completes persist range without errors. Check if DB isn't
		// marked as bootstrapped on the second pass.
		signalCh <- false
		assert.False(t, setup.DB().IsBootstrapped(), "database should not yet be bootstrapped")

		// Still boostrap retry. Bootstrapper completes in-memory range without errors. DB finishes bootstrapping.
		signalCh <- false
	}()

	require.NoError(t, setup.StartServer()) // Blocks until bootstrap is complete
	defer func() {
		require.NoError(t, setup.StopServer())
	}()

	assert.True(t, setup.DB().IsBootstrapped(), "database should be bootstrapped")
}

func TestBootstrapRetriesDueToUnfulfilledRanges(t *testing.T) {
	// Setup the test bootstrapper to only proceed when a signal is sent.
	signalCh := make(chan bool)

	setup := bootstrapRetryTestSetup(t, func(
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
		// Wait for server to start
		setup.WaitUntilServerIsUp()

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
}

type bootstrapFn = func(
	ctx context.Context,
	namespaces bootstrap.Namespaces,
	cache bootstrap.Cache,
) (bootstrap.NamespaceResults, error)

func bootstrapRetryTestSetup(t *testing.T, bootstrapFn bootstrapFn) TestSetup {
	rOpts := retention.NewOptions().
		SetRetentionPeriod(12 * time.Hour).
		SetBufferPast(5 * time.Minute).
		SetBufferFuture(5 * time.Minute)

	ns1, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().SetRetentionOptions(rOpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns1})

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	var (
		fsOpts = setup.StorageOpts().CommitLogOptions().FilesystemOptions()

		bootstrapOpts          = newDefaulTestResultOptions(setup.StorageOpts())
		bootstrapperSourceOpts = testBootstrapperSourceOptions{read: bootstrapFn}
		boostrapper            = newTestBootstrapperSource(bootstrapperSourceOpts, bootstrapOpts, nil)

		processOpts = bootstrap.NewProcessOptions().
				SetTopologyMapProvider(setup).
				SetOrigin(setup.Origin())
	)

	processProvider, err := bootstrap.NewProcessProvider(
		boostrapper, processOpts, bootstrapOpts, fsOpts)
	require.NoError(t, err)
	setup.SetStorageOpts(setup.StorageOpts().SetBootstrapProcessProvider(processProvider))
	return setup
}
