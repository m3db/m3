// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/bootstrapper/uninitialized"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapMajorityDown(t *testing.T) {
	if testing.Short() {
		t.SkipNow()
	}

	// Test setups
	log := xtest.NewLogger(t)
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(20 * time.Hour).
		SetBlockSize(2 * time.Hour).
		SetBufferPast(10 * time.Minute).
		SetBufferFuture(2 * time.Minute)

	nsID := testNamespaces[0]
	namesp, err := namespace.NewMetadata(nsID,
		namespace.NewOptions().SetRetentionOptions(retentionOpts))
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{namesp}).
		// Use TChannel clients for writing / reading because we want to target individual nodes at a time
		// and not write/read all nodes in the cluster.
		SetUseTChannelClientForWriting(true).
		SetUseTChannelClientForReading(true)

	setupOpts := []bootstrappableTestSetupOptions{
		{
			finalBootstrapper:         uninitialized.UninitializedTopologyBootstrapperName,
			bootstrapConsistencyLevel: topology.ReadConsistencyLevelMajority,
			disablePeersBootstrapper:  false,
			enableRepairs:             false,
		},
		{
			finalBootstrapper:         uninitialized.UninitializedTopologyBootstrapperName,
			bootstrapConsistencyLevel: topology.ReadConsistencyLevelMajority,
			disablePeersBootstrapper:  false,
			enableRepairs:             false,
		},
		{
			finalBootstrapper:         uninitialized.UninitializedTopologyBootstrapperName,
			bootstrapConsistencyLevel: topology.ReadConsistencyLevelMajority,
			disablePeersBootstrapper:  false,
			enableRepairs:             false,
		},
	}
	setups, closeFn := newDefaultBootstrappableTestSetups(t, opts, setupOpts)
	defer closeFn()
	// setups[0].StorageOpts().RuntimeOptionsManager() // Set consistency level to none initially and then to majority?
	// Write test data for first node
	now := setups[0].NowFn()()
	// Make sure we have multiple blocks of data for multiple series to exercise
	// the grouping and aggregating logic in the client peer bootstrapping process
	seriesMaps := generate.BlocksByStart([]generate.BlockConfig{
		{IDs: []string{"foo"}, NumPoints: 1, Start: now},
	})
	err = writeTestDataToDisk(namesp, setups[0], seriesMaps, 0)
	require.NoError(t, err)

	for _, setup := range setups {
		require.NoError(t, setup.StartServer())
	}

	require.NoError(t, setups[0].StopServer())

	ctx := context.NewContext()
	require.NoError(t, setups[1].DB().Write(ctx, nsID, ident.StringID("while-setup-0-stopped"), now, 123456, xtime.Second, nil))
	ctx.Close()

	ctx = context.NewContext()
	require.NoError(t, setups[2].DB().Write(ctx, nsID, ident.StringID("while-setup-0-stopped"), now, 123456, xtime.Second, nil))
	ctx.Close()

	// This doesn't seem do any peers bootstrapping?
	setups[0].StartServer()

	// Stop the servers
	defer func() {
		setups.parallel(func(s TestSetup) {
			require.NoError(t, s.StopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for i, setup := range setups {
		if verifySeriesMaps(t, setup, namesp.ID(), seriesMaps) {
			fmt.Printf("SETUP #%d VERIFIED\n", i)
		} else {
			fmt.Printf("SETUP #%d VERIFICATION FAILED\n", i)
		}
	}
}
