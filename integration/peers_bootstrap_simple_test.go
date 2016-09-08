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
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper/peers"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/log"

	"github.com/stretchr/testify/require"
)

func TestPeersBootstrapSimple(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setups
	var (
		replicas   = 2
		namesp     = namespace.NewMetadata(testNamespaces[0], namespace.NewOptions())
		log        = xlog.NewLogger(os.Stdout)
		setups     testSetups
		cleanupFns []func()
	)
	defer func() {
		for _, fn := range cleanupFns {
			fn()
		}
	}()
	opts := newTestOptions().
		SetNamespaces([]namespace.Metadata{namesp})
	retentionOpts := retention.NewOptions().
		SetRetentionPeriod(6 * time.Hour).
		// Set short buffer future so peers bootstrapper doesn't wait a long time
		SetBufferFuture(time.Second)
	for i := 0; i < replicas; i++ {
		var (
			instance     = i
			isLast       = instance == replicas-1
			instanceOpts = multiAddrTestOptions(opts, instance)
			setup        *testSetup
		)
		setup = newBootstrappableTestSetup(t, instanceOpts, retentionOpts, func() bootstrap.Bootstrap {
			instrumentOpts := setup.storageOpts.InstrumentOptions()

			bsOpts := bootstrap.NewOptions().
				SetRetentionOptions(setup.storageOpts.RetentionOptions()).
				SetInstrumentOptions(instrumentOpts)

			noOpAll := bootstrapper.NewNoOpAllBootstrapper()

			session := newMultiAddrAdminSession(t, instrumentOpts, setup.shardSet, replicas, instance)
			peersOpts := peers.NewOptions().
				SetBootstrapOptions(bsOpts).
				SetAdminSession(session)
			peersBootstrapper := peers.NewPeersBootstrapper(peersOpts, noOpAll)

			cleanupFns = append(cleanupFns, func() {
				session.Close()
			})

			fsOpts := setup.storageOpts.CommitLogOptions().FilesystemOptions()
			filePathPrefix := fsOpts.FilePathPrefix()

			bfsOpts := fs.NewOptions().
				SetBootstrapOptions(bsOpts).
				SetFilesystemOptions(fsOpts)

			var fsBootstrapper bootstrap.Bootstrapper
			if isLast {
				fsBootstrapper = fs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, peersBootstrapper)
			} else {
				fsBootstrapper = fs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, noOpAll)
			}

			return bootstrap.NewBootstrapProcess(bsOpts, fsBootstrapper)
		})
		logger := setup.storageOpts.InstrumentOptions().Logger()
		logger = logger.WithFields(xlog.NewLogField("instance", i))
		iopts := setup.storageOpts.InstrumentOptions().SetLogger(logger)
		setup.storageOpts = setup.storageOpts.SetInstrumentOptions(iopts)

		setups = append(setups, setup)
		cleanupFns = append(cleanupFns, func() {
			setup.close()
		})
	}

	// Write test data for first node
	now := setups[0].getNowFn()
	blockSize := setups[0].storageOpts.RetentionOptions().BlockSize()
	seriesMaps, err := writeTestDataToDisk(namesp.Name(), setups[0], []testData{
		{ids: []string{"foo", "bar"}, numPoints: 100, start: now.Add(-blockSize)},
		{ids: []string{"foo"}, numPoints: 50, start: now},
	})
	require.NoError(t, err)

	// Start the first server with filesystem bootstrapper
	require.NoError(t, setups[0].startServer())

	// Start the last server with peers and filesystem bootstrappers
	require.NoError(t, setups[1].startServer())
	log.Debug("servers are now up")

	// Stop the server
	defer func() {
		setups.forEachParallel(func(s *testSetup) {
			require.NoError(t, s.stopServer())
		})
		log.Debug("servers are now down")
	}()

	// Verify in-memory data match what we expect
	for _, setup := range setups {
		verifySeriesMaps(t, setup, namesp.Name(), seriesMaps)
	}
}
