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
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage/namespace"

	"github.com/stretchr/testify/require"
)

func TestDiskCleansupInactiveDirectories(t *testing.T) {
	var resetSetup *testSetup
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := newTestOptions(t)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)

	md := testSetup.namespaceMetadataOrFail(testNamespaces[0])
	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start tte server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Info("disk cleanup directories test")
	require.NoError(t, testSetup.startServer())

	// Stop the server at the end of the test

	var (
		fsCleanupErr = make(chan error)
		nsResetErr   = make(chan error)
		nsCleanupErr = make(chan error)

		fsWaitTimeout = 30 * time.Second
		nsWaitTimeout = 10 * time.Second

		namespaces = []namespace.Metadata{md}
		shardSet   = testSetup.db.ShardSet()
		shards     = shardSet.All()
		extraShard = shards[0]
	)

	// Now create some fileset files and commit logs
	shardSet, err = sharding.NewShardSet(shards[1:], shardSet.HashFn())
	require.NoError(t, err)
	testSetup.db.AssignShardSet(shardSet)

	// Check filesets are good to go
	go func() {
		fsCleanupErr <- waitUntilDataFileSetsCleanedUp(filePathPrefix,
			testSetup.db.Namespaces(), extraShard.ID(), fsWaitTimeout)
	}()
	log.Info("blocking until file cleanup is received")
	require.NoError(t, <-fsCleanupErr)

	// Server needs to restart for namespace changes to be absorbed
	go func() {
		var resetErr error
		resetSetup, resetErr = waitUntilNamespacesHaveReset(testSetup, namespaces, shardSet)
		nsResetErr <- resetErr
	}()
	defer func() {
		require.NoError(t, resetSetup.stopServer())
	}()
	nsToDelete := testNamespaces[1]
	log.Info("blocking until namespaces have reset and deleted")
	go func() {
		time.Sleep(10 * time.Second)
	}()
	require.NoError(t, <-nsResetErr)

	go func() {
		nsCleanupErr <- waitUntilNamespacesCleanedUp(filePathPrefix, nsToDelete, nsWaitTimeout)
	}()
	log.Info("blocking until the namespace cleanup is received")
	require.NoError(t, <-nsCleanupErr)
}
