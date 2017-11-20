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

	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/namespace"

	"github.com/stretchr/testify/require"
)

func TestDiskCleansupInactiveDirectories(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}
	// Test setup
	testOpts := newTestOptions(t)
	testSetup, err := newTestSetup(t, testOpts)
	require.NoError(t, err)

	md1 := testSetup.namespaceMetadataOrFail(testNamespaces[0])
	md2 := testSetup.namespaceMetadataOrFail(testNamespaces[1])
	filePathPrefix := testSetup.storageOpts.CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start tte server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	log.Info("disk cleanup directories test")
	require.NoError(t, testSetup.startServer())

	// Stop the server at the end of the test
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	var (
		fsCleanupErr = make(chan error)
		nsResetErr   = make(chan error)
		nsCleanupErr = make(chan error)

		fsWaitTimeout = 30 * time.Second
		nsWaitTimeout = 30 * time.Second

		namespaces = []namespace.Metadata{md1}
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
		fsCleanupErr <- waitUntilFilesetsCleanedUp(filePathPrefix,
			testSetup.db.Namespaces(), extraShard.ID(), fsWaitTimeout)
	}()
	log.Info("blocking until file cleanup is received")
	require.NoError(t, <-fsCleanupErr)

	// Delete the namespace we're looking for now
	_, err = testSetup.db.Truncate(md2.ID())
	require.NoError(t, err)

	// Server needs to restart for namespace changes to be absorbed
	go func() {
		nsResetErr <- waitUntilNamespacesHaveReset(testSetup, namespaces, shardSet)
	}()
	log.Info("blocking until namespaces have reset")
	require.NoError(t, <-nsResetErr)

	go func() {
		nsCleanupErr <- waitUntilNamespacesCleanedUp(testSetup, filePathPrefix, testNamespaces[1], nsWaitTimeout)
	}()
	log.Info("blocking until the namespace cleanup is received")
	require.NoError(t, <-nsCleanupErr)
}
