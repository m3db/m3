// +build integration

// Copyright (c) 2018 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	xclock "github.com/m3db/m3/src/x/clock"

	"github.com/stretchr/testify/require"
)

func TestDiskCleanupIndex(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(48 * time.Hour)
		nsBlockSize  = time.Hour
		idxBlockSize = 2 * time.Hour
		nsROpts      = rOpts.SetBlockSize(nsBlockSize)
	)

	md, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().
		SetCleanupEnabled(true).
		SetRetentionOptions(nsROpts).SetIndexOptions(
		namespace.NewIndexOptions().SetBlockSize(idxBlockSize).SetEnabled(true)))
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{md})

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	retentionPeriod := md.Options().RetentionOptions().RetentionPeriod()
	filePathPrefix := setup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Start the server
	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Debug("disk index cleanup test")
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Now create some fileset files
	numTimes := 10
	filesetsIdentifiers := make([]fs.FileSetFileIdentifier, numTimes)
	now := setup.NowFn()().Truncate(idxBlockSize)
	for i := 0; i < numTimes; i++ {
		filesetsIdentifiers[i] = fs.FileSetFileIdentifier{
			Namespace:  md.ID(),
			BlockStart: now.Add(time.Duration(i) * idxBlockSize),
		}
	}
	writeIndexFileSetFiles(t, setup.StorageOpts(), md, filesetsIdentifiers)

	deltaNow := now.Add(time.Minute)
	filesets, err := fs.IndexFileSetsBefore(filePathPrefix, md.ID(), deltaNow)
	require.NoError(t, err)
	require.NotEmpty(t, filesets)

	// Move now forward by retentionPeriod + blockSize so fileset files at now will be deleted
	newNow := now.Add(retentionPeriod).Add(idxBlockSize)
	setup.SetNowFn(newNow)

	// Check if files have been deleted
	waitTimeout := 30 * time.Second
	deleted := xclock.WaitUntil(func() bool {
		filesets, err := fs.IndexFileSetsBefore(filePathPrefix, md.ID(), deltaNow)
		require.NoError(t, err)
		return len(filesets) == 0
	}, waitTimeout)
	require.True(t, deleted)
}
