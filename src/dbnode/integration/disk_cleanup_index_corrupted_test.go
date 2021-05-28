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
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/retention"
	xclock "github.com/m3db/m3/src/x/clock"
)

func TestDiskCleanupIndexCorrupted(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(48 * time.Hour)
		nsBlockSize  = time.Hour
		idxBlockSize = 2 * time.Hour

		nsROpts = rOpts.SetBlockSize(nsBlockSize)
		idxOpts = namespace.NewIndexOptions().SetBlockSize(idxBlockSize).SetEnabled(true)
		nsOpts  = namespace.NewOptions().
			SetCleanupEnabled(true).
			SetRetentionOptions(nsROpts).
			SetIndexOptions(idxOpts)
	)

	ns, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns})
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	filePathPrefix := setup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Now create some fileset files
	var (
		filesetsIdentifiers = make([]fs.FileSetFileIdentifier, 0)
		numVolumes          = 4

		now         = setup.NowFn()().Truncate(idxBlockSize)
		blockStarts = []time.Time{
			now.Add(-3 * idxBlockSize),
			now.Add(-2 * idxBlockSize),
			now.Add(-1 * idxBlockSize),
		}
	)
	for _, blockStart := range blockStarts {
		for idx := 0; idx < numVolumes; idx++ {
			filesetsIdentifiers = append(filesetsIdentifiers, fs.FileSetFileIdentifier{
				Namespace:   ns.ID(),
				BlockStart:  blockStart,
				VolumeIndex: idx,
			})
		}
	}
	writeIndexFileSetFiles(t, setup.StorageOpts(), ns, filesetsIdentifiers)

	filesets := fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
		FilePathPrefix:   filePathPrefix,
		Namespace:        ns.ID(),
		ReaderBufferSize: setup.FilesystemOpts().InfoReaderBufferSize(),
	})
	require.Len(t, filesets, len(blockStarts)*numVolumes)

	filesThatShouldBeKept := make([]string, 0)
	keep := func(files []string) {
		filesThatShouldBeKept = append(filesThatShouldBeKept, files...)
	}
	// Corrupt some filesets.
	forBlockStart(blockStarts[0], filesets, func(filesets []fs.ReadIndexInfoFileResult) {
		keep(missingDigest(t, filesets[0])) // most recent volume index for volume type
		corruptedInfo(t, filesets[1])
		missingInfo(t, filesets[2])
		keep(corruptedInfo(t, filesets[3])) // corrupted info files are kept if it's the most recent volume index
	})

	forBlockStart(blockStarts[1], filesets, func(filesets []fs.ReadIndexInfoFileResult) {
		keep(filesets[0].AbsoluteFilePaths)
		missingDigest(t, filesets[1])
		missingDigest(t, filesets[2])
		keep(missingDigest(t, filesets[3])) // most recent volume index for volume type
	})

	forBlockStart(blockStarts[2], filesets, func(filesets []fs.ReadIndexInfoFileResult) {
		missingInfo(t, filesets[0])
		corruptedInfo(t, filesets[1])
		missingDigest(t, filesets[2])
		keep(filesets[3].AbsoluteFilePaths) // most recent volume index for volume type
	})
	sort.Strings(filesThatShouldBeKept)

	// Start the server
	log := setup.StorageOpts().InstrumentOptions().Logger()
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()

	// Check if corrupted files have been deleted
	waitTimeout := 30 * time.Second
	deleted := xclock.WaitUntil(func() bool {
		files, err := fs.IndexFileSetsBefore(filePathPrefix, ns.ID(), now.Add(time.Minute))
		require.NoError(t, err)
		sort.Strings(files)
		return reflect.DeepEqual(files, filesThatShouldBeKept)
	}, waitTimeout)
	if !assert.True(t, deleted) {
		files, err := fs.IndexFileSetsBefore(filePathPrefix, ns.ID(), now.Add(time.Minute))
		require.NoError(t, err)
		sort.Strings(files)
		require.Equal(t, filesThatShouldBeKept, files)
	}
}

func forBlockStart(
	blockStart time.Time,
	filesets []fs.ReadIndexInfoFileResult,
	fn func([]fs.ReadIndexInfoFileResult),
) {
	res := make([]fs.ReadIndexInfoFileResult, 0)
	for _, f := range filesets {
		if f.ID.BlockStart.Equal(blockStart) {
			res = append(res, f)
		}
	}
	sort.Slice(res, func(i, j int) bool {
		return res[i].ID.VolumeIndex < res[j].ID.VolumeIndex
	})
	fn(res)
}

func corruptedInfo(t *testing.T, fileset fs.ReadIndexInfoFileResult) []string {
	for _, f := range fileset.AbsoluteFilePaths {
		if strings.Contains(f, "info.db") {
			require.NoError(t, os.Truncate(f, 0))
			return fileset.AbsoluteFilePaths
		}
	}
	require.Fail(t, "could not find info file")
	return nil
}

func missingInfo(t *testing.T, fileset fs.ReadIndexInfoFileResult) []string { //nolint:unparam
	for i, f := range fileset.AbsoluteFilePaths {
		if strings.Contains(f, "info.db") {
			require.NoError(t, os.Remove(f))
			res := make([]string, 0)
			res = append(res, fileset.AbsoluteFilePaths[:i]...)
			res = append(res, fileset.AbsoluteFilePaths[i+1:]...)
			return res
		}
	}
	require.Fail(t, "could not find info file")
	return nil
}

func missingDigest(t *testing.T, fileset fs.ReadIndexInfoFileResult) []string {
	for i, f := range fileset.AbsoluteFilePaths {
		if strings.Contains(f, "digest.db") {
			require.NoError(t, os.Remove(f))
			res := make([]string, 0)
			res = append(res, fileset.AbsoluteFilePaths[:i]...)
			res = append(res, fileset.AbsoluteFilePaths[i+1:]...)
			return res
		}
	}
	require.Fail(t, "could not find digest file")
	return nil
}
