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
	"io/ioutil"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

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

	// Test setup
	var (
		rOpts        = retention.NewOptions().SetRetentionPeriod(48 * time.Hour)
		nsBlockSize  = time.Hour
		idxBlockSize = 2 * time.Hour
		nsROpts      = rOpts.SetBlockSize(nsBlockSize)
	)

	ns, err := namespace.NewMetadata(testNamespaces[0], namespace.NewOptions().
		SetCleanupEnabled(true).
		SetRetentionOptions(nsROpts).SetIndexOptions(
		namespace.NewIndexOptions().SetBlockSize(idxBlockSize).SetEnabled(true)))
	require.NoError(t, err)

	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns})

	// Test setup
	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	filePathPrefix := setup.StorageOpts().CommitLogOptions().FilesystemOptions().FilePathPrefix()

	// Now create some fileset files
	var (
		filesetsIdentifiers = make([]fs.FileSetFileIdentifier, 0)
		maxVolumeIndex      = 3

		now         = setup.NowFn()().Truncate(idxBlockSize)
		blockStarts = []time.Time{
			now.Add(-3 * idxBlockSize),
			now.Add(-2 * idxBlockSize),
			now.Add(-1 * idxBlockSize),
		}
	)
	for _, blockStart := range blockStarts {
		for idx := 0; idx <= maxVolumeIndex; idx++ {
			filesetsIdentifiers = append(filesetsIdentifiers, fs.FileSetFileIdentifier{
				Namespace:   ns.ID(),
				BlockStart:  blockStart,
				VolumeIndex: idx,
			})
		}
	}
	writeIndexFileSetFiles(t, setup.StorageOpts(), ns, filesetsIdentifiers)

	// Corrupt some of the written filesets.
	deltaNow := now.Add(time.Minute)
	filesets := fs.ReadIndexInfoFiles(fs.ReadIndexInfoFilesOptions{
		FilePathPrefix:   filePathPrefix,
		Namespace:        ns.ID(),
		ReaderBufferSize: setup.FilesystemOpts().InfoReaderBufferSize(),
	})
	require.NoError(t, err)
	require.NotEmpty(t, filesets)

	var (
		filesThatShouldBeKept = make([]string, 0)
		filesDeletedByTest    = make(map[string]struct{})
	)
	// Corrupt some filesets.
	for _, fileset := range filesets {
		shouldKeep := false
		switch {
		case fileset.ID.BlockStart.Equal(blockStarts[0]) && fileset.ID.VolumeIndex != maxVolumeIndex/2:
			// Corrupt non-info file. Keep the most recent volume index non-corrupted to allow
			// cleanup of corrupted volumes.
			for _, file := range fileset.AbsoluteFilePaths {
				if strings.Contains(file, "digest.db") {
					require.NoError(t, os.Remove(file))
					filesDeletedByTest[file] = struct{}{}
				}
			}
			if fileset.ID.VolumeIndex == maxVolumeIndex {
				// This volume is chosen as the most recent for the volume type.
				shouldKeep = true
			}

		case fileset.ID.BlockStart.Equal(blockStarts[1]) && fileset.ID.VolumeIndex > 0:
			// Overwrite info file to simulate incomplete write.
			for _, file := range fileset.AbsoluteFilePaths {
				if strings.Contains(file, "info.db") {
					require.NoError(t, ioutil.WriteFile(file, []byte{}, setup.FilesystemOpts().NewFileMode()))
				}
			}
			if fileset.ID.VolumeIndex == maxVolumeIndex {
				// This volume is the most recent and it also has incomplete info file, so it is kept.
				shouldKeep = true
			}

		case fileset.ID.BlockStart.Equal(blockStarts[2]) && fileset.ID.VolumeIndex < maxVolumeIndex:
			// Delete info file to simulate corruptions from M3DB versions where info file
			// had not been written first.
			for _, file := range fileset.AbsoluteFilePaths {
				if strings.Contains(file, "info.db") {
					require.NoError(t, os.Remove(file))
					filesDeletedByTest[file] = struct{}{}
				}
			}
		default:
			shouldKeep = true
		}

		if shouldKeep {
			for _, file := range fileset.AbsoluteFilePaths {
				if _, ok := filesDeletedByTest[file]; !ok {
					filesThatShouldBeKept = append(filesThatShouldBeKept, file)
				}
			}
		}
	}
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
		files, err := fs.IndexFileSetsBefore(filePathPrefix, ns.ID(), deltaNow)
		require.NoError(t, err)
		sort.Strings(files)
		return reflect.DeepEqual(files, filesThatShouldBeKept)
	}, waitTimeout)
	require.True(t, deleted)
}
