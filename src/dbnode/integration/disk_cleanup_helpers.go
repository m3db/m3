// +build integration

// Copyright (c) 2017 Uber Technologies, Inc.
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

	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/namespace"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

var (
	errDataCleanupTimedOut = errors.New("cleaning up data files took too long")
)

func newDataFileSetWriter(storageOpts storage.Options) (fs.DataFileSetWriter, error) {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	return fs.NewWriter(fsOpts)
}

func writeDataFileSetFiles(t *testing.T, storageOpts storage.Options, md namespace.Metadata, shard uint32, fileTimes []time.Time) {
	rOpts := md.Options().RetentionOptions()
	writer, err := newDataFileSetWriter(storageOpts)
	require.NoError(t, err)
	for _, start := range fileTimes {
		writerOpts := fs.DataWriterOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  md.ID(),
				Shard:      shard,
				BlockStart: start,
			},
			BlockSize: rOpts.BlockSize(),
		}
		require.NoError(t, writer.Open(writerOpts))
		require.NoError(t, writer.Close())
	}
}

func writeIndexFileSetFiles(t *testing.T, storageOpts storage.Options, md namespace.Metadata, fileTimes []time.Time) {
	blockSize := md.Options().IndexOptions().BlockSize()
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	writer, err := fs.NewIndexWriter(fsOpts)
	require.NoError(t, err)
	for _, start := range fileTimes {
		writerOpts := fs.IndexWriterOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  md.ID(),
				BlockStart: start,
			},
			BlockSize: blockSize,
			Shards:    map[uint32]struct{}{0: struct{}{}},
		}
		require.NoError(t, writer.Open(writerOpts))
		require.NoError(t, writer.Close())
	}
}

// TODO: Fix this, should be based on indexes.
type cleanupTimesCommitLog struct {
	filePathPrefix string
	times          []time.Time
}

func (c *cleanupTimesCommitLog) anyExist() bool {
	for _, t := range c.times {
		_, index, err := commitlog.NextFile(c.filePathPrefix)
		if err != nil {
			panic(err)
		}
		if index != 0 {
			return true
		}
	}
	return false
}

func (c *cleanupTimesCommitLog) allExist() bool {
	for _, t := range c.times {
		_, index, err := commitlog.NextFile(c.filePathPrefix)
		if err != nil {
			panic(err)
		}
		if index == 0 {
			return false
		}
	}
	return true
}

type cleanupTimesFileSet struct {
	filePathPrefix string
	namespace      ident.ID
	shard          uint32
	times          []time.Time
}

func (fset *cleanupTimesFileSet) anyExist() bool {
	for _, t := range fset.times {
		exists, err := fs.DataFileSetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t)
		if err != nil {
			panic(err)
		}

		if exists {
			return true
		}
	}
	return false
}

func (fset *cleanupTimesFileSet) allExist() bool {
	for _, t := range fset.times {
		exists, err := fs.DataFileSetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t)
		if err != nil {
			panic(err)
		}

		if !exists {
			return false
		}
	}

	return true
}

func waitUntilDataCleanedUpExtended(
	filesetFiles []cleanupTimesFileSet,
	commitlog cleanupTimesCommitLog,
	timeout time.Duration,
) error {
	dataCleanedUp := func() bool {
		if commitlog.anyExist() {
			return false
		}

		for _, fset := range filesetFiles {
			if fset.anyExist() {
				return false
			}
		}

		return true
	}

	if waitUntil(dataCleanedUp, timeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

// nolint: unused
func waitUntilNamespacesCleanedUp(filePathPrefix string, namespace ident.ID, waitTimeout time.Duration) error {
	dataCleanedUp := func() bool {
		namespaceDir := fs.NamespaceDataDirPath(filePathPrefix, namespace)
		exists, err := fs.FileExists(namespaceDir)
		if err != nil {
			panic(err)
		}
		return !exists
	}

	if waitUntil(dataCleanedUp, waitTimeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

// nolint: unused
func waitUntilNamespacesHaveReset(testSetup *testSetup, newNamespaces []namespace.Metadata, newShardSet sharding.ShardSet) (*testSetup, error) {
	err := testSetup.stopServer()
	if err != nil {
		return nil, err
	}
	// Reset to the desired shard set and namespaces
	// Because restarting the server would bootstrap
	// To old data we wanted to delete
	testSetup.opts = testSetup.opts.SetNamespaces(newNamespaces)

	resetSetup, err := newTestSetup(nil, testSetup.opts, testSetup.fsOpts)
	if err != nil {
		return nil, err
	}
	resetSetup.shardSet = newShardSet
	err = resetSetup.startServer()
	if err != nil {
		return nil, err
	}

	return resetSetup, nil
}

// nolint: unused
func waitUntilDataFileSetsCleanedUp(filePathPrefix string, namespaces []storage.Namespace, extraShard uint32, waitTimeout time.Duration) error {
	dataCleanedUp := func() bool {
		for _, n := range namespaces {
			shardDir := fs.ShardDataDirPath(filePathPrefix, n.ID(), extraShard)
			exists, err := fs.FileExists(shardDir)
			if err != nil {
				panic(err)
			}
			if exists {
				return false
			}
		}
		return true
	}

	if waitUntil(dataCleanedUp, waitTimeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

func waitUntilDataCleanedUp(filePathPrefix string, namespace ident.ID, shard uint32, toDelete time.Time, timeout time.Duration) error {
	return waitUntilDataCleanedUpExtended(
		[]cleanupTimesFileSet{
			cleanupTimesFileSet{
				filePathPrefix: filePathPrefix,
				namespace:      namespace,
				shard:          shard,
				times:          []time.Time{toDelete},
			},
		},
		cleanupTimesCommitLog{
			filePathPrefix: filePathPrefix,
			times:          []time.Time{toDelete},
		},
		timeout)
}

func getTimes(start time.Time, end time.Time, intervalSize time.Duration) []time.Time {
	totalPeriod := end.Sub(start)
	numPeriods := int(totalPeriod / intervalSize)

	times := make([]time.Time, 0, numPeriods)
	for i := 0; i < numPeriods; i++ {
		times = append(times, start.Add(time.Duration(i)*intervalSize))
	}

	return times
}
