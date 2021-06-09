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

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/sharding"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/require"
)

var errDataCleanupTimedOut = errors.New("cleaning up data files took too long")

func newDataFileSetWriter(storageOpts storage.Options) (fs.DataFileSetWriter, error) {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	return fs.NewWriter(fsOpts)
}

func writeDataFileSetFiles(
	t *testing.T, storageOpts storage.Options, md namespace.Metadata,
	shard uint32, fileTimes []xtime.UnixNano,
) {
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

func writeIndexFileSetFiles(t *testing.T, storageOpts storage.Options, md namespace.Metadata,
	filesets []fs.FileSetFileIdentifier) {
	blockSize := md.Options().IndexOptions().BlockSize()
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	writer, err := fs.NewIndexWriter(fsOpts)
	require.NoError(t, err)
	for _, fileset := range filesets {
		writerOpts := fs.IndexWriterOpenOptions{
			Identifier: fileset,
			BlockSize:  blockSize,
			Shards:     map[uint32]struct{}{0: {}},
		}
		require.NoError(t, writer.Open(writerOpts))
		require.NoError(t, writer.Close())
	}
}

type cleanupTimesCommitLog struct {
	clOpts  commitlog.Options
	indices []int64
}

func (c *cleanupTimesCommitLog) anyExist() bool {
	commitlogs, _, err := commitlog.Files(c.clOpts)
	if err != nil {
		panic(err)
	}

	for _, cl := range commitlogs {
		for _, index := range c.indices {
			if cl.Index == index {
				return true
			}
		}
	}

	return false
}

type cleanupTimesFileSet struct {
	filePathPrefix string
	namespace      ident.ID
	shard          uint32
	times          []xtime.UnixNano
}

func (fset *cleanupTimesFileSet) anyExist() bool {
	for _, t := range fset.times {
		exists, err := fs.DataFileSetExists(fset.filePathPrefix, fset.namespace, fset.shard, t, 0)
		if err != nil {
			panic(err)
		}

		if exists {
			return true
		}
	}
	return false
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
func waitUntilNamespacesHaveReset(testSetup TestSetup, newNamespaces []namespace.Metadata, newShardSet sharding.ShardSet) (TestSetup, error) {
	err := testSetup.StopServer()
	if err != nil {
		return nil, err
	}
	// Reset to the desired shard set and namespaces
	// Because restarting the server would bootstrap
	// To old data we wanted to delete
	testSetup.SetOpts(testSetup.Opts().SetNamespaces(newNamespaces))

	resetSetup, err := NewTestSetup(nil, testSetup.Opts(), testSetup.FilesystemOpts())
	if err != nil {
		return nil, err
	}
	resetSetup.SetShardSet(newShardSet)
	err = resetSetup.StartServer()
	if err != nil {
		return nil, err
	}

	return resetSetup, nil
}

// nolint: unused
func waitUntilDataFileSetsCleanedUp(clOpts commitlog.Options, namespaces []storage.Namespace, extraShard uint32, waitTimeout time.Duration) error {
	filePathPrefix := clOpts.FilesystemOptions().FilePathPrefix()
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

func waitUntilDataCleanedUp(
	clOpts commitlog.Options,
	namespace ident.ID,
	shard uint32,
	toDelete xtime.UnixNano,
	timeout time.Duration,
) error {
	filePathPrefix := clOpts.FilesystemOptions().FilePathPrefix()
	return waitUntilDataCleanedUpExtended(
		[]cleanupTimesFileSet{
			{
				filePathPrefix: filePathPrefix,
				namespace:      namespace,
				shard:          shard,
				times:          []xtime.UnixNano{toDelete},
			},
		},
		cleanupTimesCommitLog{
			clOpts:  clOpts,
			indices: []int64{},
		},
		timeout)
}
