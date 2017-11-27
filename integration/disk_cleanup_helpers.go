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
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"

	"github.com/stretchr/testify/require"
)

var (
	errDataCleanupTimedOut = errors.New("cleaning up data files took too long")
)

// nolint: deadcode, unused
func newNamespaceDir(storageOpts storage.Options, md namespace.Metadata) string {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	filePathPrefix := fsOpts.FilePathPrefix()
	return fs.NamespaceDirPath(filePathPrefix, md.ID())
}

// nolint: deadcode
func newFilesetWriter(storageOpts storage.Options) fs.FileSetWriter {
	fsOpts := storageOpts.CommitLogOptions().FilesystemOptions()
	filePathPrefix := fsOpts.FilePathPrefix()
	writerBufferSize := fsOpts.WriterBufferSize()
	newFileMode := fsOpts.NewFileMode()
	newDirectoryMode := fsOpts.NewDirectoryMode()
	return fs.NewWriter(filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
}

// nolint: deadcode
func writeFilesetFiles(t *testing.T, storageOpts storage.Options, md namespace.Metadata, shard uint32, fileTimes []time.Time) {
	rOpts := md.Options().RetentionOptions()
	writer := newFilesetWriter(storageOpts)
	for _, start := range fileTimes {
		require.NoError(t, writer.Open(md.ID(), rOpts.BlockSize(), shard, start))
		require.NoError(t, writer.Close())
	}
}

// nolint: deadcode
func writeCommitLogs(t *testing.T, filePathPrefix string, fileTimes []time.Time) {
	for _, start := range fileTimes {
		commitLogFile, _ := fs.NextCommitLogsFile(filePathPrefix, start)
		_, err := os.Create(commitLogFile)
		require.NoError(t, err)
	}
}

type cleanupTimesCommitLog struct {
	filePathPrefix string
	times          []time.Time
}

func (c *cleanupTimesCommitLog) anyExist() bool {
	for _, t := range c.times {
		_, index := fs.NextCommitLogsFile(c.filePathPrefix, t)
		if index != 0 {
			return true
		}
	}
	return false
}

func (c *cleanupTimesCommitLog) allExist() bool {
	for _, t := range c.times {
		_, index := fs.NextCommitLogsFile(c.filePathPrefix, t)
		if index == 0 {
			return false
		}
	}
	return true
}

type cleanupTimesFileset struct {
	filePathPrefix string
	namespace      ts.ID
	shard          uint32
	times          []time.Time
}

func (fset *cleanupTimesFileset) anyExist() bool {
	for _, t := range fset.times {
		if fs.FilesetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t) {
			return true
		}
	}
	return false
}

func (fset *cleanupTimesFileset) allExist() bool {
	for _, t := range fset.times {
		if !fs.FilesetExistsAt(fset.filePathPrefix, fset.namespace, fset.shard, t) {
			return false
		}
	}
	return true
}

func waitUntilDataCleanedUpExtended(
	filesetFiles []cleanupTimesFileset,
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

// nolint: deadcode, unused
func waitUntilNamespacesCleanedUp(filePathPrefix string, namespace ts.ID, waitTimeout time.Duration) error {
	nsDir := fs.NamespaceDirPath(filePathPrefix, namespace)
	fmt.Println("Loooking for namespace to be delete:", nsDir)
	dataCleanedUp := func() bool {
		namespaceDir := fs.NamespaceDirPath(filePathPrefix, namespace)
		fmt.Println("found", namespaceDir)
		return !fs.FileExists(namespaceDir)
	}

	if waitUntil(dataCleanedUp, waitTimeout) {
		return nil
	}
	return errDataCleanupTimedOut
}

// nolint: deadcode, unused
func waitUntilNamespacesHaveReset(testSetup *testSetup, newNamespaces []namespace.Metadata, newShardSet sharding.ShardSet) (*testSetup, error) {
	testSetup.stopServer()
	//	testSetup.waitUntilServerIsDown()
	// Reset to the desired shard set and namespaces
	// Because restarting the server would bootstrap
	// To old data we wanted to delete
	fmt.Println("resetting the namespaces to the following new namespaces")
	for _, md := range newNamespaces {
		fmt.Println("new ns:", md.ID().String())
	}
	testSetup.opts = testSetup.opts.SetNamespaces(newNamespaces)
	testSetupNamespaces := testSetup.opts.Namespaces()
	for _, md := range testSetupNamespaces {
		fmt.Println("changed to new ns:", md.ID().String())
	}

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

// nolint: deadcode, unused
func waitUntilFilesetsCleanedUp(filePathPrefix string, namespaces []storage.Namespace, extraShard uint32, waitTimeout time.Duration) error {
	dataCleanedUp := func() bool {
		for _, n := range namespaces {
			shardDir := fs.ShardDirPath(filePathPrefix, n.ID(), extraShard)
			if fs.FileExists(shardDir) {
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

// nolint: deadcode
func waitUntilDataCleanedUp(filePathPrefix string, namespace ts.ID, shard uint32, toDelete time.Time, timeout time.Duration) error {
	return waitUntilDataCleanedUpExtended(
		[]cleanupTimesFileset{
			cleanupTimesFileset{
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

// nolint: deadcode
func getTimes(start time.Time, end time.Time, intervalSize time.Duration) []time.Time {
	totalPeriod := end.Sub(start)
	numPeriods := int(totalPeriod / intervalSize)

	times := make([]time.Time, 0, numPeriods)
	for i := 0; i < numPeriods; i++ {
		times = append(times, start.Add(time.Duration(i)*intervalSize))
	}

	return times
}
