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

	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/persist/fs"
	"github.com/m3db/m3db/sharding"
	"github.com/m3db/m3db/storage/bootstrap"
	"github.com/m3db/m3db/storage/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3db/storage/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func writeToDisk(
	writer fs.FileSetWriter,
	shardSet sharding.ShardSet,
	encoder encoding.Encoder,
	start time.Time,
	dm seriesList,
) error {
	idsPerNamespacePerShard := make(map[string]map[uint32][]series)
	for _, s := range dm {
		namespace, exists := idsPerNamespacePerShard[s.namespace]
		if !exists {
			namespace = make(map[uint32][]series)
			idsPerNamespacePerShard[s.namespace] = namespace
		}
		shard := shardSet.Shard(s.id)
		namespace[shard] = append(namespace[shard], s)
	}
	segmentHolder := make([][]byte, 2)
	for name, namespace := range idsPerNamespacePerShard {
		for shard, series := range namespace {
			if err := writer.Open(name, shard, start); err != nil {
				return err
			}
			for _, s := range series {
				encoder.Reset(start, 0)
				for _, dp := range s.data {
					if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
						return err
					}
				}
				segment := encoder.Stream().Segment()
				segmentHolder[0] = segment.Head
				segmentHolder[1] = segment.Tail
				if err := writer.WriteAll(s.id, segmentHolder); err != nil {
					return err
				}
			}
			if err := writer.Close(); err != nil {
				return err
			}
		}
	}
	return nil
}

func TestFilesystemBootstrap(t *testing.T) {
	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	fsOpts := testSetup.storageOpts.GetCommitLogOptions().GetFilesystemOptions()
	blockSize := testSetup.storageOpts.GetRetentionOptions().GetBlockSize()
	filePathPrefix := fsOpts.GetFilePathPrefix()
	testSetup.storageOpts = testSetup.storageOpts.
		RetentionOptions(testSetup.storageOpts.GetRetentionOptions().
			RetentionPeriod(2 * time.Hour)).
		NewBootstrapFn(func() bootstrap.Bootstrap {
			noOpAll := bootstrapper.NewNoOpAllBootstrapper()
			bsOpts := bootstrap.NewOptions()
			bfsOpts := bfs.NewOptions().
				BootstrapOptions(bsOpts).
				FilesystemOptions(fsOpts)
			bs := bfs.NewFileSystemBootstrapper(filePathPrefix, bfsOpts, noOpAll)
			return bootstrap.NewBootstrapProcess(bsOpts, bs)
		})

	writerBufferSize := fsOpts.GetWriterBufferSize()
	newFileMode := fsOpts.GetNewFileMode()
	newDirectoryMode := fsOpts.GetNewDirectoryMode()
	writer := fs.NewWriter(blockSize, filePathPrefix, writerBufferSize, newFileMode, newDirectoryMode)
	encoder := testSetup.storageOpts.GetEncoderPool().Get()
	dataMaps := make(map[time.Time]seriesList)

	// Write test data
	now := testSetup.getNowFn()
	inputData := []struct {
		metricNames []string
		numPoints   int
		start       time.Time
	}{
		{[]string{"foo", "bar"}, 100, now.Add(-blockSize)},
		{[]string{"foo", "baz"}, 50, now},
	}
	for _, input := range inputData {
		testData := generateTestData(testNamespaces[0], input.metricNames, input.numPoints, input.start)
		dataMaps[input.start] = testData
		require.NoError(t, writeToDisk(writer, testSetup.shardSet, encoder, input.start, testData))
	}

	// Start the server with filesystem bootstrapper
	log := testSetup.storageOpts.GetInstrumentOptions().GetLogger()
	log.Debug("filesystem bootstrap test")
	require.NoError(t, testSetup.startServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	// Verify in-memory data match what we expect
	verifyDataMaps(t, testSetup, dataMaps)
}
