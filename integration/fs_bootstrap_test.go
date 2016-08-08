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

	"github.com/m3db/m3db/bootstrap"
	"github.com/m3db/m3db/bootstrap/bootstrapper"
	bfs "github.com/m3db/m3db/bootstrap/bootstrapper/fs"
	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func writeToDisk(
	writer m3db.FileSetWriter,
	shardingScheme m3db.ShardScheme,
	encoder m3db.Encoder,
	start time.Time,
	dm dataMap,
) error {
	idsPerShard := make(map[uint32][]string)
	for id := range dm {
		shard := shardingScheme.Shard(id)
		idsPerShard[shard] = append(idsPerShard[shard], id)
	}
	segmentHolder := make([][]byte, 2)
	for shard, ids := range idsPerShard {
		if err := writer.Open(shard, start); err != nil {
			return err
		}
		for _, id := range ids {
			encoder.Reset(start, 0)
			for _, dp := range dm[id] {
				if err := encoder.Encode(dp, xtime.Second, nil); err != nil {
					return err
				}
			}
			segment := encoder.Stream().Segment()
			segmentHolder[0] = segment.Head
			segmentHolder[1] = segment.Tail
			if err := writer.WriteAll(id, segmentHolder); err != nil {
				return err
			}
		}
		if err := writer.Close(); err != nil {
			return err
		}
	}
	return nil
}

func TestFilesystemBootstrap(t *testing.T) {
	// Test setup
	testSetup, err := newTestSetup(newTestOptions())
	require.NoError(t, err)
	defer testSetup.close()

	blockSize := testSetup.dbOpts.GetBlockSize()
	filePathPrefix := testSetup.dbOpts.GetFilePathPrefix()
	testSetup.dbOpts = testSetup.dbOpts.RetentionPeriod(2 * time.Hour).NewBootstrapFn(func() m3db.Bootstrap {
		noOpAll := bootstrapper.NewNoOpAllBootstrapper()
		bs := bfs.NewFileSystemBootstrapper(filePathPrefix, testSetup.dbOpts, noOpAll)
		bsOpts := bootstrap.NewOptions()
		return bootstrap.NewBootstrapProcess(bsOpts, testSetup.dbOpts, bs)
	})

	writerFn := testSetup.dbOpts.GetNewFileSetWriterFn()
	writer := writerFn(blockSize, filePathPrefix, testSetup.dbOpts.GetWriterBufferSize(), testSetup.dbOpts.GetFileWriterOptions())
	encoder := testSetup.dbOpts.GetEncoderPool().Get()
	dataMaps := make(map[time.Time]dataMap)

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
		testData := generateTestData(input.metricNames, input.numPoints, input.start)
		dataMaps[input.start] = testData
		require.NoError(t, writeToDisk(writer, testSetup.shardingScheme, encoder, input.start, testData))
	}

	// Start the server with filesystem bootstrapper
	log := testSetup.dbOpts.GetLogger()
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
