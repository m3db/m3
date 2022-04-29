//go:build integration
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
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io/ioutil"
	"math/rand"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/c2h5oh/datasize"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

// TestCommitLogIndexPerfSpeedBootstrap tests the performance of the commit log
// bootstrapper when a large amount of data is present that needs to be read.
// Note: Also useful for doing adhoc testing, using the environment variables
// to turn up and down the number of IDs and points.
func TestCommitLogIndexPerfSpeedBootstrap(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	// Test setup
	var (
		rOpts     = retention.NewOptions().SetRetentionPeriod(12 * time.Hour)
		blockSize = rOpts.BlockSize()
	)

	nsOpts := namespace.NewOptions().
		SetRetentionOptions(rOpts).
		SetIndexOptions(namespace.NewIndexOptions().
			SetEnabled(true).
			SetBlockSize(2 * blockSize))
	ns, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)
	opts := NewTestOptions(t).
		SetNamespaces([]namespace.Metadata{ns}).
		// Allow for wall clock timing
		SetNowFn(time.Now)

	setup, err := NewTestSetup(t, opts, nil)
	require.NoError(t, err)
	defer setup.Close()

	commitLogOpts := setup.StorageOpts().CommitLogOptions().
		SetFlushInterval(defaultIntegrationTestFlushInterval)
	setup.SetStorageOpts(setup.StorageOpts().SetCommitLogOptions(commitLogOpts))

	log := setup.StorageOpts().InstrumentOptions().Logger()
	log.Info("commit log bootstrap test")

	// Write test data
	log.Info("generating data")

	// NB(r): Use TEST_NUM_SERIES=50000 for a representative large data set to
	// test loading locally
	numSeries := 1024
	if str := os.Getenv("TEST_NUM_SERIES"); str != "" {
		numSeries, err = strconv.Atoi(str)
		require.NoError(t, err)
	}

	step := time.Second
	numPoints := 128
	if str := os.Getenv("TEST_NUM_POINTS"); str != "" {
		numPoints, err = strconv.Atoi(str)
		require.NoError(t, err)
	}

	require.True(t, (time.Duration(numPoints)*step) < blockSize,
		fmt.Sprintf("num points %d multiplied by step %s is greater than block size %s",
			numPoints, step.String(), blockSize.String()))

	numTags := 8
	if str := os.Getenv("TEST_NUM_TAGS"); str != "" {
		numTags, err = strconv.Atoi(str)
		require.NoError(t, err)
	}

	numTagSets := 128
	if str := os.Getenv("TEST_NUM_TAG_SETS"); str != "" {
		numTagSets, err = strconv.Atoi(str)
		require.NoError(t, err)
	}

	// Pre-generate tag sets, but not too many to reduce heap size.
	tagSets := make([]ident.Tags, 0, numTagSets)
	for i := 0; i < numTagSets; i++ {
		tags := ident.NewTags()
		for j := 0; j < numTags; j++ {
			tag := ident.Tag{
				Name:  ident.StringID(fmt.Sprintf("series.%d.tag.%d", i, j)),
				Value: ident.StringID(fmt.Sprintf("series.%d.tag-value.%d", i, j)),
			}
			tags.Append(tag)
		}
		tagSets = append(tagSets, tags)
	}

	log.Info("writing data")

	now := setup.NowFn()()
	blockStart := now.Add(-3 * blockSize)

	// create new commit log
	commitLog, err := commitlog.NewCommitLog(commitLogOpts)
	require.NoError(t, err)
	require.NoError(t, commitLog.Open())

	ctx := context.NewBackground()
	defer ctx.Close()

	shardSet := setup.ShardSet()
	idPrefix := "test.id.test.id.test.id.test.id.test.id.test.id.test.id.test.id"
	idPrefixBytes := []byte(idPrefix)
	numBytes := make([]byte, 8)
	numHexBytes := make([]byte, hex.EncodedLen(len(numBytes)))
	tagEncoderPool := commitLogOpts.FilesystemOptions().TagEncoderPool()
	tagSliceIter := ident.NewTagsIterator(ident.Tags{})
	for i := 0; i < numPoints; i++ {
		for j := 0; j < numSeries; j++ {
			checkedBytes := checked.NewBytes(nil, nil)
			seriesID := ident.BinaryID(checkedBytes)

			// Write the ID prefix
			checkedBytes.Resize(0)
			checkedBytes.AppendAll(idPrefixBytes)

			// Write out the binary representation then hex encode the
			// that into the ID to give it a unique ID for this series number
			binary.LittleEndian.PutUint64(numBytes, uint64(j))
			hex.Encode(numHexBytes, numBytes)
			checkedBytes.AppendAll(numHexBytes)

			// Use the tag sets appropriate for this series number
			seriesTags := tagSets[j%len(tagSets)]

			tagSliceIter.Reset(seriesTags)
			tagEncoder := tagEncoderPool.Get()
			err := tagEncoder.Encode(tagSliceIter)
			require.NoError(t, err)

			encodedTagsChecked, ok := tagEncoder.Data()
			require.True(t, ok)

			series := ts.Series{
				Namespace:   ns.ID(),
				Shard:       shardSet.Lookup(seriesID),
				ID:          seriesID,
				EncodedTags: ts.EncodedTags(encodedTagsChecked.Bytes()),
				UniqueIndex: uint64(j),
			}
			dp := ts.Datapoint{
				TimestampNanos: blockStart.Add(time.Duration(i) * step),
				Value:          rand.Float64(), //nolint: gosec
			}
			require.NoError(t, commitLog.Write(ctx, series, dp, xtime.Second, nil))
		}
	}

	// ensure writes finished
	require.NoError(t, commitLog.Close())

	log.Info("finished writing data")

	// emit how big commit logs are
	commitLogsDirPath := fs.CommitLogsDirPath(commitLogOpts.FilesystemOptions().FilePathPrefix())
	files, err := ioutil.ReadDir(commitLogsDirPath)
	require.NoError(t, err)

	log.Info("test wrote commit logs", zap.Int("numFiles", len(files)))
	for _, file := range files {
		log.Info("test wrote commit logs",
			zap.String("file", file.Name()),
			zap.String("size", datasize.ByteSize(file.Size()).HR()))
	}

	// Setup bootstrapper after writing data so filesystem inspection can find it.
	setupCommitLogBootstrapperWithFSInspection(t, setup, commitLogOpts)

	// restore now time so measurements take effect
	setup.SetStorageOpts(setup.StorageOpts().SetClockOptions(clock.NewOptions()))

	// Start the server with filesystem bootstrapper
	require.NoError(t, setup.StartServer())
	log.Debug("server is now up")

	// Stop the server
	defer func() {
		require.NoError(t, setup.StopServer())
		log.Debug("server is now down")
	}()
}
