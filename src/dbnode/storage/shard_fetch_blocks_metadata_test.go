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

package storage

import (
	"crypto/rand"
	"fmt"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/digest"
	"github.com/m3db/m3/src/dbnode/generated/proto/pagetoken"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/ident"
	xtest "github.com/m3db/m3x/test"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardFetchBlocksMetadata(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions()
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	start := time.Now()
	end := start.Add(defaultTestRetentionOpts.BlockSize())

	var ids []ident.ID
	fetchOpts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     true,
		IncludeChecksums: true,
		IncludeLastRead:  true,
	}
	seriesFetchOpts := series.FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: fetchOpts,
		IncludeCachedBlocks:        true,
	}
	lastRead := time.Now().Add(-time.Minute)
	for i := 0; i < 10; i++ {
		id := ident.StringID(fmt.Sprintf("foo.%d", i))
		tags := ident.NewTags(
			ident.StringTag("aaa", "bbb"),
			ident.StringTag("ccc", "ddd"),
		)
		tagsIter := ident.NewTagsIterator(tags)
		series := addMockSeries(ctrl, shard, id, tags, uint64(i))
		if i == 2 {
			series.EXPECT().
				FetchBlocksMetadata(gomock.Not(nil), start, end, seriesFetchOpts).
				Return(block.NewFetchBlocksMetadataResult(id, tagsIter,
					block.NewFetchBlockMetadataResults()), nil)
		} else if i > 2 && i <= 7 {
			ids = append(ids, id)
			blocks := block.NewFetchBlockMetadataResults()
			at := start.Add(time.Duration(i))
			blocks.Add(block.NewFetchBlockMetadataResult(at, 0, nil, lastRead, nil))
			series.EXPECT().
				FetchBlocksMetadata(gomock.Not(nil), start, end, seriesFetchOpts).
				Return(block.NewFetchBlocksMetadataResult(id, tagsIter,
					blocks), nil)
		}
	}

	res, nextPageToken, err := shard.FetchBlocksMetadata(ctx, start, end, 5, 2, fetchOpts)
	require.NoError(t, err)
	require.Equal(t, len(ids), len(res.Results()))
	require.Equal(t, int64(8), *nextPageToken)
	for i := 0; i < len(res.Results()); i++ {
		require.Equal(t, ids[i], res.Results()[i].ID)
	}
}

func TestShardFetchBlocksMetadataV2WithSeriesCachePolicyCacheAll(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions().SetSeriesCachePolicy(series.CacheAll)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()
	start := time.Now()
	end := start.Add(defaultTestRetentionOpts.BlockSize())

	fetchLimit := int64(5)
	startCursor := int64(2)

	var ids []ident.ID
	fetchOpts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     true,
		IncludeChecksums: true,
		IncludeLastRead:  true,
	}
	seriesFetchOpts := series.FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: fetchOpts,
		IncludeCachedBlocks:        true,
	}
	lastRead := time.Now().Add(-time.Minute)
	for i := int64(0); i < 10; i++ {
		id := ident.StringID(fmt.Sprintf("foo.%d", i))
		tags := ident.NewTags(
			ident.StringTag("aaa", "bbb"),
			ident.StringTag("ccc", "ddd"),
		)
		tagsIter := ident.NewTagsIterator(tags)
		series := addMockSeries(ctrl, shard, id, tags, uint64(i))
		if i == startCursor {
			series.EXPECT().
				FetchBlocksMetadata(gomock.Not(nil), start, end, seriesFetchOpts).
				Return(block.NewFetchBlocksMetadataResult(id, tagsIter,
					block.NewFetchBlockMetadataResults()), nil)
		} else if i > startCursor && i <= startCursor+fetchLimit {
			ids = append(ids, id)
			blocks := block.NewFetchBlockMetadataResults()
			at := start.Add(time.Duration(i))
			blocks.Add(block.NewFetchBlockMetadataResult(at, 0, nil, lastRead, nil))
			series.EXPECT().
				FetchBlocksMetadata(gomock.Not(nil), start, end, seriesFetchOpts).
				Return(block.NewFetchBlocksMetadataResult(id, tagsIter,
					blocks), nil)
		}
	}

	currPageToken, err := proto.Marshal(&pagetoken.PageToken{
		ActiveSeriesPhase: &pagetoken.PageToken_ActiveSeriesPhase{
			IndexCursor: startCursor,
		},
	})
	require.NoError(t, err)

	res, nextPageToken, err := shard.FetchBlocksMetadataV2(ctx, start, end,
		fetchLimit, currPageToken, fetchOpts)
	require.NoError(t, err)
	require.Equal(t, len(ids), len(res.Results()))

	pageToken := new(pagetoken.PageToken)
	err = proto.Unmarshal(nextPageToken, pageToken)
	require.NoError(t, err)

	require.NotNil(t, pageToken.GetActiveSeriesPhase())
	require.Equal(t, int64(8), pageToken.GetActiveSeriesPhase().IndexCursor)

	for i := 0; i < len(res.Results()); i++ {
		require.Equal(t, ids[i], res.Results()[i].ID)
	}
}

type fetchBlockMetadataResultByStart []block.FetchBlockMetadataResult

func (b fetchBlockMetadataResultByStart) Len() int      { return len(b) }
func (b fetchBlockMetadataResultByStart) Swap(i, j int) { b[i], b[j] = b[j], b[i] }
func (b fetchBlockMetadataResultByStart) Less(i, j int) bool {
	return b[i].Start.Before(b[j].Start)
}

func TestShardFetchBlocksMetadataV2WithSeriesCachePolicyNotCacheAll(t *testing.T) {
	ctrl := gomock.NewController(xtest.Reporter{t})
	defer ctrl.Finish()

	opts := testDatabaseOptions().SetSeriesCachePolicy(series.CacheRecentlyRead)
	ctx := opts.ContextPool().Get()
	defer ctx.Close()

	fsOpts := opts.CommitLogOptions().FilesystemOptions()

	shard := testDatabaseShard(t, opts)
	defer shard.Close()

	ropts := defaultTestRetentionOpts
	blockSize := ropts.BlockSize()
	retentionPeriod := ropts.RetentionPeriod()
	now := time.Now()
	mostRecentBlockStart := now.Truncate(blockSize)
	start := mostRecentBlockStart.Add(-retentionPeriod)
	end := mostRecentBlockStart.Add(blockSize)

	// Choose num of series to return from different phases
	numActiveSeries := 10
	numFlushedSeries := 25

	// Choose a fetch limit that spans multiple pages and a partial page
	fetchLimit := int64(4)

	fetchOpts := block.FetchBlocksMetadataOptions{
		IncludeSizes:     true,
		IncludeChecksums: true,
		IncludeLastRead:  true,
	}

	// Populate the mocks and filesets and collect what the expected
	// results will be
	expected := map[string]fetchBlockMetadataResultByStart{}

	// Write flushed series
	for at := start; at.Before(mostRecentBlockStart); at = at.Add(blockSize) {
		writer, err := fs.NewWriter(fsOpts)
		require.NoError(t, err)

		writerOpts := fs.DataWriterOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:  shard.namespace.ID(),
				Shard:      shard.shard,
				BlockStart: at,
			},
			BlockSize: blockSize,
		}
		err = writer.Open(writerOpts)
		require.NoError(t, err)

		for i := 0; i < numFlushedSeries; i++ {
			idxBlock := time.Duration(at.UnixNano()-start.UnixNano()) / blockSize
			if (idxBlock%2 == 0 && i%2 == 0) || (idxBlock%2 != 0 && i%2 != 0) {
				continue // Every other block skip the evens and odds
			}

			id := ident.StringID(fmt.Sprintf("series+instance=%d", i))
			data := make([]byte, 8)
			_, err = rand.Read(data)
			require.NoError(t, err)

			checksum := digest.Checksum(data)

			bytes := checked.NewBytes(data, nil)
			bytes.IncRef()
			err = writer.Write(id, ident.Tags{}, bytes, checksum)
			require.NoError(t, err)

			blockMetadataResult := block.NewFetchBlockMetadataResult(at,
				int64(len(data)), &checksum, time.Time{}, nil)
			expected[id.String()] = append(expected[id.String()], blockMetadataResult)
		}

		err = writer.Close()
		require.NoError(t, err)
	}

	// Add mock active series
	seriesFetchOpts := series.FetchBlocksMetadataOptions{
		FetchBlocksMetadataOptions: fetchOpts,
		IncludeCachedBlocks:        false,
	}
	lastRead := time.Now().Add(-time.Minute)
	for i := 0; i < numActiveSeries; i++ {
		id := ident.StringID(fmt.Sprintf("series+instance=%d", i))
		tags := ident.NewTags(
			ident.StringTag("instance", strconv.Itoa(i)),
		)
		tagsIter := ident.NewTagsIterator(tags)
		series := addMockSeries(ctrl, shard, id, tags, uint64(i))
		blocks := block.NewFetchBlockMetadataResults()
		at := mostRecentBlockStart
		blockMetadataResult := block.NewFetchBlockMetadataResult(at, 0, nil, lastRead, nil)
		blocks.Add(blockMetadataResult)
		series.EXPECT().
			FetchBlocksMetadata(gomock.Not(nil), start, end, seriesFetchOpts).
			Return(block.NewFetchBlocksMetadataResult(id, tagsIter, blocks), nil)

		// Add to the expected blocks result
		expected[id.String()] = append(expected[id.String()], blockMetadataResult)
	}

	// Iterate the actual results
	actual := map[string]fetchBlockMetadataResultByStart{}

	var (
		currPageToken PageToken
		first         = true
	)
	for {
		if !first && currPageToken == nil {
			break // Reached end of iteration
		}

		first = false
		res, nextPageToken, err := shard.FetchBlocksMetadataV2(ctx, start, end,
			fetchLimit, currPageToken, fetchOpts)
		require.NoError(t, err)

		currPageToken = nextPageToken

		for _, elem := range res.Results() {
			for _, r := range elem.Blocks.Results() {
				actual[elem.ID.String()] = append(actual[elem.ID.String()], r)
			}
		}
	}

	// Sort the results
	for key := range expected {
		sort.Sort(expected[key])
	}
	for key := range actual {
		sort.Sort(actual[key])
	}

	// Evaluate results
	require.Equal(t, len(expected), len(actual))

	for id, expectedResults := range expected {
		actualResults, ok := actual[id]
		if !ok {
			require.FailNow(t, fmt.Sprintf("id %s missing from actual results", id))
		}

		require.Equal(t, len(expectedResults), len(actualResults))

		for i, expectedBlock := range expectedResults {
			actualBlock := actualResults[i]

			assert.True(t, expectedBlock.Start.Equal(actualBlock.Start))
			assert.Equal(t, expectedBlock.Size, actualBlock.Size)
			if expectedBlock.Checksum == nil {
				assert.Nil(t, actualBlock.Checksum)
			} else if actualBlock.Checksum == nil {
				assert.Fail(t, fmt.Sprintf("expected checksum but no actual checksum"))
			} else {
				assert.Equal(t, *expectedBlock.Checksum, *actualBlock.Checksum)
			}
			assert.True(t, expectedBlock.LastRead.Equal(actualBlock.LastRead))
			assert.Equal(t, expectedBlock.Err, actualBlock.Err)
		}
	}
}
