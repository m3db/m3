// Copyright (c) 2019 Uber Technologies, Inc.
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
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// fsMergeWithMem implements fs.MergeWith, where the merge target is data in
// memory. It relies on data structures being passed in that are created
// within the shard informing fsMergeWithMem which series require merging.
// These data structures enable efficient reading of data as well as keeping
// track of which series were read so that the remaining series can be looped
// through.
type fsMergeWithMem struct {
	shard              databaseShard
	retriever          series.QueryableBlockRetriever
	dirtySeries        *dirtySeriesMap
	dirtySeriesToWrite map[xtime.UnixNano]*idList
	reusableID         *ident.ReusableBytesID
}

func newFSMergeWithMem(
	shard databaseShard,
	retriever series.QueryableBlockRetriever,
	dirtySeries *dirtySeriesMap,
	dirtySeriesToWrite map[xtime.UnixNano]*idList,
) fs.MergeWith {
	return &fsMergeWithMem{
		shard:              shard,
		retriever:          retriever,
		dirtySeries:        dirtySeries,
		dirtySeriesToWrite: dirtySeriesToWrite,
		reusableID:         ident.NewReusableBytesID(),
	}
}

func (m *fsMergeWithMem) Read(
	ctx context.Context,
	seriesID ident.ID,
	blockStart xtime.UnixNano,
	nsCtx namespace.Context,
) ([]xio.BlockReader, bool, error) {
	// Check if this series is in memory (and thus requires merging).
	element, exists := m.dirtySeries.Get(idAndBlockStart{
		blockStart: blockStart,
		id:         seriesID.Bytes(),
	})
	if !exists {
		return nil, false, nil
	}

	// Series is in memory, so it will get merged with disk and
	// written in this loop. Therefore, we need to remove it from
	// the "to write" list so that the later loop does not rewrite
	// it.
	m.dirtySeriesToWrite[blockStart].Remove(element)

	result, ok, err := m.fetchBlocks(ctx, seriesID, blockStart, nsCtx)
	if err != nil {
		return nil, false, err
	}
	return result.Blocks, ok, nil
}

func (m *fsMergeWithMem) fetchBlocks(
	ctx context.Context,
	id ident.ID,
	blockStart xtime.UnixNano,
	nsCtx namespace.Context,
) (block.FetchBlockResult, bool, error) {
	startTime := blockStart.ToTime()
	currVersion, err := m.retriever.RetrievableBlockColdVersion(startTime)
	if err != nil {
		return block.FetchBlockResult{}, false, err
	}
	nextVersion := currVersion + 1

	result, err := m.shard.FetchBlocksForColdFlush(ctx, id, startTime, nextVersion, nsCtx)
	if err != nil {
		return block.FetchBlockResult{}, false, err
	}

	if len(result.Blocks) > 0 {
		return result, true, nil
	}

	return block.FetchBlockResult{}, false, nil
}

// The data passed to ForEachRemaining (through the fs.ForEachRemainingFn) is
// basically a copy that will be finalized when the context is closed, but the
// ID and tags are expected to live for as long as the caller of the MergeWith
// requires them, so they should either be NoFinalize() or passed as copies.
func (m *fsMergeWithMem) ForEachRemaining(
	ctx context.Context,
	blockStart xtime.UnixNano,
	fn fs.ForEachRemainingFn,
	nsCtx namespace.Context,
) error {
	reusableID := m.reusableID
	seriesList := m.dirtySeriesToWrite[blockStart]

	for seriesElement := seriesList.Front(); seriesElement != nil; seriesElement = seriesElement.Next() {
		seriesMetadata := seriesElement.Value
		reusableID.Reset(seriesMetadata.ID)
		mergeWithData, hasData, err := m.fetchBlocks(ctx, reusableID, blockStart, nsCtx)
		if err != nil {
			return err
		}

		if hasData {
			err = fn(seriesMetadata, mergeWithData)
			if err != nil {
				return err
			}
		}
	}

	return nil
}
