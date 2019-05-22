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
	"errors"

	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

// fsMergeWithMem implements fs.MergeWith
type fsMergeWithMem struct {
	shard              databaseShard
	retriever          series.QueryableBlockRetriever
	dirtySeries        *dirtySeriesMap
	dirtySeriesToWrite map[xtime.UnixNano]*idList
}

func newFSMergeWithMem(
	shard databaseShard,
	retriever series.QueryableBlockRetriever,
	dirtySeries *dirtySeriesMap,
	dirtySeriesToWrite map[xtime.UnixNano]*idList,
) *fsMergeWithMem {
	return &fsMergeWithMem{
		shard:              shard,
		retriever:          retriever,
		dirtySeries:        dirtySeries,
		dirtySeriesToWrite: dirtySeriesToWrite,
	}
}

func (m *fsMergeWithMem) Read(
	seriesID ident.ID,
	blockStart xtime.UnixNano,
	nsCtx namespace.Context,
) ([]xio.BlockReader, bool, error) {
	// Check if this series is in memory (and thus requires merging).
	element, exists := m.dirtySeries.Get(idAndBlockStart{blockStart: blockStart, id: seriesID})
	if !exists {
		return nil, false, nil
	}

	startTime := blockStart.ToTime()

	// Series is in memory, so it will get merged with disk and
	// written in this loop. Therefore, we need to remove it from
	// the "to write" list so that the later loop does not rewrite
	// it.
	m.dirtySeriesToWrite[blockStart].Remove(element)

	nextVersion := m.retriever.RetrievableBlockColdVersion(startTime) + 1

	tmpCtx := context.NewContext()
	blocks, err := m.shard.FetchBlocksForColdFlush(tmpCtx, element.Value, startTime, nextVersion, nsCtx)
	tmpCtx.BlockingClose()
	if err != nil {
		return nil, false, err
	}

	if len(blocks) > 0 {
		return blocks, true, nil
	}

	return nil, false, nil
}

func (m *fsMergeWithMem) ForEachRemaining(blockStart xtime.UnixNano, fn fs.ForEachRemainingFn) error {
	seriesList := m.dirtySeriesToWrite[blockStart]

	for seriesElement := seriesList.Front(); seriesElement != nil; seriesElement = seriesElement.Next() {
		seriesID := seriesElement.Value
		tags, err := m.shard.TagsFromSeriesID(seriesID)
		if err != nil {
			return err
		}

		success := fn(seriesID, tags)
		if !success {
			return errors.New("foreach iteration unsuccessful")
		}
	}

	return nil
}
