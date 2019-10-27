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

package main

import (
	"io"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/x/xio"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

const sep rune = '!'
const tagSep rune = '.'

type iteratorOptions struct {
	blockSize     time.Duration
	start         time.Time
	encoderPool   encoding.EncoderPool
	iteratorPools encoding.IteratorPools
	tagOptions    models.TagOptions
}

var iterAlloc = func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
	return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
}

func buildBlockReader(
	block seriesBlock,
	start time.Time,
	opts iteratorOptions,
) ([]xio.BlockReader, error) {
	encoder := opts.encoderPool.Get()
	encoder.Reset(start, len(block), nil)
	for _, dp := range block {
		err := encoder.Encode(dp, xtime.Second, nil)
		if err != nil {
			encoder.Close()
			return nil, err
		}
	}

	segment := encoder.Discard()
	return []xio.BlockReader{
		xio.BlockReader{
			SegmentReader: xio.NewSegmentReader(segment),
			Start:         start,
			BlockSize:     opts.blockSize,
		},
	}, nil
}

func buildTagIteratorAndID(
	tagMap tagMap,
	opts models.TagOptions,
) (ident.TagIterator, ident.ID) {
	var (
		tags      = ident.Tags{}
		modelTags = models.NewTags(len(tagMap), opts)
	)

	for name, value := range tagMap {
		modelTags = modelTags.AddOrUpdateTag(models.Tag{
			Name:  []byte(name),
			Value: []byte(value),
		})

		tags.Append(ident.StringTag(name, value))
	}

	id := string(modelTags.ID())
	return ident.NewTagsIterator(tags), ident.StringID(id)
}

func buildSeriesIterator(
	series series,
	opts iteratorOptions,
) (encoding.SeriesIterator, error) {
	var (
		blocks  = series.blocks
		tags    = series.tags
		readers = make([][]xio.BlockReader, 0, len(blocks))
		start   = opts.start
	)

	for _, block := range blocks {
		seriesBlock, err := buildBlockReader(block, start, opts)
		if err != nil {
			return nil, err
		}

		readers = append(readers, seriesBlock)
		start = start.Add(opts.blockSize)
	}

	multiReader := encoding.NewMultiReaderIterator(
		iterAlloc,
		opts.iteratorPools.MultiReaderIterator(),
	)

	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
	multiReader.ResetSliceOfSlices(sliceOfSlicesIter, nil)

	end := opts.start.Add(opts.blockSize)
	if len(blocks) > 0 {
		lastBlock := blocks[len(blocks)-1]
		end = lastBlock[len(lastBlock)-1].Timestamp
	}

	tagIter, id := buildTagIteratorAndID(tags, opts.tagOptions)
	return encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{
				ID:             id,
				Namespace:      ident.StringID("ns"),
				Tags:           tagIter,
				StartInclusive: opts.start,
				EndExclusive:   end,
				Replicas: []encoding.MultiReaderIterator{
					multiReader,
				},
			}, nil),
		nil
}

func buildSeriesIterators(
	series []series,
	opts iteratorOptions,
) (encoding.SeriesIterators, error) {
	iters := make([]encoding.SeriesIterator, 0, len(series))
	for _, s := range series {
		iter, err := buildSeriesIterator(s, opts)
		if err != nil {
			return nil, err
		}

		iters = append(iters, iter)
	}

	return encoding.NewSeriesIterators(
		iters,
		opts.iteratorPools.MutableSeriesIterators(),
	), nil
}
