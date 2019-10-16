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
	"strings"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/m3db/m3/src/dbnode/x/xio"
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
}

var iterAlloc = func(r io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
	return m3tsz.NewReaderIterator(r, m3tsz.DefaultIntOptimizationEnabled, encoding.NewOptions())
}

func buildIterator(
	dps [][]ts.Datapoint,
	tagMap map[string]string,
	opts iteratorOptions,
) (encoding.SeriesIterator, error) {
	var (
		encoders = make([]encoding.Encoder, 0, len(dps))
		readers  = make([][]xio.BlockReader, 0, len(dps))

		tags = ident.Tags{}
		sb   strings.Builder
	)

	for range dps {
		encoders = append(encoders, opts.encoderPool.Get())
	}

	defer func() {
		for _, e := range encoders {
			if e != nil {
				e.Close()
			}
		}
	}()

	// Build a merged BlockReader
	for i, datapoints := range dps {
		encoder := encoders[i]
		encoder.Reset(opts.start, len(datapoints), nil)
		if len(datapoints) > 0 {
			for _, dp := range datapoints {
				err := encoder.Encode(dp, xtime.Second, nil)
				if err != nil {
					return nil, err
				}
			}

			segment := encoder.Discard()
			readers = append(readers, []xio.BlockReader{{
				SegmentReader: xio.NewSegmentReader(segment),
				Start:         opts.start,
				BlockSize:     opts.blockSize,
			}})
		}
	}

	multiReader := encoding.NewMultiReaderIterator(
		iterAlloc,
		opts.iteratorPools.MultiReaderIterator(),
	)

	sliceOfSlicesIter := xio.NewReaderSliceOfSlicesFromBlockReadersIterator(readers)
	multiReader.ResetSliceOfSlices(sliceOfSlicesIter, nil)

	for name, value := range tagMap {
		sb.WriteString(name)
		sb.WriteRune(sep)
		sb.WriteString(value)
		sb.WriteRune(tagSep)
		tags.Append(ident.StringTag(name, value))
	}

	return encoding.NewSeriesIterator(
			encoding.SeriesIteratorOptions{
				ID:             ident.StringID(sb.String()),
				Namespace:      ident.StringID("ns"),
				Tags:           ident.NewTagsIterator(tags),
				StartInclusive: opts.start,
				EndExclusive:   opts.start.Add(opts.blockSize),
				Replicas: []encoding.MultiReaderIterator{
					multiReader,
				},
			}, nil),
		nil
}
