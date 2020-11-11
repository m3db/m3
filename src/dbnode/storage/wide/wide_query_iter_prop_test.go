// +build big
//
// Copyright (c) 2020 Uber Technologies, Inc.
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

package wide

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/encoding/m3tsz"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
)

type expectedRecord struct {
	vals         []float64
	expectedTags []byte
	expectedID   []byte
}

type testRecord struct {
	record   ShardIteratorRecord
	expected expectedRecord
}

func genRecord(encoderPool encoding.EncoderPool, start time.Time) gopter.Gen {
	return gopter.CombineGens(
		gen.SliceOf(gen.Float64()),
		gen.AnyString(),
		gen.AnyString(),
	).Map(func(val []interface{}) testRecord {
		var (
			vals = val[0].([]float64)
			id   = val[1].(string)
			tags = val[2].(string)
		)

		encoder := encoderPool.Get()
		encoder.Reset(start, len(vals), nil)
		for i, v := range vals {
			timestamp := start.Add(time.Duration(i+1) * time.Second)
			dp := ts.Datapoint{
				Value:          v,
				Timestamp:      timestamp,
				TimestampNanos: xtime.ToUnixNano(timestamp),
			}

			encoder.Encode(dp, xtime.Nanosecond, nil)
		}

		seg := encoder.Discard()
		var encodedData []byte
		if seg.Head != nil {
			seg.Head.IncRef()
			encodedData = append(encodedData, seg.Head.Bytes()...)
			seg.Head.DecRef()
		}

		if seg.Tail != nil {
			seg.Tail.IncRef()
			encodedData = append(encodedData, seg.Tail.Bytes()...)
			seg.Tail.DecRef()
		}

		return testRecord{
			record: ShardIteratorRecord{
				EncodedTags: []byte(tags),
				ID:          []byte(id),
				Data:        encodedData,
			},
			expected: expectedRecord{
				vals:         append(make([]float64, 0, len(vals)), vals...),
				expectedTags: []byte(tags),
				expectedID:   []byte(id),
			},
		}
	})
}

func genRecords(encoderPool encoding.EncoderPool, start time.Time) gopter.Gen {
	return gen.SliceOf(genRecord(encoderPool, start)).
		Map(func(val []testRecord) []testRecord {
			return val
		})
}

func TestWideQueryIter(t *testing.T) {
	var (
		parameters = gopter.DefaultTestParameters()
		seed       = time.Now().UnixNano()
		props      = gopter.NewProperties(parameters)
		reporter   = gopter.NewFormatedReporter(true, 80, os.Stdout)

		// opts         = NewOptions().SetDecodingOptions(decodingOpts)
		numTests = 1000
	)

	parameters.MinSuccessfulTests = numTests
	parameters.MinSize = 1
	parameters.MaxSize = 30
	parameters.Rng.Seed(seed)

	encoderPool := encoding.NewEncoderPool(nil)
	encoderPool.Init(func() encoding.Encoder {
		return m3tsz.NewEncoder(time.Time{}, nil, false, encoding.NewOptions())
	})

	readerPool := encoding.NewReaderIteratorPool(nil)
	readerPool.Init(
		func(reader io.Reader, _ namespace.SchemaDescr) encoding.ReaderIterator {
			decoder := m3tsz.NewDecoder(false, nil)
			return decoder.Decode(reader)
		})

	opts := NewOptions().SetReaderIteratorPool(readerPool)
	now := time.Now().Truncate(time.Hour)

	// NB: capture seed to be able to replicate failed runs.
	fmt.Println("Running test with seed", seed)
	props.Property("Checksum mismatcher detects correctly",
		prop.ForAll(
			func(genRecords []testRecord) (bool, error) {
				iter := NewQueryIterator(now, []uint32{1}, opts)

				go func() {
					writeShard, err := iter.ShardIter(1)
					require.NoError(t, err)
					for _, rec := range genRecords {
						require.NoError(t, writeShard.PushRecord(rec.record))
					}

					iter.SetDoneError(nil)
				}()

				if !iter.Next() {
					return false, errors.New("no shard iterator")
				}

				var (
					shardVals [][]float64
					metas     []SeriesMetadata
				)

				shardIter := iter.Current()
				for shardIter.Next() {
					seriesIter := shardIter.Current()
					var seriesVals []float64
					for seriesIter.Next() {
						dp, _, _ := seriesIter.Current()
						seriesVals = append(seriesVals, dp.Value)
					}

					metas = append(metas, seriesIter.SeriesMetadata())
					shardVals = append(shardVals, seriesVals)
				}

				if iter.Next() {
					return false, errors.New("too many shard iterators")
				}

				if err := iter.Err(); err != nil {
					return false, err
				}

				iter.Close()

				if len(shardVals) != len(genRecords) {
					return false, fmt.Errorf("expected %d vals, got %d",
						len(shardVals), len(genRecords))
				}

				for idx, record := range genRecords {
					if !assert.ObjectsAreEqual(shardVals[idx], record.expected.vals) {
						return false, errors.New("value mismatch")
					}

					if !bytes.Equal(record.expected.expectedID, metas[idx].ID.Bytes()) {
						return false, errors.New("ID mismatch")
					}

					if !bytes.Equal(record.expected.expectedTags, metas[idx].EncodedTags) {
						return false, errors.New("tag mismatch")
					}
				}

				return true, nil
			}, genRecords(encoderPool, now)))

	if !props.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
