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

package tile

import (
	"fmt"
	"io"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/tools"
	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/dbnode/persist"
	"github.com/m3db/m3/src/dbnode/persist/fs"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// func newSequentialIterator(
// 	ctrl *gomock.Controller,
// 	start time.Time,
// 	step time.Duration,
// 	numPoints int,
// ) encoding.ReaderIterator {
// 	bl := encoding.NewMockReaderIterator(ctrl)
// 	currVal, currTs, currTsNano := 0.0, start, xtime.ToUnixNano(start)
// 	for i := 0; i < numPoints; i++ {
// 		i := i
// 		bl.EXPECT().Next().DoAndReturn(func() bool {
// 			// NB: only increment after first Next.
// 			if i > 0 {
// 				currVal++
// 				currTs = currTs.Add(step)
// 				currTsNano += xtime.UnixNano(step)
// 			}
// 			return true
// 		}).Times(1)

// 		bl.EXPECT().Current().DoAndReturn(func() (ts.Datapoint, xtime.Unit, []byte) {
// 			return ts.Datapoint{
// 				Value:          currVal,
// 				Timestamp:      currTs,
// 				TimestampNanos: currTsNano,
// 			}, xtime.Second, nil
// 		}).AnyTimes()
// 	}

// 	bl.EXPECT().Next().Return(false)
// 	bl.EXPECT().Err().Return(nil).AnyTimes()
// 	bl.EXPECT().Close().AnyTimes()

// 	return bl
// // }

// func halfFrameSizes(numPoints int) []float64 {
// 	frames := make([]float64, numPoints*2-1)
// 	v := 0.0
// 	for i := range frames {
// 		if i%2 == 0 {
// 			frames[i] = v
// 			v++
// 		}
// 	}

// 	return frames
// }

// func halfFrameCounts(numPoints int) []int {
// 	frames := make([]int, numPoints*2-1)
// 	for i := range frames {
// 		if i%2 == 0 {
// 			frames[i] = 1
// 		}
// 	}

// 	return frames
// }

func buildMockDataReader(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	reader := fs.NewMockDataFileSetReader(ctrl)

	size := 10
	for i := 0; i < size; i++ {
		var (
			trailing  = fmt.Sprintf("_%d", i)
			id        = ident.StringID("id" + trailing)
			tags      = ident.MustNewTagStringsIterator("__name__", "foo"+trailing)
			dataBytes = checked.NewMockBytes(ctrl) // FIXME:
			checksum  = uint32(i)
		)

		reader.EXPECT().Read().Return(id, tags, dataBytes, checksum, nil)
	}

	reader.EXPECT().Read().Return(nil, nil, nil, 0, io.EOF)
}

func TestSeriesBlockIterator(t *testing.T) {
	bytesPool := tools.NewCheckedBytesPool()
	bytesPool.Init()

	encodingOpts := encoding.NewOptions().SetBytesPool(bytesPool)

	for z := 0; z < 5; z++ {
		fsOpts := fs.NewOptions().SetFilePathPrefix("/Users/arnikola/tmp/var/lib/m3db")
		reader, err := fs.NewReader(bytesPool, fsOpts)
		require.NoError(t, err)

		startNanos := int64(1593921600000000000)
		openOpts := fs.DataReaderOpenOptions{
			Identifier: fs.FileSetFileIdentifier{
				Namespace:   ident.StringID("default"),
				Shard:       184,
				BlockStart:  time.Unix(0, startNanos),
				VolumeIndex: 0,
			},
			FileSetType: persist.FileSetFlushType,
		}

		require.NoError(t, reader.Open(openOpts))
		concurrency := 5 //runtime.NumCPU()
		opts := Options{
			FrameSize:    xtime.UnixNano(10) * xtime.UnixNano(time.Minute),
			Start:        xtime.UnixNano(startNanos),
			Concurrency:  concurrency,
			UseArrow:     false,
			EncodingOpts: encodingOpts,
		}

		it, err := NewSeriesBlockIterator(reader, opts)
		require.NoError(t, err)

		defer func() {
			assert.NoError(t, it.Close())
		}()

		var wg sync.WaitGroup
		// print := 124
		for i := 0; it.Next(); i++ {
			frameIters := it.Current()
			for j, frameIter := range frameIters {
				frameIter := frameIter
				j := j
				wg.Add(1)
				go func() {
					defer wg.Done()
					vs := make([]float64, 0, 12)
					for frameIter.Next() {
						frame := frameIter.Current()
						v := frame.Sum()
						fs := frame.Summary().Sum()

						if !math.IsNaN(v) && v != 0 {
							assert.True(t, frame.Summary().Valid())
							if v != fs {
								fmt.Println(frame.Values())
							}

							require.Equal(t, v, fs)
						} else {
							if frame.Summary().Valid() && fs != 0 {
								fmt.Println(i*concurrency+j, ":", v, "fs", fs)
								fmt.Println(frame.Values())
								require.False(t, frame.Summary().Valid())
							}
						}

						vs = append(vs, v)
					}

					// if len(vs) > 0 {
					// 	fmt.Println(i*concurrency+j, vs)
					// }
				}()
			}

			wg.Wait()
		}
	}

}
