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

package remote

import (
	"bytes"
	"fmt"
	"math"
	"sort"
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/query/storage"

	protobuf "github.com/golang/protobuf/proto"
)

type WriteRequest struct {
	Series []WriteSeries
}

func (p *WriteRequest) Finalize() {
	putWriteRequest(p)
}

type WriteSeries struct {
	Labels  []Label
	Samples []Sample
}

type Label struct {
	Name  []byte
	Value []byte
}

type Sample struct {
	TimeUnixMillis int64
	Value          float64

	Result storage.WriteQueryResult
	State  interface{}
}

func ParseWriteRequest(data []byte) (*WriteRequest, error) {
	req := getWriteRequest()

	buff := proto.NewBuffer(data)
	innerBuff := proto.NewBuffer(nil)
	innerInnerBuff := proto.NewBuffer(nil)
	for !buff.EOF() {
		fieldNum, wireType, err := buff.DecodeTagAndWireType()
		if err != nil {
			return nil, err
		}

		if fieldNum != 1 {
			return nil, fmt.Errorf("expecting only time series field")
		}
		if wireType != protobuf.WireBytes {
			return nil, fmt.Errorf("expecting series message wire type")
		}

		// Extend series as necessary (get underlying refs to
		// existing labels/samples slices).
		length := len(req.Series)
		capacity := cap(req.Series)
		index := length
		if length == capacity {
			if capacity == 0 {
				capacity = 1
			}
			newSeries := make([]WriteSeries, length, 2*capacity)
			copy(newSeries, req.Series)
			req.Series = newSeries
		}

		req.Series = req.Series[:index+1]
		series := req.Series[index]

		inner, err := buff.DecodeRawBytes(false)
		if err != nil {
			return nil, err
		}

		innerBuff.Reset(inner)
		for !innerBuff.EOF() {
			fieldNum, wireType, err := innerBuff.DecodeTagAndWireType()
			if err != nil {
				return nil, err
			}

			switch fieldNum {
			case 1:
				// Decode label.
				if wireType != protobuf.WireBytes {
					return nil, fmt.Errorf("expecting label message wire type")
				}

				var label Label

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return nil, err
				}

				innerInnerBuff.Reset(innerInner)
				for !innerInnerBuff.EOF() {
					fieldNum, wireType, err := innerInnerBuff.DecodeTagAndWireType()
					if err != nil {
						return nil, err
					}

					switch fieldNum {
					case 1:
						if wireType != protobuf.WireBytes {
							return nil, fmt.Errorf("expecting label name bytes wire type")
						}

						label.Name, err = innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return nil, err
						}
					case 2:
						if wireType != protobuf.WireBytes {
							return nil, fmt.Errorf("expecting label name bytes wire type")
						}

						label.Value, err = innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return nil, err
						}
					default:
						return nil, fmt.Errorf("unknown label field: %v", fieldNum)
					}
				}

				series.Labels = append(series.Labels, label)
			case 2:
				// Decode sample.
				if wireType != protobuf.WireBytes {
					return nil, fmt.Errorf("expecting label message wire type")
				}

				var sample Sample

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return nil, err
				}

				innerInnerBuff.Reset(innerInner)
				for !innerInnerBuff.EOF() {
					fieldNum, wireType, err := innerInnerBuff.DecodeTagAndWireType()
					if err != nil {
						return nil, err
					}

					switch fieldNum {
					case 1:
						if wireType != protobuf.WireFixed64 {
							return nil, fmt.Errorf("expecting sample value wire type")
						}

						var valueBits uint64
						valueBits, err = innerInnerBuff.DecodeFixed64()
						if err != nil {
							return nil, err
						}

						sample.Value = math.Float64frombits(valueBits)
					case 2:
						if wireType != protobuf.WireVarint {
							return nil, fmt.Errorf("expecting sample timestamp wire type")
						}

						var value uint64
						value, err = innerInnerBuff.DecodeVarint()
						if err != nil {
							return nil, err
						}

						sample.TimeUnixMillis = int64(value)
					default:
						return nil, fmt.Errorf("unknown sample field: %v", fieldNum)
					}
				}

				series.Samples = append(series.Samples, sample)
			default:
				return nil, fmt.Errorf("unknown series field: %v", fieldNum)
			}
		}

		// Make sure series are sorted by name.
		sort.Sort(labelsByName(series.Labels))

		// Reset value.
		req.Series[index] = series
	}

	return req, nil
}

var (
	writeRequestPool = sync.Pool{
		New: func() interface{} {
			return &WriteRequest{}
		},
	}
	emptyWriteSeries WriteSeries
	emptyLabel       Label
	emptySample      Sample
)

func getWriteRequest() *WriteRequest {
	return writeRequestPool.Get().(*WriteRequest)
}

func putWriteRequest(v *WriteRequest) {
	for i := range v.Series {
		for j := range v.Series[i].Labels {
			v.Series[i].Labels[j] = emptyLabel
		}
		v.Series[i].Labels = v.Series[i].Labels[:0]
		for j := range v.Series[i].Samples {
			v.Series[i].Samples[j] = emptySample
		}
		v.Series[i].Samples = v.Series[i].Samples[:0]
	}
	v.Series = v.Series[:0]
	writeRequestPool.Put(v)
}

var _ sort.Interface = labelsByName(nil)

type labelsByName []Label

func (l labelsByName) Len() int {
	return len(l)
}

func (l labelsByName) Less(i, j int) bool {
	return bytes.Compare(l[i].Name, l[j].Name) < 0
}

func (l labelsByName) Swap(i, j int) {
	l[i], l[j] = l[j], l[i]
}
