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

	pooledBatch pooledBatch
}

func (p *WriteRequest) Finalize() {
	parsePutPooled(p.pooledBatch)
	p.pooledBatch = pooledBatch{}
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
	req := &WriteRequest{}

	numSeries := 0
	numLabels := 0
	numSamples := 0
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

		inner, err := buff.DecodeRawBytes(false)
		if err != nil {
			return nil, err
		}

		numSeries++

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

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return nil, err
				}

				numLabels++

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

						_, err = innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return nil, err
						}
					case 2:
						if wireType != protobuf.WireBytes {
							return nil, fmt.Errorf("expecting label name bytes wire type")
						}

						_, err = innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return nil, err
						}
					default:
						return nil, fmt.Errorf("unknown label field: %v", fieldNum)
					}
				}
			case 2:
				// Decode sample.
				if wireType != protobuf.WireBytes {
					return nil, fmt.Errorf("expecting label message wire type")
				}

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return nil, err
				}

				numSamples++

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

						_, err = innerInnerBuff.DecodeFixed64()
						if err != nil {
							return nil, err
						}
					case 2:
						if wireType != protobuf.WireVarint {
							return nil, fmt.Errorf("expecting sample timestamp wire type")
						}

						_, err = innerInnerBuff.DecodeVarint()
						if err != nil {
							return nil, err
						}
					default:
						return nil, fmt.Errorf("unknown sample field: %v", fieldNum)
					}
				}
			default:
				return nil, fmt.Errorf("unknown series field: %v", fieldNum)
			}
		}
	}

	req.pooledBatch = parseGetPooled(numSeries, numLabels, numSamples)

	req.Series = req.pooledBatch.series[:0]
	labelsPtr := req.pooledBatch.labels[:numLabels]
	samplesPtr := req.pooledBatch.samples[:numSamples]

	buff.Reset(data)
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

		var (
			labels  = labelsPtr[:0]
			samples = samplesPtr[:0]
		)

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

				labels = append(labels, label)
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

				samples = append(samples, sample)
			default:
				return nil, fmt.Errorf("unknown series field: %v", fieldNum)
			}
		}

		// Make sure series are sorted by name.
		sort.Sort(labelsByName(labels))

		// Extend series as necessary (get underlying refs to
		// existing labels/samples slices).
		index := len(req.Series)
		req.Series = req.Series[:index+1]
		req.Series[index].Labels = labels
		req.Series[index].Samples = samples

		// Forward the label and sample ptrs
		labelsPtr = labelsPtr[len(labels):]
		samplesPtr = samplesPtr[len(samples):]
	}

	return req, nil
}

type pooledBatch struct {
	series  []WriteSeries
	labels  []Label
	samples []Sample
}

var (
	pooled = &struct {
		sync.Mutex
		series  [][]WriteSeries
		labels  [][]Label
		samples [][]Sample
	}{}
)

func parseGetPooled(series, labels, samples int) pooledBatch {
	var result pooledBatch

	pooled.Lock()
	for i, slice := range pooled.series {
		if cap(slice) >= series {
			result.series = slice[:0]
			if len(pooled.series) == 1 {
				pooled.series[0] = nil
				pooled.series = pooled.series[:0]
			} else {
				pooled.series[i] = pooled.series[len(pooled.series)-1]
				pooled.series[len(pooled.series)-1] = nil
				pooled.series = pooled.series[:len(pooled.series)-1]
			}
			break
		}
	}
	for i, slice := range pooled.labels {
		if cap(slice) >= labels {
			result.labels = slice[:0]
			if len(pooled.labels) == 1 {
				pooled.labels[0] = nil
				pooled.labels = pooled.labels[:0]
			} else {
				pooled.labels[i] = pooled.labels[len(pooled.labels)-1]
				pooled.labels[len(pooled.labels)-1] = nil
				pooled.labels = pooled.labels[:len(pooled.labels)-1]
			}
			break
		}
	}
	for i, slice := range pooled.samples {
		if cap(slice) >= samples {
			result.samples = slice[:0]
			if len(pooled.samples) == 1 {
				pooled.samples[0] = nil
				pooled.samples = pooled.samples[:0]
			} else {
				pooled.samples[i] = pooled.samples[len(pooled.samples)-1]
				pooled.samples[len(pooled.samples)-1] = nil
				pooled.samples = pooled.samples[:len(pooled.samples)-1]
			}
			break
		}
	}
	pooled.Unlock()

	growthRoom := 1.5
	if result.series == nil {
		result.series = make([]WriteSeries, 0, int(growthRoom*float64(series)))
	}
	if result.labels == nil {
		result.labels = make([]Label, 0, int(growthRoom*float64(labels)))
	}
	if result.samples == nil {
		result.samples = make([]Sample, 0, int(growthRoom*float64(samples)))
	}

	return result
}

func parsePutPooled(result pooledBatch) {
	pooled.Lock()
	if cap(result.series) > 0 {
		pooled.series = append(pooled.series, result.series)
	}
	if cap(result.labels) > 0 {
		pooled.labels = append(pooled.labels, result.labels)
	}
	if cap(result.samples) > 0 {
		pooled.samples = append(pooled.samples, result.samples)
	}
	pooled.Unlock()
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
