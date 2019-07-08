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
	"container/heap"
	"errors"
	"fmt"
	"math"
	"net/http"
	"sort"
	"sync"

	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"

	protobuf "github.com/golang/protobuf/proto"
)

const (
	maxParseByteBuffers = 4096
	writeStateBatchSize = 128
)

var errUnexpectedEOF = errors.New("unexpected EOF")

type Parser struct {
	bytesPool *parseBytesPool
	statePool *writeStateBatchPool
}

func NewParser() *Parser {
	return &Parser{
		bytesPool: newParseBytesPool(),
		statePool: newWriteStateBatchPool(),
	}
}

func (p *Parser) ParseWriteRequest(
	r *http.Request,
) (*WriteRequest, *xhttp.ParseError) {
	parseBuff := p.bytesPool.Get()
	data, parseErr := prometheus.ParsePromCompressedRequest(parseBuff, r)
	if parseErr != nil {
		p.bytesPool.Put(parseBuff)
		return nil, parseErr
	}

	req, err := newWriteRequest(p, data)
	if err != nil {
		p.bytesPool.Put(data)
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	numBatches := req.stats.numSeries / writeStateBatchSize
	if req.stats.numSeries%writeStateBatchSize != 0 {
		// Need an extra batch (thanks to integer division).
		numBatches++
	}

	req.states = make([]*writeStateBatch, 0, numBatches)
	for i := 0; i < numBatches; i++ {
		req.states = append(req.states, p.statePool.Get())
	}

	return req, nil
}

type WriteRequest struct {
	stats writeRequestStats

	data   []byte
	states []*writeStateBatch

	// iterator state
	idx            int
	samplesOffset  int
	iterErr        error
	currSeriesData []byte
	currLabels     []Label
	currSamples    []Sample

	rootBuff       *proto.Buffer
	innerBuff      *proto.Buffer
	innerInnerBuff *proto.Buffer
	rewriteBuff    *proto.Buffer
	rewriteBytes   []byte

	parser *Parser
}

type writeRequestStats struct {
	numSeries, numLabels, numSamples int
}

func newWriteRequest(
	parser *Parser,
	data []byte,
) (*WriteRequest, error) {
	w := &WriteRequest{
		data:           data,
		idx:            -1,
		rootBuff:       proto.NewBuffer(nil),
		innerBuff:      proto.NewBuffer(nil),
		innerInnerBuff: proto.NewBuffer(nil),
		rewriteBuff:    proto.NewBuffer(nil),
		parser:         parser,
	}
	var err error
	w.stats, err = w.parse()
	if err != nil {
		return nil, err
	}
	w.IterRestart()
	return w, nil
}

type statePos struct {
	batchIdx int
	elemIdx  int
}

func (r *WriteRequest) statePos(relativeSampleIdx int) statePos {
	sampleIdx := r.samplesOffset - len(r.currSamples) + relativeSampleIdx
	return statePos{
		batchIdx: sampleIdx / writeStateBatchSize,
		elemIdx:  sampleIdx % writeStateBatchSize,
	}
}

func (r *WriteRequest) DatapointResult(
	datapointIdx int,
) storage.WriteQueryResult {
	pos := r.statePos(datapointIdx)
	return r.states[pos.batchIdx].values[pos.elemIdx].result
}

func (r *WriteRequest) DatapointState(
	datapointIdx int,
) interface{} {
	pos := r.statePos(datapointIdx)
	return r.states[pos.batchIdx].values[pos.elemIdx].state
}

func (r *WriteRequest) SetDatapointResult(
	datapointIdx int,
	result storage.WriteQueryResult,
) {
	pos := r.statePos(datapointIdx)
	r.states[pos.batchIdx].values[pos.elemIdx].result = result
}

func (r *WriteRequest) SetDatapointState(
	datapointIdx int,
	state interface{},
) {
	pos := r.statePos(datapointIdx)
	r.states[pos.batchIdx].values[pos.elemIdx].state = state
}

func (r *WriteRequest) Len() int {
	return r.stats.numSeries
}

func (r *WriteRequest) IterRestart() {
	r.idx = -1
	r.samplesOffset = 0
	r.rootBuff.Reset(r.data)
	r.innerBuff.Reset(nil)
	r.innerInnerBuff.Reset(nil)
}

func (r *WriteRequest) IterCurr() ([]Label, []Sample) {
	return r.currLabels, r.currSamples
}

func (r *WriteRequest) IterErr() error {
	return r.iterErr
}

func (r *WriteRequest) IterNext() bool {
	if r.iterErr != nil {
		return false
	}

	r.idx++
	next := r.idx < r.stats.numSeries
	if !next {
		return false
	}

	if r.rootBuff.EOF() {
		r.iterErr = errUnexpectedEOF
		return false
	}

	fieldNum, wireType, err := r.rootBuff.DecodeTagAndWireType()
	if err != nil {
		r.iterErr = fmt.Errorf(
			"decoded write request message error: %v", err)
		return false
	}

	if fieldNum != 1 {
		r.iterErr = fmt.Errorf(
			"expecting time series field: actual=%d", fieldNum)
		return false
	}
	if wireType != protobuf.WireBytes {
		r.iterErr = fmt.Errorf(
			"expecting time series message wire type: actual=%d", wireType)
		return false
	}

	inner, err := r.rootBuff.DecodeRawBytes(false)
	if err != nil {
		r.iterErr = fmt.Errorf(
			"decode time series message error: %v", err)
		return false
	}

	r.currSeriesData = inner
	r.currLabels = r.currLabels[:0]
	r.currSamples = r.currSamples[:0]

	r.innerBuff.Reset(inner)
	for !r.innerBuff.EOF() {
		fieldNum, wireType, err := r.innerBuff.DecodeTagAndWireType()
		if err != nil {
			r.iterErr = fmt.Errorf(
				"decoded time series message field error: %v", err)
			return false
		}

		switch fieldNum {
		case 1:
			// Decode label.
			if wireType != protobuf.WireBytes {
				r.iterErr = fmt.Errorf(
					"expecting label message wire type: actual=%d", wireType)
				return false
			}

			innerInner, err := r.innerBuff.DecodeRawBytes(false)
			if err != nil {
				r.iterErr = fmt.Errorf(
					"decode label message error: %v", err)
				return false
			}

			var label Label

			r.innerInnerBuff.Reset(innerInner)
			for !r.innerInnerBuff.EOF() {
				fieldNum, wireType, err := r.innerInnerBuff.DecodeTagAndWireType()
				if err != nil {
					r.iterErr = fmt.Errorf(
						"decoded label message field error: %v", err)
					return false
				}

				switch fieldNum {
				case 1:
					if wireType != protobuf.WireBytes {
						r.iterErr = fmt.Errorf(
							"expecting label name message wire type: actual=%d", wireType)
						return false
					}

					label.Name, err = r.innerInnerBuff.DecodeRawBytes(false)
					if err != nil {
						r.iterErr = fmt.Errorf(
							"decode label name message error: %v", err)
						return false
					}
				case 2:
					if wireType != protobuf.WireBytes {
						r.iterErr = fmt.Errorf(
							"expecting label value message wire type: actual=%d", wireType)
						return false
					}

					label.Value, err = r.innerInnerBuff.DecodeRawBytes(false)
					if err != nil {
						r.iterErr = fmt.Errorf(
							"decode label value message error: %v", err)
						return false
					}
				default:
					r.iterErr = fmt.Errorf(
						"decode label unknown field: %d", fieldNum)
					return false
				}
			}

			r.currLabels = append(r.currLabels, label)
		case 2:
			// Decode sample.
			if wireType != protobuf.WireBytes {
				r.iterErr = fmt.Errorf(
					"expecting sample message wire type: actual=%d", wireType)
				return false
			}

			innerInner, err := r.innerBuff.DecodeRawBytes(false)
			if err != nil {
				r.iterErr = fmt.Errorf(
					"decode sample message error: %v", err)
				return false
			}

			var sample Sample

			r.innerInnerBuff.Reset(innerInner)
			for !r.innerInnerBuff.EOF() {
				fieldNum, wireType, err := r.innerInnerBuff.DecodeTagAndWireType()
				if err != nil {
					r.iterErr = fmt.Errorf(
						"decoded sample message field error: %v", err)
					return false
				}

				switch fieldNum {
				case 1:
					if wireType != protobuf.WireFixed64 {
						r.iterErr = fmt.Errorf(
							"expecting sample value int64 wire type: actual=%d", wireType)
						return false
					}

					var valueBits uint64
					valueBits, err = r.innerInnerBuff.DecodeFixed64()
					if err != nil {
						r.iterErr = fmt.Errorf(
							"decode sample value message error: %v", err)
						return false
					}

					sample.Value = math.Float64frombits(valueBits)

				case 2:
					if wireType != protobuf.WireVarint {
						r.iterErr = fmt.Errorf(
							"expecting sample timestamp varint wire type: actual=%d", wireType)
						return false
					}

					var ts uint64
					ts, err = r.innerInnerBuff.DecodeVarint()
					if err != nil {
						r.iterErr = fmt.Errorf(
							"decode sample timestamp message error: %v", err)
						return false
					}

					sample.TimeUnixMillis = int64(ts)
				default:
					r.iterErr = fmt.Errorf(
						"decode sample unknown field: %d", fieldNum)
					return false
				}
			}

			r.currSamples = append(r.currSamples, sample)
		default:
			r.iterErr = fmt.Errorf(
				"decode time series unknown field: %d", fieldNum)
			return false
		}
	}

	r.samplesOffset += len(r.currSamples)
	return true
}

func (r *WriteRequest) parse() (writeRequestStats, error) {
	// Inline all for fastest upfront parsing.
	var (
		stats          writeRequestStats
		rootBuff       = r.rootBuff
		innerBuff      = r.innerBuff
		innerInnerBuff = r.innerInnerBuff
	)

	rootBuff.Reset(r.data)
	for !rootBuff.EOF() {
		fieldNum, wireType, err := rootBuff.DecodeTagAndWireType()
		if err != nil {
			return stats, err
		}

		if fieldNum != 1 {
			return stats, fmt.Errorf("expecting only time series field")
		}
		if wireType != protobuf.WireBytes {
			return stats, fmt.Errorf("expecting series message wire type")
		}

		inner, err := rootBuff.DecodeRawBytes(false)
		if err != nil {
			return stats, err
		}

		stats.numSeries++

		// We are going to check the labels are in order and reorder if not.
		var (
			lastLabel        Label
			labelsOutOfOrder bool
			seriesBuff       = inner
		)
		r.currLabels = r.currLabels[:0]
		r.currSamples = r.currSamples[:0]

		innerBuff.Reset(inner)
		for !innerBuff.EOF() {
			fieldNum, wireType, err := innerBuff.DecodeTagAndWireType()
			if err != nil {
				return stats, err
			}

			switch fieldNum {
			case 1:
				// Decode label.
				if wireType != protobuf.WireBytes {
					return stats, fmt.Errorf("expecting label message wire type")
				}

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return stats, err
				}

				label := Label{
					messageBytes: innerInner,
				}
				stats.numLabels++

				innerInnerBuff.Reset(innerInner)
				for !innerInnerBuff.EOF() {
					fieldNum, wireType, err := innerInnerBuff.DecodeTagAndWireType()
					if err != nil {
						return stats, err
					}

					switch fieldNum {
					case 1:
						if wireType != protobuf.WireBytes {
							return stats, fmt.Errorf("expecting label name bytes wire type")
						}

						label.Name, err = r.innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return stats, err
						}
					case 2:
						if wireType != protobuf.WireBytes {
							return stats, fmt.Errorf("expecting label name bytes wire type")
						}

						label.Value, err = r.innerInnerBuff.DecodeRawBytes(false)
						if err != nil {
							return stats, err
						}
					default:
						return stats, fmt.Errorf("unknown label field: %v", fieldNum)
					}
				}

				// Check if out of order and track if so.
				if len(r.currLabels) > 0 &&
					!labelsOutOfOrder &&
					bytes.Compare(label.Name, lastLabel.Name) < 0 {
					labelsOutOfOrder = true
				}
				lastLabel = label

				r.currLabels = append(r.currLabels, label)
			case 2:
				// Decode sample.
				if wireType != protobuf.WireBytes {
					return stats, fmt.Errorf("expecting label message wire type")
				}

				innerInner, err := innerBuff.DecodeRawBytes(false)
				if err != nil {
					return stats, err
				}

				sample := Sample{
					messageBytes: innerInner,
				}
				stats.numSamples++

				innerInnerBuff.Reset(innerInner)
				for !innerInnerBuff.EOF() {
					fieldNum, wireType, err := innerInnerBuff.DecodeTagAndWireType()
					if err != nil {
						return stats, err
					}

					switch fieldNum {
					case 1:
						if wireType != protobuf.WireFixed64 {
							return stats, fmt.Errorf("expecting sample value wire type")
						}

						_, err = innerInnerBuff.DecodeFixed64()
						if err != nil {
							return stats, err
						}
					case 2:
						if wireType != protobuf.WireVarint {
							return stats, fmt.Errorf("expecting sample timestamp wire type")
						}

						_, err = innerInnerBuff.DecodeVarint()
						if err != nil {
							return stats, err
						}
					default:
						return stats, fmt.Errorf("unknown sample field: %v", fieldNum)
					}
				}

				r.currSamples = append(r.currSamples, sample)
			default:
				return stats, fmt.Errorf("unknown series field: %v", fieldNum)
			}
		}

		if labelsOutOfOrder {
			// Need to reorder the labels, order them then write out all the submessages.
			sort.Sort(labelsByName(r.currLabels))

			// Need to rewrite this series message, but can't use direct ref to
			// bytes or will overwrite the very thing we are writing out.
			writeBuff := r.rewriteBuff
			writeBuff.ResetEncode()

			// Write out labels.
			for i := range r.currLabels {
				writeBuff.EncodeTagAndWireType(1, protobuf.WireBytes)
				writeBuff.EncodeRawBytes(r.currLabels[i].messageBytes)
			}

			// Write out samples.
			for i := range r.currSamples {
				writeBuff.EncodeTagAndWireType(2, protobuf.WireBytes)
				writeBuff.EncodeRawBytes(r.currSamples[i].messageBytes)
			}

			msgLen := len(seriesBuff)
			if n := copy(seriesBuff, writeBuff.Bytes()); n != msgLen {
				return stats, fmt.Errorf(
					"could not reorder labels: expected_msg_len=%d, actual_msg_len",
					msgLen, n)
			}
		}
	}

	return stats, nil
}

func (p *WriteRequest) Finalize() {
	// Return data to pools.
	p.parser.bytesPool.Put(p.data)
	for _, batch := range p.states {
		p.parser.statePool.Put(batch)
	}

	// Zero refs.
	*p = WriteRequest{}
}

type WriteSeries struct {
	Labels  []Label
	Samples []Sample
}

type Label struct {
	Name  []byte
	Value []byte

	messageBytes []byte
}

type Sample struct {
	TimeUnixMillis int64
	Value          float64

	Result storage.WriteQueryResult
	State  interface{}

	messageBytes []byte
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

type writeState struct {
	result storage.WriteQueryResult
	state  interface{}
}

type writeStateBatch struct {
	values [writeStateBatchSize]writeState
}

func newWriteStateBatch() *writeStateBatch {
	return &writeStateBatch{}
}

type writeStateBatchPool struct {
	pool sync.Pool
}

func newWriteStateBatchPool() *writeStateBatchPool {
	return &writeStateBatchPool{
		pool: sync.Pool{
			New: func() interface{} {
				return newWriteStateBatch()
			},
		},
	}
}

func (p *writeStateBatchPool) Get() *writeStateBatch {
	return p.pool.Get().(*writeStateBatch)
}

func (p *writeStateBatchPool) Put(v *writeStateBatch) {
	// Memset optimization.
	empty := writeState{}
	for i := range v.values {
		v.values[i] = empty
	}
	p.pool.Put(v)
}

type parseBytesPool struct {
	sync.Mutex
	heap *bytesHeap
}

func newParseBytesPool() *parseBytesPool {
	p := &parseBytesPool{heap: &bytesHeap{}}
	heap.Init(p.heap)
	return p
}

func (p *parseBytesPool) Get() []byte {
	var result []byte
	p.Lock()
	count := p.heap.Len()
	if count > 0 {
		// Always return the largest.
		largest := heap.Remove(p.heap, count-1)
		result = largest.([]byte)
	}
	p.Unlock()
	return result[:0]
}

func (p *parseBytesPool) Put(v []byte) {
	p.Lock()
	heap.Push(p.heap, v)
	count := p.heap.Len()
	if count > maxParseByteBuffers {
		// Remove two at once, largest and smallest
		// to keep the "middle" sized buffers.
		heap.Remove(p.heap, count-1)
		heap.Pop(p.heap)
	}
	p.Unlock()
}

type bytesHeap [][]byte

func (h bytesHeap) Len() int           { return len(h) }
func (h bytesHeap) Less(i, j int) bool { return cap(h[i]) < cap(h[j]) }
func (h bytesHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *bytesHeap) Push(x interface{}) {
	// Push and Pop use pointer receivers because they modify the slice's length,
	// not just its contents.
	*h = append(*h, x.([]byte))
}

func (h *bytesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
