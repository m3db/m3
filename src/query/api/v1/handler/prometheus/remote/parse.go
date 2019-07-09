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

	"github.com/m3db/m3/src/x/mmap"

	"github.com/m3db/m3/src/dbnode/encoding/proto"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/storage"
	xhttp "github.com/m3db/m3/src/x/net/http"

	protobuf "github.com/golang/protobuf/proto"
)

const (
	maxParseByteBuffers = 2048
	maxWriteRequestPool = 2048
	writeStateBatchSize = 128
)

var errUnexpectedEOF = errors.New("unexpected EOF")

type Parser struct {
	bytesPool   *parseBytesPool
	statePool   *writeStateBatchPool
	requestPool *writeRequestPool
}

func NewParser() *Parser {
	p := &Parser{
		bytesPool: newParseBytesPool(),
		statePool: newWriteStateBatchPool(),
	}
	p.requestPool = newWriteRequestPool(p)
	return p
}

func (p *Parser) ParseWriteRequest(
	r *http.Request,
) (*WriteRequest, *xhttp.ParseError) {
	opts := prometheus.ParsePromCompressedRequestOptions{
		Alloc: p,
	}
	data, parseErr := prometheus.ParsePromCompressedRequest(r, opts)
	if parseErr != nil {
		return nil, parseErr
	}

	req := p.requestPool.getWriteRequest()
	err := req.reset(data)
	if err != nil {
		p.bytesPool.Put(data)
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return req, nil
}

func (p *Parser) AllocBytes(l int) []byte {
	return p.bytesPool.Get(l)
}

type WriteRequest struct {
	stats writeRequestStats

	data   []byte
	states []*writeStateBatch

	// iterator state
	idx           int
	samplesOffset int
	iterErr       error
	currLabels    []Label
	currSamples   []Sample

	rootBuff       *proto.Buffer
	innerBuff      *proto.Buffer
	innerInnerBuff *proto.Buffer
	rewriteBuff    *proto.Buffer

	// sorter (to avoid allocs to interface)
	sorter      sort.Interface
	labelSorter *labelSorter

	parser *Parser
}

type writeRequestStats struct {
	numSeries, numLabels, numSamples int
}

func newWriteRequest(parser *Parser) *WriteRequest {
	r := &WriteRequest{
		rootBuff:       proto.NewBuffer(nil),
		innerBuff:      proto.NewBuffer(nil),
		innerInnerBuff: proto.NewBuffer(nil),
		rewriteBuff:    proto.NewBuffer(nil),
		parser:         parser,
		labelSorter:    &labelSorter{},
	}
	r.sorter = r.labelSorter
	return r
}

func (r *WriteRequest) reset(data []byte) error {
	r.data = data

	var err error
	r.stats, err = r.parse()
	if err != nil {
		return err
	}

	numBatches := r.stats.numSeries / writeStateBatchSize
	if r.stats.numSeries%writeStateBatchSize != 0 {
		// Need an extra batch (thanks to integer division).
		numBatches++
	}

	r.states = r.states[:0]
	for i := 0; i < numBatches; i++ {
		r.states = append(r.states, r.parser.statePool.Get())
	}

	r.IterRestart()
	return nil
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
				if len(r.currLabels) > 0 {
					if !labelsOutOfOrder && bytes.Compare(label.Name, lastLabel.Name) < 0 {
						labelsOutOfOrder = true
					}
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
			r.labelSorter.values = r.currLabels
			// Call the already bound interface (avoiding alloc).
			sort.Sort(r.sorter)

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
					"could not reorder labels: expected_msg_len=%d, actual_msg_len=%d",
					msgLen, n)
			}
		}
	}

	return stats, nil
}

func (r *WriteRequest) Finalize() {
	// Return data to pools.
	r.parser.bytesPool.Put(r.data)
	for _, batch := range r.states {
		r.parser.statePool.Put(batch)
	}

	// Nil out refs.
	r.data = nil

	for i := range r.states {
		r.states[i] = nil
	}
	r.states = r.states[:0]

	emptyLabel := Label{}
	for i := range r.currLabels {
		r.currLabels[i] = emptyLabel
	}
	r.currLabels = r.currLabels[:0]

	emptySample := Sample{}
	for i := range r.currSamples {
		r.currSamples[i] = emptySample
	}
	r.currSamples = r.currSamples[:0]

	r.rootBuff.Reset(nil)
	r.innerBuff.Reset(nil)
	r.innerInnerBuff.Reset(nil)
	// Retain rewriteBuff

	// This is set to pre-existing slices, not reused.
	r.labelSorter.values = nil

	r.parser.requestPool.putWriteRequest(r)
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

var _ sort.Interface = &labelSorter{}

type labelSorter struct {
	values []Label
}

func (l *labelSorter) Len() int {
	return len(l.values)
}

func (l *labelSorter) Less(i, j int) bool {
	return bytes.Compare(l.values[i].Name, l.values[j].Name) < 0
}

func (l *labelSorter) Swap(i, j int) {
	l.values[i], l.values[j] = l.values[j], l.values[i]
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

type writeRequestPool struct {
	sync.Mutex
	parser *Parser
	values []*WriteRequest
}

func newWriteRequestPool(parser *Parser) *writeRequestPool {
	return &writeRequestPool{
		parser: parser,
		values: make([]*WriteRequest, 0, maxWriteRequestPool),
	}
}

func (p *writeRequestPool) getWriteRequest() *WriteRequest {
	var v *WriteRequest
	p.Lock()
	index := len(p.values) - 1
	if index >= 0 {
		v = p.values[index]
		p.values[index] = nil
		p.values = p.values[:index]
	}
	p.Unlock()
	if v == nil {
		v = newWriteRequest(p.parser)
	}
	return v
}

func (p *writeRequestPool) putWriteRequest(v *WriteRequest) {
	p.Lock()
	if len(p.values) < maxWriteRequestPool {
		p.values = append(p.values, v)
	}
	p.Unlock()
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

func (p *parseBytesPool) Get(l int) []byte {
	var result []byte
	p.Lock()
	n := p.heap.Len()
	if n > 0 {
		// Always return the largest.
		largest := heap.Pop(p.heap)
		result = largest.([]byte)
		if cap(result) < l {
			// If not big enough, just return immediately.
			heap.Push(p.heap, result)
		}
	}
	p.Unlock()

	if cap(result) < l {
		// Add a growth factor so it's more likely
		// it can be reused.
		allocLen := int64(1.5 * float64(l))

		// Need to allocate, use mmap to hide the memory.
		r, err := mmap.Bytes(allocLen, mmap.Options{
			Read:  true,
			Write: true,
		})
		if err != nil {
			result = make([]byte, 0, allocLen)
		} else {
			result = r.Result
		}
	}

	return result[:l]
}

func (p *parseBytesPool) Put(v []byte) {
	var removed []byte
	p.Lock()
	heap.Push(p.heap, v)
	for n := p.heap.Len(); n > maxParseByteBuffers; n = p.heap.Len() {
		// Remove the smallest buffer.
		removed = heap.Remove(p.heap, n-1).([]byte)
	}
	p.Unlock()
	if removed != nil {
		mmap.Munmap(removed)
	}
}

type bytesHeap [][]byte

func (h bytesHeap) Len() int           { return len(h) }
func (h bytesHeap) Less(i, j int) bool { return cap(h[i]) > cap(h[j]) }
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
