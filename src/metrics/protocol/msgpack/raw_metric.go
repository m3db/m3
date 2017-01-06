// Copyright (c) 2016 Uber Technologies, Inc.
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

package msgpack

import (
	"bytes"
	"fmt"
	"time"

	"github.com/m3db/m3metrics/metric"
	"github.com/m3db/m3metrics/metric/aggregated"
)

var (
	emptyMetric aggregated.Metric
)

type readBytesFn func(start int, n int) []byte

// rawMetric is a raw metric
type rawMetric struct {
	data             []byte            // raw data containing encoded metric
	buf              *bytes.Buffer     // intermediate buffer
	it               iteratorBase      // base iterator for lazily decoding metric fields
	metric           aggregated.Metric // current metric
	idDecoded        bool              // whether id has been decoded
	timestampDecoded bool              // whether timestamp has been decoded
	valueDecoded     bool              // whether value has been decoded
	readBytesFn      readBytesFn       // reading bytes function
}

// NewRawMetric creates a new raw metric
func NewRawMetric(data []byte) aggregated.RawMetric {
	buf := bytes.NewBuffer(data)
	m := &rawMetric{
		data: data,
		buf:  buf,
		it:   newBaseIterator(buf),
	}

	m.readBytesFn = m.readBytes

	return m
}

func (m *rawMetric) ID() (metric.ID, error) {
	m.decodeID()
	if err := m.it.err(); err != nil {
		return nil, err
	}
	return m.metric.ID, nil
}

func (m *rawMetric) Timestamp() (time.Time, error) {
	m.decodeID()
	m.decodeTimestamp()
	if err := m.it.err(); err != nil {
		return time.Time{}, err
	}
	return m.metric.Timestamp, nil
}

func (m *rawMetric) Value() (float64, error) {
	m.decodeID()
	m.decodeTimestamp()
	m.decodeValue()
	if err := m.it.err(); err != nil {
		return 0.0, err
	}
	return m.metric.Value, nil
}

func (m *rawMetric) Metric() (aggregated.Metric, error) {
	m.decodeID()
	m.decodeTimestamp()
	m.decodeValue()
	if err := m.it.err(); err != nil {
		return emptyMetric, err
	}
	return m.metric, nil
}

func (m *rawMetric) Bytes() []byte {
	return m.data
}

func (m *rawMetric) Reset(data []byte) {
	m.data = data
	m.metric = emptyMetric
	m.idDecoded = false
	m.timestampDecoded = false
	m.valueDecoded = false
	m.buf.Reset()
	_, err := m.buf.Write(data)
	m.it.setErr(err)
}

// NB(xichen): decodeID decodes the ID without making a copy
// of the bytes stored in the buffer. The decoded ID is a slice
// of the internal buffer, which remains valid until the buffered
// data become invalid (e.g. when Reset() is called).
func (m *rawMetric) decodeID() {
	if m.it.err() != nil || m.idDecoded {
		return
	}
	version := m.it.decodeVersion()
	if m.it.err() != nil {
		return
	}
	if version > metricVersion {
		err := fmt.Errorf("metric version received %d is higher than supported version %d", version, metricVersion)
		m.it.setErr(err)
		return
	}
	_, _, ok := m.it.checkNumFieldsForType(metricType)
	if !ok {
		return
	}
	idLen := m.it.decodeBytesLen()
	if m.it.err() != nil {
		return
	}
	numRead := len(m.data) - m.buf.Len()
	m.metric.ID = m.readBytesFn(numRead, idLen)
	if m.it.err() != nil {
		return
	}
	m.idDecoded = true
}

func (m *rawMetric) decodeTimestamp() {
	if m.it.err() != nil || m.timestampDecoded {
		return
	}
	t := m.it.decodeTime()
	if m.it.err() != nil {
		return
	}
	m.metric.Timestamp = t
	m.timestampDecoded = true
}

func (m *rawMetric) decodeValue() {
	if m.it.err() != nil || m.valueDecoded {
		return
	}
	v := m.it.decodeFloat64()
	if m.it.err() != nil {
		return
	}
	m.metric.Value = v
	m.valueDecoded = true
}

func (m *rawMetric) readBytes(start int, n int) []byte {
	numBytes := len(m.data)
	if n < 0 || start < 0 || start+n > numBytes {
		err := fmt.Errorf("invalid start %d and length %d, numBytes=%d", start, n, numBytes)
		m.it.setErr(err)
		return nil
	}
	// Advance the internal buffer index
	if numRead := len(m.buf.Next(n)); numRead != n {
		err := fmt.Errorf("num bytes read %d doesn't match target length %d", numRead, n)
		m.it.setErr(err)
		return nil
	}
	return m.data[start : start+n]
}
