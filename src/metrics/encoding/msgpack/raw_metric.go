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
	"io"

	"github.com/m3db/m3/src/metrics/metric/aggregated"
	"github.com/m3db/m3/src/metrics/metric/id"
)

var (
	emptyMetric aggregated.Metric
)

type readBytesFn func(start int, n int) []byte

// rawMetric is a raw metric.
type rawMetric struct {
	data         []byte            // raw data containing encoded metric.
	it           iteratorBase      // base iterator for lazily decoding metric fields.
	metric       aggregated.Metric // current metric.
	idDecoded    bool              // whether id has been decoded.
	timeDecoded  bool              // whether time has been decoded.
	valueDecoded bool              // whether value has been decoded.
	readBytesFn  readBytesFn       // reading bytes function.
}

// NewRawMetric creates a new raw metric.
func NewRawMetric(data []byte, readerBufferSize int) aggregated.RawMetric {
	reader := bytes.NewReader(data)
	m := &rawMetric{
		data: data,
		it:   newBaseIterator(reader, readerBufferSize),
	}
	m.readBytesFn = m.readBytes
	return m
}

func (m *rawMetric) ID() (id.RawID, error) {
	m.decodeID()
	if err := m.it.err(); err != nil {
		return nil, err
	}
	return m.metric.ID, nil
}

func (m *rawMetric) TimeNanos() (int64, error) {
	m.decodeID()
	m.decodeTime()
	if err := m.it.err(); err != nil {
		return 0, err
	}
	return m.metric.TimeNanos, nil
}

func (m *rawMetric) Value() (float64, error) {
	m.decodeID()
	m.decodeTime()
	m.decodeValue()
	if err := m.it.err(); err != nil {
		return 0.0, err
	}
	return m.metric.Value, nil
}

func (m *rawMetric) Metric() (aggregated.Metric, error) {
	m.decodeID()
	m.decodeTime()
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
	m.metric = emptyMetric
	m.idDecoded = false
	m.timeDecoded = false
	m.valueDecoded = false
	m.data = data
	m.reader().Reset(data)
	m.it.reset(m.reader())
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
	// NB(xichen): DecodeBytesLen() returns -1 if the byte slice is nil.
	if idLen == -1 {
		m.metric.ID = nil
		m.idDecoded = true
		return
	}
	numRead := len(m.data) - m.reader().Len()
	m.metric.ID = m.readBytesFn(numRead, idLen)
	if m.it.err() != nil {
		return
	}
	m.idDecoded = true
}

func (m *rawMetric) decodeTime() {
	if m.it.err() != nil || m.timeDecoded {
		return
	}
	timeNanos := m.it.decodeVarint()
	if m.it.err() != nil {
		return
	}
	m.metric.TimeNanos = timeNanos
	m.timeDecoded = true
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

func (m *rawMetric) reader() *bytes.Reader {
	return m.it.reader().(*bytes.Reader)
}

func (m *rawMetric) readBytes(start int, n int) []byte {
	numBytes := len(m.data)
	if n < 0 || start < 0 || start+n > numBytes {
		err := fmt.Errorf("invalid start %d and length %d, numBytes=%d", start, n, numBytes)
		m.it.setErr(err)
		return nil
	}
	// Advance the internal buffer index.
	_, err := m.reader().Seek(int64(n), io.SeekCurrent)
	if err != nil {
		m.it.setErr(err)
		return nil
	}
	return m.data[start : start+n]
}
