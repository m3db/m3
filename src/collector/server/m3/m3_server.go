// Copyright (c) 2018 Uber Technologies, Inc.
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

package m3

import (
	"bytes"
	"errors"
	"fmt"
	"net"
	"sync"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/uber-go/tally/m3"
	customtransport "github.com/uber-go/tally/m3/customtransports"
	m3thrift "github.com/uber-go/tally/m3/thrift"
)

const (
	protocol = m3.Compact
)

// Server is an M3 server.
type Server interface {
	Address() (string, error)
	Serve() error
	Close() error
}

var (
	errAlreadyClosed  = errors.New("already closed")
	errEncoderNoBytes = errors.New("tags encoder has no access to bytes")
)

type server struct {
	sync.RWMutex
	conn        *net.UDPConn
	processor   thrift.TProcessor
	reporter    reporter.Reporter
	encoderPool serialize.TagEncoderPool
	decoderPool serialize.TagDecoderPool
	tagOpts     models.TagOptions
	logger      *zap.Logger
	closed      bool
}

// NewServer returns a new M3 server.
func NewServer(
	listenAddr string,
	reporter reporter.Reporter,
	encoderPool serialize.TagEncoderPool,
	decoderPool serialize.TagDecoderPool,
	tagOpts models.TagOptions,
	logger *zap.Logger,
) (Server, error) {
	udpAddr, err := net.ResolveUDPAddr("udp", listenAddr)
	if err != nil {
		return nil, err
	}

	conn, err := net.ListenUDP(udpAddr.Network(), udpAddr)
	if err != nil {
		return nil, err
	}

	s := &server{
		conn:        conn,
		reporter:    reporter,
		encoderPool: encoderPool,
		decoderPool: decoderPool,
		tagOpts:     tagOpts,
		logger:      logger,
	}
	s.processor = m3thrift.NewM3Processor(newService(s.processBatch))
	return s, nil
}

func (s *server) Address() (string, error) {
	return s.conn.LocalAddr().String(), nil
}

func (s *server) Serve() error {
	readBuf := make([]byte, 65536)
	for {
		n, err := s.conn.Read(readBuf)
		if err != nil {
			s.RLock()
			closed := s.closed
			s.RUnlock()
			if !closed {
				return fmt.Errorf("failed to read: %v", err)
			}
			return nil
		}
		trans, _ := customtransport.NewTBufferedReadTransport(bytes.NewBuffer(readBuf[0:n]))
		proto := thrift.NewTCompactProtocol(trans)
		s.processor.Process(proto, proto)
	}
}

func (s *server) processBatch(batch *m3thrift.MetricBatch) {
	if batch == nil {
		return
	}

	for _, metric := range batch.Metrics {
		id, err := s.newMetricID(batch, metric)
		if err != nil {
			s.logger.Warn("could not construct metric ID", zap.Error(err))
			continue
		}

		if err := s.reportMetric(id, metric); err != nil {
			s.logger.Warn("could not report metric", zap.Error(err))
			continue
		}
	}
}

func (s *server) newMetricID(
	batch *m3thrift.MetricBatch,
	metric *m3thrift.Metric,
) (id.ID, error) {
	numTags := len(batch.CommonTags) + len(metric.Tags)
	tags := models.NewTags(numTags, s.tagOpts)
	for tag := range metric.Tags {
		n, v := tag.TagName, tag.TagValue
		if v != nil {
			tags = tags.AddTag(models.Tag{Name: []byte(n), Value: []byte(*v)})
		}
	}
	tagsIter := storage.TagsToIdentTagIterator(tags)

	encoder := s.encoderPool.Get()
	encoder.Reset()
	defer encoder.Finalize()

	if err := encoder.Encode(tagsIter); err != nil {
		return nil, err
	}

	data, ok := encoder.Data()
	if !ok {
		return nil, errEncoderNoBytes
	}

	// Take a copy of the pooled encoder's bytes
	bytes := append([]byte(nil), data.Bytes()...)

	metricTagsIter := serialize.NewMetricTagsIterator(s.decoderPool.Get(), nil)
	metricTagsIter.Reset(bytes)
	return metricTagsIter, nil
}

func (s *server) reportMetric(id id.ID, metric *m3thrift.Metric) error {
	var err error
	switch {
	case metric.MetricValue != nil &&
		metric.MetricValue.Count != nil &&
		metric.MetricValue.Count.I64Value != nil:

		value := *metric.MetricValue.Count.I64Value
		err = s.reporter.ReportCounter(id, value)

	case metric.MetricValue != nil &&
		metric.MetricValue.Gauge != nil &&
		metric.MetricValue.Gauge.I64Value != nil:

		value := *metric.MetricValue.Gauge.I64Value
		err = s.reporter.ReportGauge(id, float64(value))

	case metric.MetricValue != nil &&
		metric.MetricValue.Gauge != nil &&
		metric.MetricValue.Gauge.DValue != nil:

		value := *metric.MetricValue.Gauge.DValue
		err = s.reporter.ReportGauge(id, value)

	case metric.MetricValue != nil &&
		metric.MetricValue.Timer != nil &&
		metric.MetricValue.Timer.I64Value != nil:

		value := *metric.MetricValue.Timer.I64Value
		err = s.reporter.ReportBatchTimer(id, []float64{float64(value)})

	case metric.MetricValue != nil &&
		metric.MetricValue.Timer != nil &&
		metric.MetricValue.Timer.DValue != nil:

		value := *metric.MetricValue.Timer.DValue
		err = s.reporter.ReportBatchTimer(id, []float64{value})

	default:
		err = fmt.Errorf("unknown metric type")
	}
	return err
}

func (s *server) Close() error {
	s.Lock()
	defer s.Unlock()

	if s.closed {
		return errAlreadyClosed
	}

	s.closed = true

	return s.conn.Close()
}

type batchCallback func(batch *m3thrift.MetricBatch)

type service struct {
	fn batchCallback
}

func newService(fn batchCallback) *service {
	return &service{fn: fn}
}

func (m *service) EmitMetricBatch(batch *m3thrift.MetricBatch) (err error) {
	m.fn(batch)
	return thrift.NewTTransportException(thrift.END_OF_FILE, "complete")
}
