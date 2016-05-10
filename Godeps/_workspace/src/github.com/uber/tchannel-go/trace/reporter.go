// Copyright (c) 2015 Uber Technologies, Inc.

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

// Package trace provides methods to submit Zipkin style spans to TCollector.
package trace

import (
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"net"
	"strconv"
	"time"

	tc "github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/thrift"
	"github.com/uber/tchannel-go/trace/thrift/gen-go/tcollector"
)

const (
	tcollectorServiceName = "tcollector"
	chanBufferSize        = 100
)

// TCollectorReporter is a trace reporter that submits trace spans to TCollector.
type TCollectorReporter struct {
	tchannel  *tc.Channel
	client    tcollector.TChanTCollector
	curHostIP uint32
	c         chan tc.TraceData
	logger    tc.Logger
}

// NewTCollectorReporter return trace reporter that submits span to TCollector.
func NewTCollectorReporter(ch *tc.Channel) *TCollectorReporter {
	thriftClient := thrift.NewClient(ch, tcollectorServiceName, nil)
	client := tcollector.NewTChanTCollectorClient(thriftClient)

	curHostIP, err := tc.ListenIP()
	if err != nil {
		ch.Logger().WithFields(tc.ErrField(err)).Warn("TCollector TraceReporter failed to get IP.")
		curHostIP = net.IPv4(0, 0, 0, 0)
	}

	// create the goroutine method to actually to the submit Span.
	reporter := &TCollectorReporter{
		tchannel:  ch,
		client:    client,
		c:         make(chan tc.TraceData, chanBufferSize),
		logger:    ch.Logger(),
		curHostIP: inetAton(curHostIP.String()),
	}
	go reporter.worker()
	return reporter
}

// Report method will submit trace span to tcollector server.
func (r *TCollectorReporter) Report(data tc.TraceData) {
	select {
	case r.c <- data:
	default:
		r.logger.Infof("TCollectorReporter: buffer channel for trace is full")
	}
}

func (r *TCollectorReporter) report(data *tc.TraceData) error {
	ctx, cancel := tc.NewContextBuilder(time.Second).
		DisableTracing().
		SetRetryOptions(&tc.RetryOptions{RetryOn: tc.RetryNever}).
		SetShardKey(base64Encode(data.Span.TraceID())).Build()
	defer cancel()

	thriftSpan, err := buildSpan(data, r.curHostIP)
	if err != nil {
		return err
	}
	// client submit
	_, err = r.client.Submit(ctx, thriftSpan)
	return err
}

func (r *TCollectorReporter) worker() {
	for data := range r.c {
		if err := r.report(&data); err != nil {
			r.logger.Infof("Span report failed: %v", err)
		}
	}
}

func buildEndpoint(ep tc.TraceEndpoint) (*tcollector.Endpoint, error) {
	host, portStr, err := net.SplitHostPort(ep.HostPort)
	if err != nil {
		return nil, err
	}

	port, err := strconv.Atoi(portStr)
	if err != nil {
		return nil, err
	}

	return &tcollector.Endpoint{
		Ipv4:        int32(inetAton(host)),
		Port:        int32(port),
		ServiceName: ep.ServiceName,
	}, nil
}

// buildSpan builds a tcollector span based on tchannel span.
func buildSpan(data *tc.TraceData, curHostIP uint32) (*tcollector.Span, error) {
	source, err := buildEndpoint(data.Source)
	if err != nil {
		return nil, err
	}
	if source.Ipv4 == 0 {
		source.Ipv4 = int32(curHostIP)
	}

	target, err := buildEndpoint(data.Target)
	if err != nil {
		return nil, err
	}

	binaryAnns, err := buildBinaryAnnotations(data.BinaryAnnotations)
	if err != nil {
		return nil, err
	}
	thriftSpan := tcollector.Span{
		TraceId:           uint64ToBytes(data.Span.TraceID()),
		SpanHost:          source,
		Host:              target,
		Name:              data.Method,
		ID:                uint64ToBytes(data.Span.SpanID()),
		ParentId:          uint64ToBytes(data.Span.ParentID()),
		Annotations:       buildAnnotations(data.Annotations),
		BinaryAnnotations: binaryAnns,
	}

	return &thriftSpan, nil
}

func buildBinaryAnnotation(ann tc.BinaryAnnotation) (*tcollector.BinaryAnnotation, error) {
	bann := &tcollector.BinaryAnnotation{Key: ann.Key}
	switch v := ann.Value.(type) {
	case bool:
		bann.AnnotationType = tcollector.AnnotationType_BOOL
		bann.BoolValue = &v
	case int64:
		bann.AnnotationType = tcollector.AnnotationType_I64
		temp := v
		bann.IntValue = &temp
	case int32:
		bann.AnnotationType = tcollector.AnnotationType_I32
		temp := int64(v)
		bann.IntValue = &temp
	case int16:
		bann.AnnotationType = tcollector.AnnotationType_I16
		temp := int64(v)
		bann.IntValue = &temp
	case int:
		bann.AnnotationType = tcollector.AnnotationType_I32
		temp := int64(v)
		bann.IntValue = &temp
	case string:
		bann.AnnotationType = tcollector.AnnotationType_STRING
		bann.StringValue = &v
	case []byte:
		bann.AnnotationType = tcollector.AnnotationType_BYTES
		bann.BytesValue = v
	case float32:
		bann.AnnotationType = tcollector.AnnotationType_DOUBLE
		temp := float64(v)
		bann.DoubleValue = &temp
	case float64:
		bann.AnnotationType = tcollector.AnnotationType_DOUBLE
		temp := float64(v)
		bann.DoubleValue = &temp
	default:
		return nil, fmt.Errorf("unrecognized data type: %T", v)
	}
	return bann, nil
}

func buildBinaryAnnotations(anns []tc.BinaryAnnotation) ([]*tcollector.BinaryAnnotation, error) {
	binaryAnns := make([]*tcollector.BinaryAnnotation, len(anns))
	for i, ann := range anns {
		b, err := buildBinaryAnnotation(ann)
		binaryAnns[i] = b
		if err != nil {
			return nil, err
		}
	}
	return binaryAnns, nil
}

func buildAnnotations(anns []tc.Annotation) []*tcollector.Annotation {
	tAnns := make([]*tcollector.Annotation, len(anns))
	for i, ann := range anns {
		tAnns[i] = &tcollector.Annotation{
			Timestamp: float64(ann.Timestamp.UnixNano() / 1e6),
			Value:     string(ann.Key),
		}
	}
	return tAnns
}

// inetAton converts string Ipv4 to uint32
func inetAton(ip string) uint32 {
	ipBytes := net.ParseIP(ip).To4()
	return binary.BigEndian.Uint32(ipBytes)
}

// base64Encode encodes uint64 with base64 StdEncoding.
func base64Encode(data uint64) string {
	return base64.StdEncoding.EncodeToString(uint64ToBytes(data))
}

// uint64ToBytes converts uint64 to bytes.
func uint64ToBytes(i uint64) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(i))
	return buf
}

// TCollectorReporterFactory builds TCollectorReporter using a given TChannel instance.
func TCollectorReporterFactory(tchannel *tc.Channel) tc.TraceReporter {
	return NewTCollectorReporter(tchannel)
}
