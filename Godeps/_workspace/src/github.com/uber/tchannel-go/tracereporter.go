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

package tchannel

import (
	"log"
	"time"
)

// When reporting spans, we report:
// bunch of "binary" annotations (eg

// AnnotationKey is the key for annotations.
type AnnotationKey string

// Known annotation keys
const (
	AnnotationKeyClientSend    = "cs"
	AnnotationKeyClientReceive = "cr"
	AnnotationKeyServerSend    = "ss"
	AnnotationKeyServerReceive = "sr"
)

// TraceEndpoint represents a service endpoint.
type TraceEndpoint struct {
	HostPort    string
	ServiceName string
}

// TraceData is the data reported to the TracerReporter.
type TraceData struct {
	Span              Span
	Annotations       []Annotation
	BinaryAnnotations []BinaryAnnotation
	Source            TraceEndpoint
	Target            TraceEndpoint
	Method            string
}

// BinaryAnnotation is additional context information about the span.
type BinaryAnnotation struct {
	Key string
	// Value contains one of: string, float64, bool, []byte, int64
	Value interface{}
}

// Annotation represents a specific event and the timestamp at which it occurred.
type Annotation struct {
	Key       AnnotationKey
	Timestamp time.Time
}

// TraceReporter is the interface used to report Trace spans.
type TraceReporter interface {
	// Report method is intended to report Span information.
	Report(data TraceData)
}

// TraceReporterFunc allows using a function as a TraceReporter.
type TraceReporterFunc func(TraceData)

// Report calls the underlying function.
func (f TraceReporterFunc) Report(data TraceData) {
	f(data)
}

// NullReporter is the default TraceReporter which does not do anything.
var NullReporter TraceReporter = nullReporter{}

type nullReporter struct{}

func (nullReporter) Report(_ TraceData) {
}

// SimpleTraceReporter is a trace reporter which prints using the default logger.
var SimpleTraceReporter TraceReporter = simpleTraceReporter{}

type simpleTraceReporter struct{}

func (simpleTraceReporter) Report(data TraceData) {
	log.Printf("SimpleTraceReporter.Report span: %+v", data)
}

// Annotations is used to track annotations and report them to a TraceReporter.
type Annotations struct {
	timeNow  func() time.Time
	reporter TraceReporter
	data     TraceData

	annotationsBacking       [2]Annotation
	binaryAnnotationsBacking [2]BinaryAnnotation
}

// SetMethod sets the method being called.
func (as *Annotations) SetMethod(method string) {
	as.data.Method = method
}

// GetTime returns the time using the timeNow function stored in the annotations.
func (as *Annotations) GetTime() time.Time {
	return as.timeNow()
}

// AddBinaryAnnotation adds a binary annotation.
func (as *Annotations) AddBinaryAnnotation(key string, value interface{}) {
	binaryAnnotation := BinaryAnnotation{Key: key, Value: value}
	as.data.BinaryAnnotations = append(as.data.BinaryAnnotations, binaryAnnotation)
}

// AddAnnotationAt adds a standard annotation with the specified time.
func (as *Annotations) AddAnnotationAt(key AnnotationKey, ts time.Time) {
	annotation := Annotation{Key: key, Timestamp: ts}
	as.data.Annotations = append(as.data.Annotations, annotation)
}

// Report reports the annotations to the given trace reporter, if tracing is enabled in the span.
func (as *Annotations) Report() {
	if as.data.Span.TracingEnabled() {
		as.reporter.Report(as.data)
	}
}
