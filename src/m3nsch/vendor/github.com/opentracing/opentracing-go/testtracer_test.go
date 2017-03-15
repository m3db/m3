package opentracing

import (
	"strconv"
	"strings"
)

const testHTTPHeaderPrefix = "testprefix-"

// testTracer is a most-noop Tracer implementation that makes it possible for
// unittests to verify whether certain methods were / were not called.
type testTracer struct{}

var fakeIDSource = 1

func nextFakeID() int {
	fakeIDSource++
	return fakeIDSource
}

type testSpanContext struct {
	HasParent bool
	FakeID    int
}

func (n testSpanContext) ForeachBaggageItem(handler func(k, v string) bool) {}

type testSpan struct {
	spanContext   testSpanContext
	OperationName string
}

// testSpan:
func (n testSpan) Context() SpanContext                                  { return n.spanContext }
func (n testSpan) SetTag(key string, value interface{}) Span             { return n }
func (n testSpan) Finish()                                               {}
func (n testSpan) FinishWithOptions(opts FinishOptions)                  {}
func (n testSpan) LogEvent(event string)                                 {}
func (n testSpan) LogEventWithPayload(event string, payload interface{}) {}
func (n testSpan) Log(data LogData)                                      {}
func (n testSpan) SetOperationName(operationName string) Span            { return n }
func (n testSpan) Tracer() Tracer                                        { return testTracer{} }
func (n testSpan) SetBaggageItem(key, val string) Span                   { return n }
func (n testSpan) BaggageItem(key string) string                         { return "" }

// StartSpan belongs to the Tracer interface.
func (n testTracer) StartSpan(operationName string, opts ...StartSpanOption) Span {
	sso := StartSpanOptions{}
	for _, o := range opts {
		o.Apply(&sso)
	}
	return n.startSpanWithOptions(operationName, sso)
}

func (n testTracer) startSpanWithOptions(name string, opts StartSpanOptions) Span {
	fakeID := nextFakeID()
	if len(opts.References) > 0 {
		fakeID = opts.References[0].ReferencedContext.(testSpanContext).FakeID
	}
	return testSpan{
		OperationName: name,
		spanContext: testSpanContext{
			HasParent: len(opts.References) > 0,
			FakeID:    fakeID,
		},
	}
}

// Inject belongs to the Tracer interface.
func (n testTracer) Inject(sp SpanContext, format interface{}, carrier interface{}) error {
	spanContext := sp.(testSpanContext)
	switch format {
	case HTTPHeaders, TextMap:
		carrier.(TextMapWriter).Set(testHTTPHeaderPrefix+"fakeid", strconv.Itoa(spanContext.FakeID))
		return nil
	}
	return ErrUnsupportedFormat
}

// Extract belongs to the Tracer interface.
func (n testTracer) Extract(format interface{}, carrier interface{}) (SpanContext, error) {
	switch format {
	case HTTPHeaders, TextMap:
		// Just for testing purposes... generally not a worthwhile thing to
		// propagate.
		sm := testSpanContext{}
		err := carrier.(TextMapReader).ForeachKey(func(key, val string) error {
			switch strings.ToLower(key) {
			case testHTTPHeaderPrefix + "fakeid":
				i, err := strconv.Atoi(val)
				if err != nil {
					return err
				}
				sm.FakeID = i
			}
			return nil
		})
		return sm, err
	}
	return nil, ErrSpanContextNotFound
}
