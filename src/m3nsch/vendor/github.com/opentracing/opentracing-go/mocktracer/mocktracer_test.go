package mocktracer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/ext"
)

func TestMockTracer_StartSpan(t *testing.T) {
	tracer := New()
	span1 := tracer.StartSpan(
		"a",
		opentracing.Tags(map[string]interface{}{"x": "y"}))

	span2 := span1.Tracer().StartSpan(
		"", opentracing.ChildOf(span1.Context()))
	span2.Finish()
	span1.Finish()
	spans := tracer.FinishedSpans()
	assert.Equal(t, 2, len(spans))

	parent := spans[1]
	child := spans[0]
	assert.Equal(t, map[string]interface{}{"x": "y"}, parent.Tags())
	assert.Equal(t, child.ParentID, parent.Context().(MockSpanContext).SpanID)
}

func TestMockSpan_SetOperationName(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("")
	span.SetOperationName("x")
	assert.Equal(t, "x", span.(*MockSpan).OperationName)
}

func TestMockSpanContext_Baggage(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("x")
	span.SetBaggageItem("x", "y")
	assert.Equal(t, "y", span.BaggageItem("x"))
	assert.Equal(t, map[string]string{"x": "y"}, span.Context().(MockSpanContext).Baggage)

	baggage := make(map[string]string)
	span.Context().ForeachBaggageItem(func(k, v string) bool {
		baggage[k] = v
		return true
	})
	assert.Equal(t, map[string]string{"x": "y"}, baggage)

	span.SetBaggageItem("a", "b")
	baggage = make(map[string]string)
	span.Context().ForeachBaggageItem(func(k, v string) bool {
		baggage[k] = v
		return false // exit early
	})
	assert.Equal(t, 2, len(span.Context().(MockSpanContext).Baggage))
	assert.Equal(t, 1, len(baggage))
}

func TestMockSpan_Tag(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("x")
	span.SetTag("x", "y")
	assert.Equal(t, "y", span.(*MockSpan).Tag("x"))
}

func TestMockSpan_Tags(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("x")
	span.SetTag("x", "y")
	assert.Equal(t, map[string]interface{}{"x": "y"}, span.(*MockSpan).Tags())
}

func TestMockTracer_FinishedSpans_and_Reset(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("x")
	span.SetTag("x", "y")
	span.Finish()
	spans := tracer.FinishedSpans()
	assert.Equal(t, 1, len(spans))
	assert.Equal(t, map[string]interface{}{"x": "y"}, spans[0].Tags())

	tracer.Reset()
	spans = tracer.FinishedSpans()
	assert.Equal(t, 0, len(spans))
}

func TestMockSpan_Logs(t *testing.T) {
	tracer := New()
	span := tracer.StartSpan("x")
	span.LogEvent("x")
	span.LogEventWithPayload("y", "z")
	span.Log(opentracing.LogData{Event: "a"})
	span.FinishWithOptions(opentracing.FinishOptions{
		BulkLogData: []opentracing.LogData{opentracing.LogData{Event: "f"}}})
	spans := tracer.FinishedSpans()
	assert.Equal(t, 1, len(spans))
	assert.Equal(t, []opentracing.LogData{
		opentracing.LogData{Event: "x"},
		opentracing.LogData{Event: "y", Payload: "z"},
		opentracing.LogData{Event: "a"},
		opentracing.LogData{Event: "f"},
	}, spans[0].Logs())
}

func TestMockTracer_Propagation(t *testing.T) {
	tests := []struct {
		sampled bool
	}{
		{true},
		{false},
	}
	for _, test := range tests {
		tracer := New()
		span := tracer.StartSpan("x")
		span.SetBaggageItem("x", "y")
		if !test.sampled {
			ext.SamplingPriority.Set(span, 0)
		}
		mSpan := span.(*MockSpan)

		assert.Equal(t, opentracing.ErrUnsupportedFormat,
			tracer.Inject(span.Context(), opentracing.Binary, nil))
		assert.Equal(t, opentracing.ErrInvalidCarrier,
			tracer.Inject(span.Context(), opentracing.TextMap, span))

		carrier := make(map[string]string)

		err := tracer.Inject(span.Context(), opentracing.TextMap, opentracing.TextMapCarrier(carrier))
		require.NoError(t, err)
		assert.Equal(t, 4, len(carrier), "expect baggage + 2 ids + sampled")

		_, err = tracer.Extract(opentracing.Binary, nil)
		assert.Equal(t, opentracing.ErrUnsupportedFormat, err)
		_, err = tracer.Extract(opentracing.TextMap, tracer)
		assert.Equal(t, opentracing.ErrInvalidCarrier, err)

		extractedContext, err := tracer.Extract(opentracing.TextMap, opentracing.TextMapCarrier(carrier))
		require.NoError(t, err)
		assert.Equal(t, mSpan.SpanContext.TraceID, extractedContext.(MockSpanContext).TraceID)
		assert.Equal(t, mSpan.SpanContext.SpanID, extractedContext.(MockSpanContext).SpanID)
		assert.Equal(t, test.sampled, extractedContext.(MockSpanContext).Sampled)
		assert.Equal(t, "y", extractedContext.(MockSpanContext).Baggage["x"])
	}
}
