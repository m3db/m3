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

package opentracing

import (
	"context"
	"testing"

	"github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/mocktracer"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestStartSpanFromContext(t *testing.T) {
	t.Run("uses local tracer if available", func(t *testing.T) {
		mtr := mocktracer.New()
		defer mockoutGlobalTracer(opentracing.NoopTracer{})()

		rootSp := mtr.StartSpan("root")
		ctx := opentracing.ContextWithSpan(context.Background(), rootSp)

		childSp, ctx := StartSpanFromContext(ctx, "child")

		rootSp.Finish()
		childSp.Finish()

		assertHasSpans(t, mtr, []string{"root", "child"})

		ctxSpan := opentracing.SpanFromContext(ctx)
		require.IsType(t, (*mocktracer.MockSpan)(nil), ctxSpan)
		assert.Equal(t, ctxSpan.(*mocktracer.MockSpan).OperationName, "child",
			"should set span on context to child span")

	})

	t.Run("uses global tracer if no parent span", func(t *testing.T) {
		mtr := mocktracer.New()
		defer mockoutGlobalTracer(mtr)()

		sp, ctx := StartSpanFromContext(context.Background(), "foo")
		sp.Finish()

		assertHasSpans(t, mtr, []string{"foo"})
		assert.NotNil(t, opentracing.SpanFromContext(ctx))
	})
}

func TestStartSpanFromContextOrRoot(t *testing.T) {
	t.Run("uses noop tracer if nothing passed in", func(t *testing.T) {
		assert.Equal(t, opentracing.NoopTracer{}.StartSpan(""),
			SpanFromContextOrNoop(context.
				Background()))
	})

	t.Run("returns span if span attached to context", func(t *testing.T) {
		mt := mocktracer.New()
		root := mt.StartSpan("root")
		ctx := opentracing.ContextWithSpan(context.Background(), root)

		assert.Equal(t, root, SpanFromContextOrNoop(ctx))
	})

}

func mockoutGlobalTracer(mtr opentracing.Tracer) func() {
	oldGGT := getGlobalTracer
	getGlobalTracer = func() opentracing.Tracer {
		return mtr
	}

	return func() {
		getGlobalTracer = oldGGT
	}
}

func assertHasSpans(t *testing.T, mtr *mocktracer.MockTracer, opNames []string) {
	spans := mtr.FinishedSpans()
	actualOpNames := make([]string, len(spans))

	for i, sp := range spans {
		actualOpNames[i] = sp.OperationName
	}

	assert.Equal(t, opNames, actualOpNames)
}
