// Copyright (c) 2021 Uber Technologies, Inc.
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

// Package opentelemetry provides Open Telemetry configuration.
package opentelemetry

import (
	"context"
	"fmt"

	"github.com/uber-go/tally"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"google.golang.org/grpc"
)

// Configuration configures an OpenTelemetry trace provider.
type Configuration struct {
	ServiceName string            `yaml:"serviceName"`
	Endpoint    string            `yaml:"endpoint"`
	Insecure    bool              `yaml:"insecure"`
	Attributes  map[string]string `yaml:"attributes"`
}

// TracerProviderOptions is a set of options to use when creating the
// trace provider.
type TracerProviderOptions struct {
	// Attributes is a set of programmatic attributes to add at construction.
	Attributes []attribute.KeyValue
}

// NewTracerProvider returns a new tracer provider.
func (c Configuration) NewTracerProvider(
	ctx context.Context,
	scope tally.Scope,
	opts TracerProviderOptions,
) (*sdktrace.TracerProvider, error) {
	attributes := make([]attribute.KeyValue, 0, 1+len(c.Attributes)+len(opts.Attributes))
	attributes = append(attributes, semconv.ServiceNameKey.String(c.ServiceName))
	for k, v := range c.Attributes {
		attributes = append(attributes, attribute.String(k, v))
	}
	attributes = append(attributes, opts.Attributes...)

	res, err := resource.New(ctx, resource.WithAttributes(attributes...))
	if err != nil {
		return nil, fmt.Errorf("failed to create resource: %w", err)
	}

	driverOpts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(c.Endpoint),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	}
	if c.Insecure {
		driverOpts = append(driverOpts, otlptracegrpc.WithInsecure())
	}
	driver := otlptracegrpc.NewClient(driverOpts...)
	traceExporter, err := otlptrace.New(ctx, driver)
	if err != nil {
		return nil, fmt.Errorf("failed to trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	batchSpanProcessor := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerMetricsProcessor := newTraceSpanProcessor(scope)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(batchSpanProcessor),
		sdktrace.WithSpanProcessor(tracerMetricsProcessor),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tracerProvider, nil
}

type traceSpanProcessor struct {
	traceStart       tally.Counter
	traceEnd         tally.Counter
	tracerShutdown   tally.Counter
	tracerForceFlush tally.Counter
}

func newTraceSpanProcessor(scope tally.Scope) sdktrace.SpanProcessor {
	traceScope := scope.SubScope("trace")
	tracerScope := scope.SubScope("tracer")
	return &traceSpanProcessor{
		traceStart:       traceScope.Counter("start"),
		traceEnd:         traceScope.Counter("end"),
		tracerShutdown:   tracerScope.Counter("shutdown"),
		tracerForceFlush: tracerScope.Counter("force-flush"),
	}
}

func (p *traceSpanProcessor) OnStart(parent context.Context, s sdktrace.ReadWriteSpan) {
	p.traceStart.Inc(1)
}

func (p *traceSpanProcessor) OnEnd(s sdktrace.ReadOnlySpan) {
	p.traceEnd.Inc(1)
}

func (p *traceSpanProcessor) Shutdown(ctx context.Context) error {
	p.tracerShutdown.Inc(1)
	return nil
}

func (p *traceSpanProcessor) ForceFlush(ctx context.Context) error {
	p.tracerForceFlush.Inc(1)
	return nil
}
