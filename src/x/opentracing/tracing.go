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
	"fmt"
	"io"
	"runtime"
	"strings"
	"time"

	lightstep "github.com/lightstep/lightstep-tracer-go"
	"github.com/opentracing/opentracing-go"
	"github.com/uber-go/tally"
	jaegercfg "github.com/uber/jaeger-client-go/config"
	jaegerzap "github.com/uber/jaeger-client-go/log/zap"
	jaegertally "github.com/uber/jaeger-lib/metrics/tally"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	otelopentracing "go.opentelemetry.io/otel/bridge/opentracing"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.4.0"
	"go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	"github.com/m3db/m3/src/x/instrument"
)

const (
	spanTagBuildRevision  = "build.revision"
	spanTagBuildVersion   = "build.version"
	spanTagBuildBranch    = "build.branch"
	spanTagBuildDate      = "build.date"
	spanTagBuildTimeUnix  = "build.time_unix"
	spanTagBuildGoVersion = "build.go_version"
)

var (
	// TracingBackendOpenTelemetry indicates the OpenTelemetry backend should be used.
	TracingBackendOpenTelemetry = "opentelemetry"
	// TracingBackendJaeger indicates the Jaeger backend should be used.
	TracingBackendJaeger = "jaeger"
	// TracingBackendLightstep indicates the LightStep backend should be used.
	TracingBackendLightstep = "lightstep"

	supportedBackends = []string{
		TracingBackendOpenTelemetry,
		TracingBackendJaeger,
		TracingBackendLightstep,
	}

	tracerSpanTags = map[string]string{
		spanTagBuildRevision:  instrument.Revision,
		spanTagBuildBranch:    instrument.Branch,
		spanTagBuildVersion:   instrument.Version,
		spanTagBuildDate:      instrument.BuildDate,
		spanTagBuildTimeUnix:  instrument.BuildTimeUnix,
		spanTagBuildGoVersion: runtime.Version(),
	}
)

// TracingConfiguration configures an opentracing backend for m3query to use. Currently only jaeger is supported.
// Tracing is disabled if no backend is specified.
type TracingConfiguration struct {
	ServiceName   string                     `yaml:"serviceName"`
	Backend       string                     `yaml:"backend"`
	OpenTelemetry OpenTelemetryConfiguration `yaml:"opentelemetry"`
	Jaeger        jaegercfg.Configuration    `yaml:"jaeger"`
	Lightstep     lightstep.Options          `yaml:"lightstep"`
}

// OpenTelemetryConfiguration is the configuration for open telemetry.
type OpenTelemetryConfiguration struct {
	ServiceName string `yaml:"serviceName"`
	Endpoint string `yaml:"endpoint"`
	Insecure bool `yaml:"insecure"`
}

type traceProviderCloser struct {
	ctx context.Context
	tracer trace.Provider
}

func newTraceProviderCloser(
	ctx context.Context, 
	tracer trace.Provider,
) io.Closer {
	return &traceProviderCloser{
		ctx: ctx,
		tracer: tracer,
	}
}

func (c *traceProviderCloser) Close() error {
	return c.tracerProvider.Shutdown(c.ctx)
}

func (c OpenTelemetryConfiguration) NewTraceProviderAndOpenTracingTracer(ctx context.Context) (
	ctx context.Context,
	trace.Provider, 
	opentracing.Tracer, 
	io.Closer, 
	error,
) {
	res, err := resource.New(ctx, resource.WithAttributes(
		semconv.ServiceNameKey.String(c.ServiceName)))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create resource: %w", err)
	}

	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(c.Endpoint),
		otlptracegrpc.WithDialOption(grpc.WithBlock()),
	}
	if c.Insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	traceExporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to trace exporter: %w", err)
	}

	// Register the trace exporter with a TracerProvider, using a batch
	// span processor to aggregate spans before export.
	bsp := sdktrace.NewBatchSpanProcessor(traceExporter)
	tracerProvider := sdktrace.NewTracerProvider(
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
		sdktrace.WithResource(res),
		sdktrace.WithSpanProcessor(bsp),
	)
	otel.SetTracerProvider(tracerProvider)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	ctx, bridgeTracer, wrappedTracer := otelopentracing.NewTracerPairWithContext(ctx, tracerProvider)
	closer := newTraceProviderCloser(ctx, traceExporter)
	return ctx, wrappedTracer, bridgeTracer, closer, nil
}

// NewTracer returns a tracer configured with the configuration provided by this struct. The tracer's concrete
// type is determined by cfg.Backend. Currently only `"jaeger"` is supported. `""` implies
// disabled (NoopTracer).
func (cfg *TracingConfiguration) NewTracer(
	defaultServiceName string, 
	scope tally.Scope, 
	logger *zap.Logger,
) (opentracing.Tracer, io.Closer, error) {
	switch cfg.Backend {
	case "":
		return opentracing.NoopTracer{}, noopCloser{}, nil

	case TracingBackendOpenTelemetry:
		logger.Info("initializing LightStep tracer")
		return cfg.newOpenTelemetryTracer(defaultServiceName)

	case TracingBackendJaeger:
		logger.Info("initializing Jaeger tracer")
		return cfg.newJaegerTracer(defaultServiceName, scope, logger)

	case TracingBackendLightstep:
		logger.Info("initializing LightStep tracer")
		return cfg.newLightstepTracer(defaultServiceName)

	default:
		return nil, nil, fmt.Errorf("unknown tracing backend: %s. Supported backends are: [%s]",
			cfg.Backend,
			strings.Join(supportedBackends, ","))
	}
}

func (cfg *TracingConfiguration) newOpenTelemetryTracer(
	defaultServiceName string, 
	scope tally.Scope, 
	logger *zap.Logger,
) (opentracing.Tracer, io.Closer, error) {
	if cfg.OpenTelemetry.ServiceName == "" {
		cfg.OpenTelemetry.ServiceName = defaultServiceName
	}

	ctx := context.Background()
	_, _, openTracingTracer, closer, err := cfg.OpenTelemetry.NewTraceProviderAndOpenTracingTracer(ctx)
	if err != nil {
		return nil, nil, err
	}

	return openTracingTracer, closer, err
}

func (cfg *TracingConfiguration) newJaegerTracer(
	defaultServiceName string, 
	scope tally.Scope, 
	logger *zap.Logger,
) (opentracing.Tracer, io.Closer, error) {
	if cfg.Jaeger.ServiceName == "" {
		cfg.Jaeger.ServiceName = defaultServiceName
	}

	for k, v := range tracerSpanTags {
		cfg.Jaeger.Tags = append(cfg.Jaeger.Tags, opentracing.Tag{
			Key:   k,
			Value: v,
		})
	}

	tracer, jaegerCloser, err := cfg.Jaeger.NewTracer(
		jaegercfg.Logger(jaegerzap.NewLogger(logger)),
		jaegercfg.Metrics(jaegertally.Wrap(scope)))
	if err != nil {
		return nil, nil, fmt.Errorf("failed to initialize jaeger: %s", err.Error())
	}

	return tracer, jaegerCloser, nil
}

func (cfg *TracingConfiguration) newLightstepTracer(
	serviceName string,
) (opentracing.Tracer, io.Closer, error) {
	if cfg.Lightstep.Tags == nil {
		cfg.Lightstep.Tags = opentracing.Tags{}
	}

	tags := cfg.Lightstep.Tags
	if _, ok := tags[lightstep.ComponentNameKey]; !ok {
		tags[lightstep.ComponentNameKey] = serviceName
	}

	for k, v := range tracerSpanTags {
		tags[k] = v
	}

	tracer, err := lightstep.CreateTracer(cfg.Lightstep)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to create lightstep tracer: %v", err)
	}

	closer := &lightstepCloser{tracer: tracer}
	return tracer, closer, nil
}

type lightstepCloser struct {
	tracer lightstep.Tracer
}

func (l *lightstepCloser) Close() error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	l.tracer.Close(ctx)
	cancel()
	return ctx.Err()
}

type noopCloser struct{}

func (noopCloser) Close() error {
	return nil
}
