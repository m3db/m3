// Copyright (c) 2017 Uber Technologies, Inc.
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

package instrument

import (
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"time"

	prom "github.com/m3db/prometheus_client_golang/prometheus"
	"github.com/uber-go/tally"
	"github.com/uber-go/tally/m3"
	"github.com/uber-go/tally/multi"
	"github.com/uber-go/tally/prometheus"
	"go.uber.org/zap"
)

var errNoReporterConfigured = errors.New("no reporter configured")

// ScopeConfiguration configures a metric scope.
type ScopeConfiguration struct {
	// Prefix of metrics in this scope.
	Prefix string `yaml:"prefix"`

	// Metrics reporting interval.
	ReportingInterval time.Duration `yaml:"reportingInterval"`

	// Common tags shared by metrics reported.
	CommonTags map[string]string `yaml:"tags"`
}

type AttributionConfiguration struct {
	// Name of this attribution
	Name string `yaml:"name"`

	// Max capacity of this attribution
	Capacity int `yaml:"capacity" validate:"nonzero,min=1"`

	// Sampling rate
	SamplingRate float64 `yaml:"samplingRate" validate:"nonzero,min=0.0,max=1.0"`

	// Matched labels of this attribution, if a time series has label A & B & C, and here we attribute
	// to A, we will count all ts with coordinator_attribution_A_sample_count{A="value for label A"}
	Labels []string `yaml:"labels" validate:"min=1,max=3"`

	// Match metrics for attribution, we support two types of matchers for now:
	// 1. only sample with label A == <value> will be used for attribution
	// 2. only sample with label A != <value> will be used for attribution
	// the final decision is concatenated using AND unary among matchers
	Matchers []string `yaml:"matchers"`
}

// MetricsConfiguration configures options for emitting metrics.
type MetricsConfiguration struct {
	// Root scope configuration.
	RootScope *ScopeConfiguration `yaml:"scope"`

	// M3 reporter configuration.
	M3Reporter *m3.Configuration `yaml:"m3"`

	// Prometheus reporter configuration.
	PrometheusReporter *PrometheusConfiguration `yaml:"prometheus"`

	// Metrics sampling rate.
	SamplingRate float64 `yaml:"samplingRate" validate:"nonzero,min=0.0,max=1.0"`

	// Extended metrics type.
	ExtendedMetrics *ExtendedMetricsType `yaml:"extended"`

	// Metric sanitization type.
	Sanitization *MetricSanitizationType `yaml:"sanitization"`

	Attributions []*AttributionConfiguration `yaml:"attributions"`
}

// NewRootScope creates a new tally.Scope based on a tally.CachedStatsReporter
// based on the the the config.
func (mc *MetricsConfiguration) NewRootScope() (tally.Scope, io.Closer, error) {
	opts := NewRootScopeAndReportersOptions{}
	scope, closer, _, err := mc.NewRootScopeAndReporters(opts)
	return scope, closer, err
}

// MetricsConfigurationReporters is the reporters constructed.
type MetricsConfigurationReporters struct {
	AllReporters       []tally.CachedStatsReporter
	M3Reporter         *MetricsConfigurationM3Reporter
	PrometheusReporter *MetricsConfigurationPrometheusReporter
}

// MetricsConfigurationM3Reporter is the M3 reporter if constructed.
type MetricsConfigurationM3Reporter struct {
	Reporter m3.Reporter
}

// MetricsConfigurationPrometheusReporter is the Prometheus reporter if constructed.
type MetricsConfigurationPrometheusReporter struct {
	Reporter prometheus.Reporter
	Registry *prom.Registry
}

// NewRootScopeAndReportersOptions is a set of options.
type NewRootScopeAndReportersOptions struct {
	PrometheusHandlerListener    net.Listener
	PrometheusDefaultServeMux    *http.ServeMux
	PrometheusExternalRegistries []PrometheusExternalRegistry
	PrometheusOnError            func(e error)
	// CommonLabels will be appended to every metric gathered.
	CommonLabels map[string]string
}

type metricsClosers struct {
	// serverCloser is responsible for closing the http server handling /metrics
	// if one was started up as a part of reporter creation.
	serverCloser io.Closer
	// reporterClose is responsible for closing the underlying tally.Reporter
	// responsible for reporting metrics for all registered scopes.
	reporterCloser io.Closer
}

func (m metricsClosers) Close() error {
	if err := m.reporterCloser.Close(); err != nil {
		return err
	}

	if m.serverCloser != nil {
		return m.serverCloser.Close()
	}

	return nil
}

// NewRootScopeAndReporters creates a new tally.Scope based on a tally.CachedStatsReporter
// based on the the the config along with the reporters used.
func (mc *MetricsConfiguration) NewRootScopeAndReporters(
	opts NewRootScopeAndReportersOptions,
) (
	tally.Scope,
	io.Closer,
	MetricsConfigurationReporters,
	error,
) {
	var (
		result  MetricsConfigurationReporters
		closers metricsClosers
	)
	if mc.M3Reporter != nil {
		r, err := mc.M3Reporter.NewReporter()
		if err != nil {
			return nil, nil, MetricsConfigurationReporters{}, err
		}
		result.AllReporters = append(result.AllReporters, r)
		result.M3Reporter = &MetricsConfigurationM3Reporter{
			Reporter: r,
		}
	}
	if mc.PrometheusReporter != nil {
		// Set a default on error method for sane handling when registering metrics
		// results in an error with the Prometheus reporter.
		onError := func(e error) {
			logger := NewOptions().Logger()
			logger.Error("register metrics error", zap.Error(e))
		}
		if opts.PrometheusOnError != nil {
			onError = opts.PrometheusOnError
		}

		// Override the default registry with an empty one that does not have the default
		// registered collectors (Go and Process). The M3 reporters will emit the Go metrics
		// and the Process metrics are reported by both the M3 process reporter and a
		// modified Prometheus process collector, which reports everything except the
		// number of open FDs.
		//
		// Collecting the number of F.Ds for a process that has many of them can take a long
		// time and be very CPU intensive, especially the default Prometheus collector
		// implementation which is less optimized than the M3 implementation.
		//
		// TODO: Emit the Prometheus process stats from our own process reporter so we
		// get the same stats regardless of the reporter used. See issue:
		// https://github.com/m3db/m3/issues/1649
		registry := prom.NewRegistry()
		if err := registry.Register(NewPrometheusProcessCollector(ProcessCollectorOpts{
			DisableOpenFDs: true,
		})); err != nil {
			return nil, nil, MetricsConfigurationReporters{}, fmt.Errorf("could not create process collector: %v", err)
		}
		opts := PrometheusConfigurationOptions{
			Registry:           registry,
			ExternalRegistries: opts.PrometheusExternalRegistries,
			HandlerListener:    opts.PrometheusHandlerListener,
			DefaultServeMux:    opts.PrometheusDefaultServeMux,
			OnError:            onError,
			CommonLabels:       opts.CommonLabels,
		}

		// Use default instrument package default histogram buckets if not set.
		if len(mc.PrometheusReporter.DefaultHistogramBuckets) == 0 {
			for _, v := range DefaultHistogramTimerHistogramBuckets().AsValues() {
				bucket := prometheus.HistogramObjective{
					Upper: v,
				}
				mc.PrometheusReporter.DefaultHistogramBuckets =
					append(mc.PrometheusReporter.DefaultHistogramBuckets, bucket)
			}
		}

		if len(mc.PrometheusReporter.DefaultSummaryObjectives) == 0 {
			for k, v := range DefaultSummaryQuantileObjectives() {
				q := prometheus.SummaryObjective{
					Percentile:   k,
					AllowedError: v,
				}
				mc.PrometheusReporter.DefaultSummaryObjectives =
					append(mc.PrometheusReporter.DefaultSummaryObjectives, q)
			}
		}

		r, srvCloser, err := mc.PrometheusReporter.NewReporter(opts)
		if err != nil {
			return nil, nil, MetricsConfigurationReporters{}, err
		}
		closers.serverCloser = srvCloser

		result.AllReporters = append(result.AllReporters, r)
		result.PrometheusReporter = &MetricsConfigurationPrometheusReporter{
			Reporter: r,
			Registry: registry,
		}
	}
	if len(result.AllReporters) == 0 {
		return nil, nil, MetricsConfigurationReporters{}, errNoReporterConfigured
	}

	var r tally.CachedStatsReporter
	if len(result.AllReporters) == 1 {
		r = result.AllReporters[0]
	} else {
		r = multi.NewMultiCachedReporter(result.AllReporters...)
	}

	scope, closer := mc.NewRootScopeReporter(r)
	closers.reporterCloser = closer

	return scope, closers, result, nil
}

// NewRootScopeReporter creates a new tally.Scope based on a given tally.CachedStatsReporter
// and given root scope config. In most cases NewRootScope should be used, but for cases such
// as hooking into the reporter to manually flush it.
func (mc *MetricsConfiguration) NewRootScopeReporter(
	r tally.CachedStatsReporter,
) (tally.Scope, io.Closer) {
	var (
		prefix string
		tags   map[string]string
	)

	if mc.RootScope != nil {
		if mc.RootScope.Prefix != "" {
			prefix = mc.RootScope.Prefix
		}
		if mc.RootScope.CommonTags != nil {
			tags = mc.RootScope.CommonTags
		}
	}

	var sanitizeOpts *tally.SanitizeOptions
	if mc.Sanitization != nil {
		sanitizeOpts = mc.Sanitization.NewOptions()
	}

	scopeOpts := tally.ScopeOptions{
		Tags:            tags,
		Prefix:          prefix,
		CachedReporter:  r,
		SanitizeOptions: sanitizeOpts,
	}
	reportInterval := mc.ReportInterval()
	scope, closer := tally.NewRootScope(scopeOpts, reportInterval)
	if mc.ExtendedMetrics != nil {
		StartReportingExtendedMetrics(scope, reportInterval, *mc.ExtendedMetrics)
	}

	return scope, closer
}

// SampleRate returns the metrics sampling rate.
func (mc *MetricsConfiguration) SampleRate() float64 {
	if mc.SamplingRate > 0.0 && mc.SamplingRate <= 1.0 {
		return mc.SamplingRate
	}
	return defaultSamplingRate
}

// ReportInterval returns the metrics reporting interval.
func (mc *MetricsConfiguration) ReportInterval() time.Duration {
	if mc.RootScope != nil && mc.RootScope.ReportingInterval != 0 {
		return mc.RootScope.ReportingInterval
	}
	return defaultReportingInterval
}
