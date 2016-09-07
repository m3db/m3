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

package instrument

import (
	"time"

	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

// Options represents the options for instrumentation
type Options interface {
	// SetLogger sets the logger
	SetLogger(value xlog.Logger) Options

	// Logger returns the logger
	Logger() xlog.Logger

	// SetMetricsScope sets the metricsScope
	SetMetricsScope(value tally.Scope) Options

	// MetricsScope returns the metricsScope
	MetricsScope() tally.Scope

	// GetGaugeInterval returns the time between updates of many gauges within the system
	GetGaugeInterval() time.Duration
}

// MethodMetrics is a bundle of common metrics with a uniform naming scheme
type MethodMetrics struct {
	Error   tally.Counter
	Success tally.Counter
	Latency tally.Timer
}

// ReportSuccessOrFailure increments Error/Success counter dependant on the error
func (m *MethodMetrics) ReportSuccessOrFailure(e error) {
	if e != nil {
		m.Error.Inc(1)
	} else {
		m.Success.Inc(1)
	}
}

// NewMethodMetrics returns a new Method metrics for the given method name
func NewMethodMetrics(scope tally.Scope, methodName string) MethodMetrics {
	return MethodMetrics{
		Error:   scope.Counter(methodName + ".error"),
		Success: scope.Counter(methodName + ".success"),
		Latency: scope.Timer(methodName + ".latency"),
	}
}
