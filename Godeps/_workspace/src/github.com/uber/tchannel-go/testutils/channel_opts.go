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

package testutils

import (
	"flag"
	"testing"
	"time"

	"github.com/uber/tchannel-go"
	"github.com/uber/tchannel-go/atomic"
)

var connectionLog = flag.Bool("connectionLog", false, "Enables connection logging in tests")

// Default service names for the test channels.
const (
	DefaultServerName = "testService"
	DefaultClientName = "testService-client"
)

// ChannelOpts contains options to create a test channel using WithServer
type ChannelOpts struct {
	tchannel.ChannelOptions

	// ServiceName defaults to DefaultServerName or DefaultClientName.
	ServiceName string

	// LogVerification contains options for controlling the log verification.
	LogVerification LogVerification

	// optFn is run with the channel options before creating the channel.
	optFn func(*ChannelOpts)

	// postFns is a list of functions that are run after the test.
	// They are run even if the test fails.
	postFns []func()
}

// LogVerification contains options to control the log verification.
type LogVerification struct {
	Disabled bool

	Filters []LogFilter
}

// LogFilter is a single substring match that can be ignored.
type LogFilter struct {
	// Filter specifies the substring match to search
	// for in the log message to skip raising an error.
	Filter string

	// Count is the maximum number of allowed warn+ logs matching
	// Filter before errors are raised.
	Count uint

	// FieldFilters specifies expected substring matches for fields.
	FieldFilters map[string]string
}

// SetServiceName sets ServiceName.
func (o *ChannelOpts) SetServiceName(svcName string) *ChannelOpts {
	o.ServiceName = svcName
	return o
}

// SetProcessName sets the ProcessName in ChannelOptions.
func (o *ChannelOpts) SetProcessName(processName string) *ChannelOpts {
	o.ProcessName = processName
	return o
}

// SetStatsReporter sets StatsReporter in ChannelOptions.
func (o *ChannelOpts) SetStatsReporter(statsReporter tchannel.StatsReporter) *ChannelOpts {
	o.StatsReporter = statsReporter
	return o
}

// SetTraceReporter sets TraceReporter in ChannelOptions.
func (o *ChannelOpts) SetTraceReporter(traceReporter tchannel.TraceReporter) *ChannelOpts {
	o.TraceReporter = traceReporter
	return o
}

// SetTraceReporterFactory sets TraceReporterFactory in ChannelOptions.
func (o *ChannelOpts) SetTraceReporterFactory(factory tchannel.TraceReporterFactory) *ChannelOpts {
	o.TraceReporterFactory = factory
	return o
}

// SetFramePool sets FramePool in DefaultConnectionOptions.
func (o *ChannelOpts) SetFramePool(framePool tchannel.FramePool) *ChannelOpts {
	o.DefaultConnectionOptions.FramePool = framePool
	return o
}

// SetTimeNow sets TimeNow in ChannelOptions.
func (o *ChannelOpts) SetTimeNow(timeNow func() time.Time) *ChannelOpts {
	o.TimeNow = timeNow
	return o
}

// SetTraceSampleRate sets the TraceSampleRate in ChannelOptions.
func (o *ChannelOpts) SetTraceSampleRate(sampleRate float64) *ChannelOpts {
	o.ChannelOptions.TraceSampleRate = &sampleRate
	return o
}

// DisableLogVerification disables log verification for this channel.
func (o *ChannelOpts) DisableLogVerification() *ChannelOpts {
	o.LogVerification.Disabled = true
	return o
}

// AddLogFilter sets an allowed filter for warning/error logs and sets
// the maximum number of times that log can occur.
func (o *ChannelOpts) AddLogFilter(filter string, maxCount uint, fields ...string) *ChannelOpts {
	fieldFilters := make(map[string]string)
	for i := 0; i < len(fields); i += 2 {
		fieldFilters[fields[i]] = fields[i+1]
	}

	o.LogVerification.Filters = append(o.LogVerification.Filters, LogFilter{
		Filter:       filter,
		Count:        maxCount,
		FieldFilters: fieldFilters,
	})
	return o
}

func (o *ChannelOpts) addPostFn(f func()) {
	o.postFns = append(o.postFns, f)
}

func defaultString(v string, defaultValue string) string {
	if v == "" {
		return defaultValue
	}
	return v
}

func getChannelOptions(opts *ChannelOpts) *tchannel.ChannelOptions {
	if opts.Logger == nil && *connectionLog {
		opts.Logger = tchannel.SimpleLogger
	}
	if opts.optFn != nil {
		opts.optFn(opts)
	}
	return &opts.ChannelOptions
}

// NewOpts returns a new ChannelOpts that can be used in a chained fashion.
func NewOpts() *ChannelOpts { return &ChannelOpts{} }

// DefaultOpts will return opts if opts is non-nil, NewOpts otherwise.
func DefaultOpts(opts *ChannelOpts) *ChannelOpts {
	if opts == nil {
		return NewOpts()
	}
	return opts
}

// WrapLogger wraps the given logger with extra verification.
func (v *LogVerification) WrapLogger(t testing.TB, l tchannel.Logger) tchannel.Logger {
	return errorLogger{l, t, v, &errorLoggerState{
		matchCount: make([]atomic.Uint32, len(v.Filters)),
	}}
}
