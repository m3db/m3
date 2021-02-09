// Copyright (c) 2020 Uber Technologies, Inc.
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

// Package limits contains paths to enforce read query limits.
package limits

import (
	"time"

	"github.com/m3db/m3/src/x/instrument"
)

// Key is a specific string type for context setting.
type Key string

// SourceContextKey is the key for setting and retrieving source from context.
const SourceContextKey Key = "source"

// QueryLimits provides an interface for managing query limits.
type QueryLimits interface {
	// DocsLimit limits queries by a global concurrent count of index docs matched.
	DocsLimit() LookbackLimit
	// BytesReadLimit limits queries by a global concurrent count of bytes read from disk.
	BytesReadLimit() LookbackLimit
	// DiskSeriesReadLimit limits queries by a global concurrent count of time series read from disk.
	DiskSeriesReadLimit() LookbackLimit

	// AnyExceeded returns an error if any of the query limits are exceeded.
	AnyExceeded() error
	// Start begins background resetting of the query limits.
	Start()
	// Stop end background resetting of the query limits.
	Stop()
}

// LookbackLimit provides an interface for a specific query limit.
type LookbackLimit interface {
	// Options returns the current limit options.
	Options() LookbackLimitOptions
	// Inc increments the recent value for the limit.
	Inc(new int, source []byte) error
	// Update changes the lookback limit settings.
	Update(opts LookbackLimitOptions) error
}

// LookbackLimitOptions holds options for a lookback limit to be enforced.
type LookbackLimitOptions struct {
	// Limit past which errors will be returned.
	// Zero disables the limit.
	Limit int64
	// Lookback is the period over which the limit is enforced.
	Lookback time.Duration
	// ForceExceeded, if true, makes all calls to the limit behave as though the limit is exceeded.
	ForceExceeded bool
}

// SourceLoggerBuilder builds a SourceLogger given instrument options.
type SourceLoggerBuilder interface {
	// NewSourceLogger builds a source logger.
	NewSourceLogger(name string, opts instrument.Options) SourceLogger
}

// SourceLogger attributes limit values to a source.
type SourceLogger interface {
	// LogSourceValue attributes values that exceed a given size to the source.
	LogSourceValue(val int64, source []byte)
}

// Options is a set of limit options.
type Options interface {
	// Validate validates the options.
	Validate() error

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetDocsLimitOpts sets the doc limit options.
	SetDocsLimitOpts(value LookbackLimitOptions) Options

	// DocsLimitOpts returns the doc limit options.
	DocsLimitOpts() LookbackLimitOptions

	// SetBytesReadLimitOpts sets the byte read limit options.
	SetBytesReadLimitOpts(value LookbackLimitOptions) Options

	// BytesReadLimitOpts returns the byte read limit options.
	BytesReadLimitOpts() LookbackLimitOptions

	// SetDiskSeriesReadLimitOpts sets the disk series read limit options.
	SetDiskSeriesReadLimitOpts(value LookbackLimitOptions) Options

	// DiskSeriesReadLimitOpts returns the disk series read limit options.
	DiskSeriesReadLimitOpts() LookbackLimitOptions

	// SetSourceLoggerBuilder sets the source logger.
	SetSourceLoggerBuilder(value SourceLoggerBuilder) Options

	// SourceLogger sets the source logger.
	SourceLoggerBuilder() SourceLoggerBuilder
}
