// Copyright (c) 2018 Uber Technologies, Inc.
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

package buffer

import (
	"time"

	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// OnFullStrategy defines the buffer behavior when the buffer is full.
type OnFullStrategy string

const (
	// ReturnError means an error will be returned
	// on new buffer requests when the buffer is full.
	ReturnError OnFullStrategy = "returnError"

	// DropOldest means the oldest message in the buffer
	// will be dropped to make room for new buffer requests
	// when the buffer is full.
	DropOldest OnFullStrategy = "dropOldest"
)

// Options configs the buffer.
type Options interface {
	// OnFullStrategy returns the strategy when buffer is full.
	OnFullStrategy() OnFullStrategy

	// SetOnFullStrategy sets the strategy when buffer is full.
	SetOnFullStrategy(value OnFullStrategy) Options

	// MaxMessageSize returns the max message size.
	MaxMessageSize() int

	// SetMaxMessageSize sets the max message size.
	SetMaxMessageSize(value int) Options

	// MaxBufferSize returns the max buffer size.
	MaxBufferSize() int

	// SetMaxBufferSize sets the max buffer size.
	SetMaxBufferSize(value int) Options

	// CloseCheckInterval returns the close check interval.
	CloseCheckInterval() time.Duration

	// SetCloseCheckInterval sets the close check interval.
	SetCloseCheckInterval(value time.Duration) Options

	// DropOldestInterval returns the interval to drop oldest buffer.
	// The max buffer size might be spilled over during the interval.
	DropOldestInterval() time.Duration

	// SetDropOldestInterval sets the interval to drop oldest buffer.
	// The max buffer size might be spilled over during the interval.
	SetDropOldestInterval(value time.Duration) Options

	// ScanBatchSize returns the scan batch size.
	ScanBatchSize() int

	// SetScanBatchSize sets the scan batch size.
	SetScanBatchSize(value int) Options

	// AllowedSpilloverRatio returns the ratio for allowed buffer spill over,
	// below which the buffer will drop oldest messages asynchronizely for
	// better performance. When the limit for allowed spill over is reached,
	// the buffer will start to drop oldest messages synchronizely.
	AllowedSpilloverRatio() float64

	// SetAllowedSpilloverRatio sets the ratio for allowed buffer spill over.
	SetAllowedSpilloverRatio(value float64) Options

	// CleanupRetryOptions returns the cleanup retry options.
	CleanupRetryOptions() retry.Options

	// SetCleanupRetryOptions sets the cleanup retry options.
	SetCleanupRetryOptions(value retry.Options) Options

	// InstrumentOptions returns the instrument options.
	InstrumentOptions() instrument.Options

	// SetInstrumentOptions sets the instrument options.
	SetInstrumentOptions(value instrument.Options) Options

	// Validate validates the options.
	Validate() error
}
