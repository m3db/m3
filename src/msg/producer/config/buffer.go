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

package config

import (
	"time"

	"github.com/m3db/m3msg/producer/buffer"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/retry"
)

// BufferConfiguration configs the buffer.
type BufferConfiguration struct {
	OnFullStrategy     *buffer.OnFullStrategy `yaml:"onFullStrategy"`
	MaxBufferSize      *int                   `yaml:"maxBufferSize"`
	MaxMessageSize     *int                   `yaml:"maxMessageSize"`
	CloseCheckInterval *time.Duration         `yaml:"closeCheckInterval"`
	ScanBatchSize      *int                   `yaml:"scanBatchSize"`
	CleanupRetry       *retry.Configuration   `yaml:"cleanupRetry"`
}

// NewOptions creates new buffer options.
func (c *BufferConfiguration) NewOptions(iOpts instrument.Options) buffer.Options {
	opts := buffer.NewOptions().SetOnFullStrategy(buffer.DropEarliest)
	if c.MaxBufferSize != nil {
		opts = opts.SetMaxBufferSize(*c.MaxBufferSize)
	}
	if c.MaxMessageSize != nil {
		opts = opts.SetMaxMessageSize(*c.MaxMessageSize)
	}
	if c.CloseCheckInterval != nil {
		opts = opts.SetCloseCheckInterval(*c.CloseCheckInterval)
	}
	if c.OnFullStrategy != nil {
		opts = opts.SetOnFullStrategy(*c.OnFullStrategy)
	}
	if c.ScanBatchSize != nil {
		opts = opts.SetScanBatchSize(*c.ScanBatchSize)
	}
	if c.CleanupRetry != nil {
		opts = opts.SetCleanupRetryOptions(c.CleanupRetry.NewOptions(iOpts.MetricsScope()))
	}
	return opts.SetInstrumentOptions(iOpts)
}
