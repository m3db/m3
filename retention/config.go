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

package retention

import (
	"time"
)

// Configuration is the set of knobs to configure retention options
type Configuration struct {
	RetentionPeriod                       *time.Duration
	BlockSize                             *time.Duration
	BufferFuture                          *time.Duration
	BufferPast                            *time.Duration
	BufferDrain                           *time.Duration
	BlockDataExpiry                       *bool
	BlockDataExpiryAfterNotAccessedPeriod *time.Duration
}

// Options returns `Options` corresponding to the provided struct values
func (c *Configuration) Options() Options {
	opts := NewOptions()
	if v := c.RetentionPeriod; v != nil {
		opts = opts.SetRetentionPeriod(*v)
	}
	if v := c.BlockSize; v != nil {
		opts = opts.SetBlockSize(*v)
	}
	if v := c.BufferFuture; v != nil {
		opts = opts.SetBufferFuture(*v)
	}
	if v := c.BufferPast; v != nil {
		opts = opts.SetBufferPast(*v)
	}
	if v := c.BufferDrain; v != nil {
		opts = opts.SetBufferDrain(*v)
	}
	if v := c.BlockDataExpiry; v != nil {
		opts = opts.SetBlockDataExpiry(*v)
	}
	if v := c.BlockDataExpiryAfterNotAccessedPeriod; v != nil {
		opts = opts.SetBlockDataExpiryAfterNotAccessedPeriod(*v)
	}
	return opts
}
