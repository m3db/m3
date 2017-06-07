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

package node

import (
	"time"

	"github.com/m3db/m3x/instrument"
	xretry "github.com/m3db/m3x/retry"
)

// Configuration is a YAML wrapper around Options
type Configuration struct {
	OperationTimeout   *time.Duration          `yaml:"operationTimeout"`
	TransferBufferSize *int                    `yaml:"transferBufferSize"`
	Retry              *xretry.Configuration   `yaml:"retry"`
	Heartbeat          *HeartbeatConfiguration `yaml:"heartbeat"`
}

// Options returns `Options` corresponding to the provided struct values
func (c *Configuration) Options(iopts instrument.Options) Options {
	opts := NewOptions(iopts)
	if c.OperationTimeout != nil {
		opts = opts.SetOperationTimeout(*c.OperationTimeout)
	}
	if c.TransferBufferSize != nil {
		opts = opts.SetTransferBufferSize(*c.TransferBufferSize)
	}
	if c.Retry != nil {
		opts = opts.SetRetrier(c.Retry.NewRetrier(iopts.MetricsScope()))
	}
	if c.Heartbeat != nil {
		opts = opts.SetHeartbeatOptions(c.Heartbeat.Options())
	}
	return opts
}
