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
	"github.com/m3db/m3x/log"
	"github.com/uber-go/tally"
)

type options struct {
	logger xlog.Logger
	scope  tally.Scope
}

// NewOptions creates new instrument options
func NewOptions() Options {
	return &options{
		logger: xlog.SimpleLogger,
		scope:  tally.NoopScope,
	}
}

func (o *options) Logger(value xlog.Logger) Options {
	opts := *o
	opts.logger = value
	return &opts
}

func (o *options) GetLogger() xlog.Logger {
	return o.logger
}

func (o *options) MetricsScope(value tally.Scope) Options {
	opts := *o
	opts.scope = value
	return &opts
}

func (o *options) GetMetricsScope() tally.Scope {
	return o.scope
}
