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

package storage

import (
	"errors"
	"time"

	"github.com/m3db/m3db/retention"
)

var (
	errNoRetentionOptions = errors.New("no retention options in file op options")
	errJitterTooBig       = errors.New("file op jitter is not smaller than block size")

	defaultFileOpJitter = 5 * time.Minute
)

type fileOpOptions struct {
	retentionOpts retention.Options
	jitter        time.Duration
}

// NewFileOpOptions creates a new file op options
func NewFileOpOptions() FileOpOptions {
	return &fileOpOptions{
		retentionOpts: retention.NewOptions(),
		jitter:        defaultFileOpJitter,
	}
}

func (o *fileOpOptions) SetRetentionOptions(value retention.Options) FileOpOptions {
	opts := *o
	opts.retentionOpts = value
	return &opts
}

func (o *fileOpOptions) RetentionOptions() retention.Options {
	return o.retentionOpts
}

func (o *fileOpOptions) SetJitter(value time.Duration) FileOpOptions {
	opts := *o
	opts.jitter = value
	return &opts
}

func (o *fileOpOptions) Jitter() time.Duration {
	return o.jitter
}

func (o *fileOpOptions) Validate() error {
	if o.retentionOpts == nil {
		return errNoRetentionOptions
	}
	if o.jitter >= o.retentionOpts.BlockSize() {
		return errJitterTooBig
	}
	return nil
}
