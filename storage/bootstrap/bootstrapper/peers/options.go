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

package peers

import (
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/bootstrap"
)

type options struct {
	bootstrapOpts bootstrap.Options
	client        client.AdminClient
	sleepFn       SleepFn
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		bootstrapOpts: bootstrap.NewOptions(),
		sleepFn:       time.Sleep,
	}
}

func (o *options) SetBootstrapOptions(value bootstrap.Options) Options {
	opts := *o
	opts.bootstrapOpts = value
	return &opts
}

func (o *options) BootstrapOptions() bootstrap.Options {
	return o.bootstrapOpts
}

func (o *options) SetAdminClient(value client.AdminClient) Options {
	opts := *o
	opts.client = value
	return &opts
}

func (o *options) AdminClient() client.AdminClient {
	return o.client
}

func (o *options) SetSleepFn(value SleepFn) Options {
	opts := *o
	opts.sleepFn = value
	return &opts
}

func (o *options) SleepFn() SleepFn {
	return o.sleepFn
}
