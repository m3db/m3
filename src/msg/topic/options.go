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

package topic

import (
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
)

type serviceOptions struct {
	configServiceClient client.Client
	kvOpts              kv.OverrideOptions
}

// NewServiceOptions returns new ServiceOptions.
func NewServiceOptions() ServiceOptions {
	return &serviceOptions{
		kvOpts: kv.NewOverrideOptions(),
	}
}

func (opts *serviceOptions) ConfigService() client.Client {
	return opts.configServiceClient
}

func (opts *serviceOptions) SetConfigService(c client.Client) ServiceOptions {
	o := *opts
	o.configServiceClient = c
	return &o
}

func (opts *serviceOptions) KVOverrideOptions() kv.OverrideOptions {
	return opts.kvOpts
}

func (opts *serviceOptions) SetKVOverrideOptions(value kv.OverrideOptions) ServiceOptions {
	o := *opts
	o.kvOpts = value
	return &o
}
