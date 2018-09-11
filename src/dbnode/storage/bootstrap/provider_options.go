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

package bootstrap

import (
	"errors"

	"github.com/m3db/m3/src/dbnode/client"
)

const (
	// defaultCacheSeriesMetadata declares that by default bootstrap providers should
	// cache series metadata between runs.
	defaultCacheSeriesMetadata = true
)

var (
	errAdminClientShouldNotBeNil = errors.New("admin client should not be nil")
)

type processOptions struct {
	cacheSeriesMetadata bool
	adminClient         client.AdminClient
}

// NewProcessOptions creates new bootstrap run options
func NewProcessOptions() ProcessOptions {
	return &processOptions{
		cacheSeriesMetadata: defaultCacheSeriesMetadata,
		adminClient:         nil,
	}
}

func (o *processOptions) Validate() error {
	if o.adminClient == nil {
		return errAdminClientShouldNotBeNil
	}

	return nil
}

func (o *processOptions) SetCacheSeriesMetadata(value bool) ProcessOptions {
	opts := *o
	opts.cacheSeriesMetadata = value
	return &opts
}

func (o *processOptions) CacheSeriesMetadata() bool {
	return o.cacheSeriesMetadata
}

func (o *processOptions) SetAdminClient(value client.AdminClient) ProcessOptions {
	opts := *o
	opts.adminClient = value
	return &opts
}

func (o *processOptions) AdminClient() client.AdminClient {
	return o.adminClient
}
