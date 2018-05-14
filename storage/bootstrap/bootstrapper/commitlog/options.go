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

package commitlog

import (
	"errors"

	"github.com/m3db/m3db/persist/fs/commitlog"
	"github.com/m3db/m3db/storage/bootstrap/result"
)

const (
	defaultEncodingConcurrency       = 4
	defaultMergeShardConcurrency     = 4
	defaultShouldCacheSeriesMetadata = true
)

var (
	errEncodingConcurrencyPositive   = errors.New("encoding concurrency must be positive")
	errMergeShardConcurrencyPositive = errors.New("merge shard concurrency must be positive")
)

type options struct {
	resultOpts                result.Options
	commitLogOpts             commitlog.Options
	encodingConcurrency       int
	mergeShardConcurrency     int
	shouldCacheSeriesMetadata bool
}

// NewOptions creates new bootstrap options
func NewOptions() Options {
	return &options{
		resultOpts:                result.NewOptions(),
		commitLogOpts:             commitlog.NewOptions(),
		encodingConcurrency:       defaultEncodingConcurrency,
		mergeShardConcurrency:     defaultMergeShardConcurrency,
		shouldCacheSeriesMetadata: defaultShouldCacheSeriesMetadata,
	}
}

func (o *options) Validate() error {
	if o.encodingConcurrency <= 0 {
		return errEncodingConcurrencyPositive
	}
	if o.mergeShardConcurrency <= 0 {
		return errMergeShardConcurrencyPositive
	}
	return o.commitLogOpts.Validate()
}

func (o *options) SetResultOptions(value result.Options) Options {
	opts := *o
	opts.resultOpts = value
	return &opts
}

func (o *options) ResultOptions() result.Options {
	return o.resultOpts
}

func (o *options) SetCommitLogOptions(value commitlog.Options) Options {
	opts := *o
	opts.commitLogOpts = value
	return &opts
}

func (o *options) CommitLogOptions() commitlog.Options {
	return o.commitLogOpts
}

func (o *options) SetEncodingConcurrency(value int) Options {
	opts := *o
	opts.encodingConcurrency = value
	return &opts
}

func (o *options) EncodingConcurrency() int {
	return o.encodingConcurrency
}

func (o *options) SetMergeShardsConcurrency(value int) Options {
	opts := *o
	opts.mergeShardConcurrency = value
	return &opts
}

func (o *options) MergeShardsConcurrency() int {
	return o.mergeShardConcurrency
}

func (o *options) SetCacheSeriesMetadata(value bool) Options {
	opts := *o
	opts.shouldCacheSeriesMetadata = value
	return &opts
}

func (o *options) CacheSeriesMetadata() bool {
	return o.shouldCacheSeriesMetadata
}
