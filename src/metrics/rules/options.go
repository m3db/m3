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

package rules

import (
	"github.com/m3db/m3/src/metrics/filters"
	"github.com/m3db/m3/src/metrics/metric/id"
)

// Options provide a set of options for rule matching.
type Options interface {
	// SetTagsFilterOptions sets the tags filter options.
	SetTagsFilterOptions(value filters.TagsFilterOptions) Options

	// TagsFilterOptions returns the tags filter options.
	TagsFilterOptions() filters.TagsFilterOptions

	// SetNewRollupIDFn sets the new rollup id function.
	SetRollupIDer(value id.IDer) Options

	// NewRollupIDFn returns the new rollup id function.
	RollupIDer () id.IDer
}

type options struct {
	tagsFilterOpts filters.TagsFilterOptions
	rollupIDer  id.IDer
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{}
}

func (o *options) SetTagsFilterOptions(value filters.TagsFilterOptions) Options {
	opts := *o
	opts.tagsFilterOpts = value
	return &opts
}

func (o *options) TagsFilterOptions() filters.TagsFilterOptions {
	return o.tagsFilterOpts
}

func (o *options) SetRollupIDer(value id.IDer) Options {
	opts := *o
	opts.rollupIDer = value
	return &opts
}

func (o *options) RollupIDer() id.IDer {
	return o.rollupIDer
}
