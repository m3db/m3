// Copyright (c) 2019 Uber Technologies, Inc.
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

package block

import "github.com/m3db/m3/src/query/ts"

var (
	defaultDatapointTransform  = func(dp ts.Datapoint) ts.Datapoint { return dp }
	defaultMetaTransform       = func(m Metadata) Metadata { return m }
	defaultSeriesMetaTransform = func(m []SeriesMeta) []SeriesMeta { return m }
)

type lazyOpts struct {
	datapointTransform  DatapointTransform
	metaTransform       MetaTransform
	seriesMetaTransform SeriesMetaTransform
}

// NewLazyOptions creates LazyOpts with default values.
func NewLazyOptions() LazyOptions {
	return &lazyOpts{
		datapointTransform:  defaultDatapointTransform,
		metaTransform:       defaultMetaTransform,
		seriesMetaTransform: defaultSeriesMetaTransform,
	}
}

func (o *lazyOpts) SetDatapointTransform(t DatapointTransform) LazyOptions {
	opts := *o
	opts.datapointTransform = t
	return &opts
}

func (o *lazyOpts) DatapointTransform() DatapointTransform {
	return o.datapointTransform
}

func (o *lazyOpts) SetMetaTransform(t MetaTransform) LazyOptions {
	opts := *o
	opts.metaTransform = t
	return &opts
}

func (o *lazyOpts) MetaTransform() MetaTransform {
	return o.metaTransform
}

func (o *lazyOpts) SetSeriesMetaTransform(t SeriesMetaTransform) LazyOptions {
	opts := *o
	opts.seriesMetaTransform = t
	return &opts
}

func (o *lazyOpts) SeriesMetaTransform() SeriesMetaTransform {
	return o.seriesMetaTransform
}
