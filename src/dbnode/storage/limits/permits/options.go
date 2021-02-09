// Copyright (c) 2021 Uber Technologies, Inc.
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

package permits

type options struct {
	seriesReadManager Manager
}

// NewOptions return a new set of default permit managers.
func NewOptions() Options {
	return &options{
		seriesReadManager: NewNoOpPermitsManager(),
	}
}

// SetSeriesReadPermitsManager sets the series read permits manager.
func (o *options) SetSeriesReadPermitsManager(value Manager) Options {
	opts := *o
	opts.seriesReadManager = value
	return &opts
}

// SeriesReadPermitsManager returns the series read permits manager.
func (o *options) SeriesReadPermitsManager() Manager {
	return o.seriesReadManager
}
