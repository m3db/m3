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

import "github.com/m3db/m3metrics/filters"

// NewIDFn creates a new metric ID based on the metric name and metric tag pairs.
type NewIDFn func(name []byte, tags []TagPair) []byte

// Options provide a set of options for rule matching.
type Options interface {
	// SetNewSortedTagIteratorFn sets the new sorted tag iterator function.
	SetNewSortedTagIteratorFn(value filters.NewSortedTagIteratorFn) Options

	// NewSortedTagIteratorFn returns the new sorted tag iterator function.
	NewSortedTagIteratorFn() filters.NewSortedTagIteratorFn

	// SetNewIDFn sets the new id function.
	SetNewIDFn(value NewIDFn) Options

	// NewIDFn returns the new id function.
	NewIDFn() NewIDFn
}

type options struct {
	iterFn  filters.NewSortedTagIteratorFn
	newIDFn NewIDFn
}

// NewOptions creates a new set of options.
func NewOptions() Options {
	return &options{}
}

func (o *options) SetNewSortedTagIteratorFn(value filters.NewSortedTagIteratorFn) Options {
	opts := o
	opts.iterFn = value
	return opts
}

func (o *options) NewSortedTagIteratorFn() filters.NewSortedTagIteratorFn {
	return o.iterFn
}

func (o *options) SetNewIDFn(value NewIDFn) Options {
	opts := o
	opts.newIDFn = value
	return opts
}

func (o *options) NewIDFn() NewIDFn {
	return o.newIDFn
}
