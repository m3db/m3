// Copyright (c) 2020 Uber Technologies, Inc.
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

// Package io provides support for customizing io handling
package io

// Options are options for resettable readers and writers.
type Options interface {
	// ResettableReaderFn returns the reader function.
	SetResettableReaderFn(value ResettableReaderFn) Options

	// // ResettableReaderFn sets the reader function.
	ResettableReaderFn() ResettableReaderFn

	// ResettableWriterFn returns the writer function.
	SetResettableWriterFn(value ResettableWriterFn) Options

	// ResettableWriterFn sets the writer function.
	ResettableWriterFn() ResettableWriterFn
}

type options struct {
	resettableReaderFn ResettableReaderFn
	resettableWriterFn ResettableWriterFn
}

// NewOptions creates a new read write options.
func NewOptions() Options {
	return &options{
		resettableReaderFn: defaultResettableReaderFn(),
		resettableWriterFn: defaultResettableWriterFn(),
	}
}

func (opts *options) SetResettableReaderFn(value ResettableReaderFn) Options {
	o := *opts
	o.resettableReaderFn = value
	return &o
}

func (opts *options) ResettableReaderFn() ResettableReaderFn {
	return opts.resettableReaderFn
}

func (opts *options) SetResettableWriterFn(value ResettableWriterFn) Options {
	o := *opts
	o.resettableWriterFn = value
	return &o
}

func (opts *options) ResettableWriterFn() ResettableWriterFn {
	return opts.resettableWriterFn
}
