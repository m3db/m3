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

package namespace

const (
	// Namespace requires bootstrapping by default
	defaultNeedsBootstrap = true

	// Namespace requires flushing by default
	defaultNeedsFlush = true

	// Namespace writes go to commit logs by default
	defaultWritesToCommitLog = true

	// Namespace requires fileset cleanup by default
	defaultNeedsFilesetCleanup = true
)

type options struct {
	needsBootstrap      bool
	needsFlush          bool
	writesToCommitLog   bool
	needsFilesetCleanup bool
}

// NewOptions creates a new namespace options
func NewOptions() Options {
	return &options{
		needsBootstrap:      defaultNeedsBootstrap,
		needsFlush:          defaultNeedsFlush,
		writesToCommitLog:   defaultWritesToCommitLog,
		needsFilesetCleanup: defaultNeedsFilesetCleanup,
	}
}

func (o *options) NeedsBootstrap(value bool) Options {
	opts := *o
	opts.needsBootstrap = value
	return &opts
}

func (o *options) GetNeedsBootstrap() bool {
	return o.needsBootstrap
}

func (o *options) NeedsFlush(value bool) Options {
	opts := *o
	opts.needsFlush = value
	return &opts
}

func (o *options) GetNeedsFlush() bool {
	return o.needsFlush
}

func (o *options) WritesToCommitLog(value bool) Options {
	opts := *o
	opts.writesToCommitLog = value
	return &opts
}

func (o *options) GetWritesToCommitLog() bool {
	return o.writesToCommitLog
}

func (o *options) NeedsFilesetCleanup(value bool) Options {
	opts := *o
	opts.needsFilesetCleanup = value
	return &opts
}

func (o *options) GetNeedsFilesetCleanup() bool {
	return o.needsFilesetCleanup
}
