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

// Options controls namespace behavior
type Options interface {
	// NeedsBootstrap sets whether this namespace requires bootstrapping
	NeedsBootstrap(value bool) Options

	// GetNeedsBootstrap returns whether this namespace requires bootstrapping
	GetNeedsBootstrap() bool

	// NeedsFlush sets whether the in-memory data for this namespace needs to be flushed
	NeedsFlush(value bool) Options

	// GetNeedsFlush returns whether the in-memory data for this namespace needs to be flushed
	GetNeedsFlush() bool

	// WritesToCommitLog sets whether writes for series in this namespace need to go to commit log
	WritesToCommitLog(value bool) Options

	// GetWritesToCommitLog returns whether writes for series in this namespace need to go to commit log
	GetWritesToCommitLog() bool

	// NeedsFilesetCleanup sets whether this namespace requires cleaning up fileset files
	NeedsFilesetCleanup(value bool) Options

	// GetNeedsFilesetCleanup returns whether this namespace requires cleaning up fileset files
	GetNeedsFilesetCleanup() bool
}

// Metadata represents namespace metadata information
type Metadata interface {
	// Name is the name of the namespace
	Name() string

	// Options is the namespace options
	Options() Options
}
