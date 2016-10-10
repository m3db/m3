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

import "github.com/m3db/m3db/ts"

// Options controls namespace behavior
type Options interface {
	// SetNeedsBootstrap sets whether this namespace requires bootstrapping
	SetNeedsBootstrap(value bool) Options

	// NeedsBootstrap returns whether this namespace requires bootstrapping
	NeedsBootstrap() bool

	// SetNeedsFlush sets whether the in-memory data for this namespace needs to be flushed
	SetNeedsFlush(value bool) Options

	// NeedsFlush returns whether the in-memory data for this namespace needs to be flushed
	NeedsFlush() bool

	// SetWritesToCommitLog sets whether writes for series in this namespace need to go to commit log
	SetWritesToCommitLog(value bool) Options

	// WritesToCommitLog returns whether writes for series in this namespace need to go to commit log
	WritesToCommitLog() bool

	// SetNeedsFilesetCleanup sets whether this namespace requires cleaning up fileset files
	SetNeedsFilesetCleanup(value bool) Options

	// NeedsFilesetCleanup returns whether this namespace requires cleaning up fileset files
	NeedsFilesetCleanup() bool

	// SetNeedsRepair sets whether the data for this namespace needs to be repaired
	SetNeedsRepair(value bool) Options

	// NeedsRepair returns whether the data for this namespace needs to be repaired
	NeedsRepair() bool
}

// Metadata represents namespace metadata information
type Metadata interface {
	// ID is the ID of the namespace
	ID() ts.ID

	// Options is the namespace options
	Options() Options
}
