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
	"github.com/m3db/m3/src/dbnode/persist/fs/commitlog"
	"github.com/m3db/m3/src/dbnode/storage/bootstrap/result"
)

// Options represents the options for bootstrapping from commit logs
type Options interface {
	// Validate validates the options
	Validate() error

	// SetResultOptions sets the result options
	SetResultOptions(value result.Options) Options

	// ResultOptions returns the result options
	ResultOptions() result.Options

	// SetCommitLogOptions sets the commit log options
	SetCommitLogOptions(value commitlog.Options) Options

	// CommitLogOptions returns the commit log options
	CommitLogOptions() commitlog.Options

	// SetEncodingConcurrency sets the concurrency for encoding
	SetEncodingConcurrency(value int) Options

	// EncodingConcurrency returns the concurrency for encoding
	EncodingConcurrency() int

	// SetMergeShardConcurrency sets the concurrency for merging shards
	SetMergeShardsConcurrency(value int) Options

	// MergeShardConcurrency returns the concurrency for merging shards
	MergeShardsConcurrency() int
}
