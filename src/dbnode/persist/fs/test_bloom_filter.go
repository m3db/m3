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

package fs

import (
	"github.com/m3db/bloom"
)

// TestManagedBloomFilter is a container object that implements a no-op lifecycle
// management on-top of a BloomFilter. I.E it wraps a bloom filter such that
// all resources are released when the Close() method is called. It's also safe
// for concurrent access
type TestManagedBloomFilter struct {
	*bloom.ConcurrentReadOnlyBloomFilter
}

// Close closes the bloom filter, Releasing any held resources
func (bf *TestManagedBloomFilter) Close() error {
	return nil
}

// NewTestManagedBloomFilter returns a test implementation of ManagedBloomFilter.
// This test implementation is backed by an in-memory implementation of bloom filter
func NewTestManagedBloomFilter(m uint, k uint, data []byte) ManagedBloomFilter {
	return &TestManagedBloomFilter{bloom.NewConcurrentReadOnlyBloomFilter(m, k, data)}
}
