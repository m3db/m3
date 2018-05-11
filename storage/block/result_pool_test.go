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

package block

import (
	"testing"
	"time"

	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func testResultPoolOptions(size int) pool.ObjectPoolOptions {
	return pool.NewObjectPoolOptions().SetSize(size)
}

func TestFetchBlockMetadataResultsPoolResetOnPut(t *testing.T) {
	p := NewFetchBlockMetadataResultsPool(testResultPoolOptions(1), 64)
	res := p.Get()

	// Make res non-empty
	res.Add(NewFetchBlockMetadataResult(time.Now(), 0, nil, time.Time{}, nil))
	require.Equal(t, 1, len(res.Results()))

	// Return res to pool
	p.Put(res)

	// Verify res has been reset
	res = p.Get()
	require.Equal(t, 0, len(res.Results()))
}

func TestFetchBlockMetadataResultsPoolRejectLargeSliceOnPut(t *testing.T) {
	p := NewFetchBlockMetadataResultsPool(testResultPoolOptions(1), 64)
	res := p.Get()

	// Make res a large slice
	iter := 1024
	for i := 0; i < iter; i++ {
		res.Add(NewFetchBlockMetadataResult(time.Now(), 0, nil, time.Time{}, nil))
	}
	require.True(t, cap(res.Results()) > 64)

	// Return res to pool
	p.Put(res)

	// Verify res wasn't put into pool
	res = p.Get()
	require.Equal(t, 64, cap(res.Results()))
}

func TestFetchBlocksMetadataResultsPoolResetOnPut(t *testing.T) {
	p := NewFetchBlocksMetadataResultsPool(testResultPoolOptions(1), 64)
	res := p.Get()

	// Make res non-empty
	res.Add(NewFetchBlocksMetadataResult(ident.StringID("foo"),
		ident.EmptyTagIterator, NewFetchBlockMetadataResults()))
	require.Equal(t, 1, len(res.Results()))

	// Return res to pool
	p.Put(res)

	// Verify res has been reset
	res = p.Get()
	require.Equal(t, 0, len(res.Results()))
}

func TestFetchBlocksMetadataResultsPoolRejectLargeSliceOnPut(t *testing.T) {
	p := NewFetchBlocksMetadataResultsPool(testResultPoolOptions(1), 64)
	res := p.Get()

	// Make res a large slice
	iter := 1024
	for i := 0; i < iter; i++ {
		res.Add(NewFetchBlocksMetadataResult(ident.StringID("foo"),
			ident.EmptyTagIterator, NewFetchBlockMetadataResults()))
	}
	require.True(t, cap(res.Results()) > 64)

	// Return res to pool
	p.Put(res)

	// Verify res wasn't put into pool
	res = p.Get()
	require.Equal(t, 64, cap(res.Results()))
}
