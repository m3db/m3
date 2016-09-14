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
	"errors"
	"sort"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestSortFetchBlockResultByTimeAscending(t *testing.T) {
	now := time.Now()
	input := []FetchBlockResult{
		NewFetchBlockResult(now, nil, nil),
		NewFetchBlockResult(now.Add(time.Second), nil, nil),
		NewFetchBlockResult(now.Add(-time.Second), nil, nil),
	}
	expected := []FetchBlockResult{input[2], input[0], input[1]}
	sort.Sort(fetchBlockResultByTimeAscending(input))
	require.Equal(t, expected, input)
}

func TestSortFetchBlockMetadataResultByTimeAscending(t *testing.T) {
	now := time.Now()
	input := []FetchBlockMetadataResult{
		NewFetchBlockMetadataResult(now, nil, nil, nil),
		NewFetchBlockMetadataResult(now.Add(time.Second), nil, nil, nil),
		NewFetchBlockMetadataResult(now.Add(-time.Second), nil, nil, nil),
	}
	expected := []FetchBlockMetadataResult{input[2], input[0], input[1]}
	SortFetchBlockMetadataResultByTimeAscending(input)
	require.Equal(t, expected, input)
}

type testValue struct {
	id       string
	metadata Metadata
}

func TestFilteredBlocksMetadataIter(t *testing.T) {
	now := time.Now()
	start := now.Add(-time.Hour)
	end := now.Add(time.Hour)
	blockSize := 2 * time.Hour
	sizes := []int64{1, 2, 3, 4, 5}
	checksums := []uint32{6, 7, 8, 9, 10}
	res := []FetchBlocksMetadataResult{
		NewFetchBlocksMetadataResult("foo", []FetchBlockMetadataResult{
			NewFetchBlockMetadataResult(now.Add(-time.Second), &sizes[0], &checksums[0], nil),
			NewFetchBlockMetadataResult(now.Add(-4*time.Hour), &sizes[1], &checksums[1], nil),
			NewFetchBlockMetadataResult(now.Add(4*time.Hour), &sizes[2], &checksums[2], nil),
		}),
		NewFetchBlocksMetadataResult("bar", []FetchBlockMetadataResult{
			NewFetchBlockMetadataResult(now, &sizes[3], &checksums[3], nil),
			NewFetchBlockMetadataResult(now.Add(time.Second), &sizes[4], &checksums[4], errors.New("foo")),
			NewFetchBlockMetadataResult(now.Add(2*time.Second), nil, nil, nil),
		}),
	}
	iter := NewFilteredBlocksMetadataIter(start, end, blockSize, res)
	var actual []testValue
	for iter.Next() {
		id, metadata := iter.Current()
		actual = append(actual, testValue{id, metadata})
	}
	expected := []testValue{
		{"foo", NewMetadata(now.Add(-time.Second), sizes[0], &checksums[0])},
		{"bar", NewMetadata(now, sizes[3], &checksums[3])},
		{"bar", NewMetadata(now.Add(2*time.Second), int64(0), nil)},
	}
	require.Equal(t, expected, actual)
}
