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

	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/assert"
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
	inputs := []FetchBlockMetadataResult{
		NewFetchBlockMetadataResult(now, 0, nil, time.Time{}, nil),
		NewFetchBlockMetadataResult(now.Add(time.Second), 0, nil, time.Time{}, nil),
		NewFetchBlockMetadataResult(now.Add(-time.Second), 0, nil, time.Time{}, nil),
	}
	expected := []FetchBlockMetadataResult{inputs[2], inputs[0], inputs[1]}
	res := newPooledFetchBlockMetadataResults(nil, nil)
	for _, input := range inputs {
		res.Add(input)
	}
	res.Sort()
	require.Equal(t, expected, res.Results())
}

func TestFilteredBlocksMetadataIter(t *testing.T) {
	now := time.Now()
	sizes := []int64{1, 2, 3}
	checksums := []uint32{6, 7, 8}
	lastRead := now.Add(-100 * time.Millisecond)
	inputs := []FetchBlocksMetadataResult{
		NewFetchBlocksMetadataResult(ident.StringID("foo"),
			ident.EmptyTagIterator, newPooledFetchBlockMetadataResults(
				[]FetchBlockMetadataResult{
					NewFetchBlockMetadataResult(now.Add(-time.Second), sizes[0], &checksums[0], lastRead, nil),
				}, nil)),
		NewFetchBlocksMetadataResult(ident.StringID("bar"),
			ident.EmptyTagIterator, newPooledFetchBlockMetadataResults(
				[]FetchBlockMetadataResult{
					NewFetchBlockMetadataResult(now, sizes[1], &checksums[1], lastRead, nil),
					NewFetchBlockMetadataResult(now.Add(time.Second), sizes[2], &checksums[2], lastRead, errors.New("foo")),
					NewFetchBlockMetadataResult(now.Add(2*time.Second), 0, nil, lastRead, nil),
				}, nil)),
	}

	res := newPooledFetchBlocksMetadataResults(nil, nil)
	for _, input := range inputs {
		res.Add(input)
	}

	iter := NewFilteredBlocksMetadataIter(res)

	var actual []Metadata
	for iter.Next() {
		_, metadata := iter.Current()
		actual = append(actual, metadata)
	}
	require.NoError(t, iter.Err())

	expected := []Metadata{
		NewMetadata(ident.StringID("foo"), ident.Tags{}, now.Add(-time.Second),
			sizes[0], &checksums[0], lastRead),
		NewMetadata(ident.StringID("bar"), ident.Tags{}, now,
			sizes[1], &checksums[1], lastRead),
		NewMetadata(ident.StringID("bar"), ident.Tags{}, now.Add(2*time.Second),
			int64(0), nil, lastRead),
	}

	require.Equal(t, len(expected), len(actual))
	for i := range expected {
		assert.True(t, expected[i].ID.Equal(actual[i].ID))
		assert.True(t, expected[i].Start.Equal(actual[i].Start))
		assert.Equal(t, expected[i].Size, actual[i].Size)
		assert.Equal(t, expected[i].Checksum, actual[i].Checksum)
		assert.Equal(t, expected[i].LastRead, actual[i].LastRead)
	}
}
