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

package client

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/storage/block"
	"github.com/m3db/m3/src/dbnode/topology"
	"github.com/m3db/m3/src/x/checked"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testHostBlock struct {
	host  topology.Host
	block block.Metadata
}

func TestPeerBlockMetadataIter(t *testing.T) {
	var (
		inputCh = make(chan receivedBlockMetadata)
		errCh   = make(chan error, 1)
		err     = errors.New("foo")
	)

	opts := newHostQueueTestOptions()
	peer := newTestHostQueue(opts)
	now := xtime.Now()
	checksums := []uint32{1, 2, 3}
	lastRead := now.Add(-100 * time.Millisecond)
	inputs := []receivedBlockMetadata{
		{
			peer:        peer,
			id:          ident.StringID("foo"),
			encodedTags: mustEncodeTags(t, ident.NewTags(ident.StringTag("aaa", "bbb"))),
			block: blockMetadata{
				start: now, size: int64(1), checksum: &checksums[0],
			},
		},
		{
			peer:        peer,
			id:          ident.StringID("foo"),
			encodedTags: mustEncodeTags(t, ident.NewTags(ident.StringTag("aaa", "bbb"))),
			block: blockMetadata{
				start: now.Add(time.Second), size: int64(2), checksum: &checksums[1], lastRead: lastRead,
			},
		},
		{
			peer:        peer,
			id:          ident.StringID("bar"),
			encodedTags: mustEncodeTags(t, ident.NewTags(ident.StringTag("ccc", "ddd"))),
			block: blockMetadata{
				start: now, size: int64(3), checksum: &checksums[2], lastRead: lastRead,
			},
		},
		{
			peer:        peer,
			id:          ident.StringID("baz"),
			encodedTags: mustEncodeTags(t, ident.NewTags(ident.StringTag("eee", "fff"))),
			block: blockMetadata{
				start: now, size: int64(4), checksum: nil, lastRead: lastRead,
			},
		},
	}

	go func() {
		for _, input := range inputs {
			inputCh <- input
		}
		errCh <- err
		close(inputCh)
	}()

	var actual []testHostBlock
	it := newMetadataIter(inputCh, errCh,
		testTagDecodingPool.Get(), testIDPool)
	for it.Next() {
		host, curr := it.Current()
		result := block.NewMetadata(curr.ID, curr.Tags, curr.Start, curr.Size,
			curr.Checksum, curr.LastRead)
		actual = append(actual, testHostBlock{host, result})
	}

	expected := []testHostBlock{
		{h, block.NewMetadata(ident.StringID("foo"),
			ident.NewTags(ident.StringTag("aaa", "bbb")),
			inputs[0].block.start, inputs[0].block.size,
			inputs[0].block.checksum, inputs[0].block.lastRead)},
		{h, block.NewMetadata(ident.StringID("foo"),
			ident.NewTags(ident.StringTag("aaa", "bbb")),
			inputs[1].block.start, inputs[1].block.size,
			inputs[1].block.checksum, inputs[1].block.lastRead)},
		{h, block.NewMetadata(ident.StringID("bar"),
			ident.NewTags(ident.StringTag("ccc", "ddd")),
			inputs[2].block.start, inputs[2].block.size,
			inputs[2].block.checksum, inputs[2].block.lastRead)},
		{h, block.NewMetadata(ident.StringID("baz"),
			ident.NewTags(ident.StringTag("eee", "fff")),
			inputs[3].block.start, inputs[3].block.size,
			inputs[3].block.checksum, inputs[3].block.lastRead)},
	}

	require.Equal(t, len(expected), len(actual))
	for i, expected := range expected {
		actual := actual[i]
		assert.Equal(t, expected.host.String(), actual.host.String())
		assert.True(t, expected.block.ID.Equal(actual.block.ID))
		tagMatcher := ident.NewTagIterMatcher(ident.NewTagsIterator(expected.block.Tags))
		assert.True(t, tagMatcher.Matches(ident.NewTagsIterator(actual.block.Tags)))
		assert.True(t, expected.block.Start.Equal(actual.block.Start))
		assert.Equal(t, expected.block.Size, actual.block.Size)
		assert.Equal(t, expected.block.Checksum, actual.block.Checksum)
		assert.True(t, expected.block.LastRead.Equal(actual.block.LastRead))
	}
}

func mustEncodeTags(t *testing.T, tags ident.Tags) checked.Bytes {
	encoder := testTagEncodingPool.Get()
	err := encoder.Encode(ident.NewTagsIterator(tags))
	require.NoError(t, err)
	data, ok := encoder.Data()
	require.True(t, ok)
	return data
}
