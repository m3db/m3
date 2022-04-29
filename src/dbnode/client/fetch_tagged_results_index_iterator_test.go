// Copyright (c) 2018 Uber Technologies, Inc.
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
	"testing"

	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/serialize"

	"github.com/stretchr/testify/require"
)

func TestFetchTaggedResultsIndexIterator(t *testing.T) {
	pools := newTestFetchTaggedPools()

	opts := serialize.NewTagEncoderOptions()
	popts := pool.NewObjectPoolOptions().SetSize(1)
	encPool := serialize.NewTagEncoderPool(opts, popts)
	encPool.Init()

	for _, tc := range []struct {
		name string
		nses []ident.ID
		ids  []ident.ID
		tags []ident.TagIterator
	}{
		{
			"testcase0",
			[]ident.ID{ident.StringID("ns0"), ident.StringID("ns1"), ident.StringID("ns2")},
			[]ident.ID{ident.StringID("id0"), ident.StringID("id1"), ident.StringID("id2")},
			[]ident.TagIterator{
				ident.NewTagsIterator(ident.NewTags(
					ident.StringTag("tn0", "tv0"))),
				ident.NewTagsIterator(ident.NewTags(
					ident.StringTag("tn0", "tv0"), ident.StringTag("tn1", "tv1"))),
				ident.NewTagsIterator(ident.NewTags(
					ident.StringTag("tn0", "tv0"), ident.StringTag("tn1", "tv1"), ident.StringTag("tn2", "tv2"))),
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			iter := newTaggedIDsIterator(pools)
			// initialize iter
			for i := range tc.nses {
				ns := tc.nses[i]
				id := tc.ids[i]
				tags := tc.tags[i].Duplicate()
				enc := encPool.Get()
				err := enc.Encode(tags)
				require.NoError(t, err)
				data, ok := enc.Data()
				require.True(t, ok)
				iter.addBacking(ns.Bytes(), id.Bytes(), data.Bytes())
			}

			// validate iter
			for i := range tc.nses {
				require.True(t, iter.Next())
				obsNs, obsID, obsTags := iter.Current()
				expNs, expID, expTags := tc.nses[i], tc.ids[i], tc.tags[i].Duplicate()
				require.Equal(t, expNs.String(), obsNs.String())
				require.Equal(t, expID.String(), obsID.String())
				require.True(t, ident.NewTagIterMatcher(expTags).Matches(obsTags))
			}
		})
	}
}
