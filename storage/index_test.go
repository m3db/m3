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

package storage

import (
	"testing"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"

	"github.com/stretchr/testify/require"
)

func TestDatabaseIndexCtor(t *testing.T) {
	idx, err := newDatabaseIndex(testDatabaseOptions())
	require.NoError(t, err)
	require.NotNil(t, idx)
}

func TestDatabaseIndexDocConversion(t *testing.T) {
	Idx, err := newDatabaseIndex(testDatabaseOptions())
	require.NoError(t, err)

	idx, ok := Idx.(*dbIndex)
	require.True(t, ok)

	id := ident.StringID("foo")
	ns := ident.StringID("bar")
	tags := ident.Tags{
		ident.StringTag("name", "value"),
	}

	d := idx.doc(ns, id, tags)
	require.Equal(t, "foo", string(d.ID))
	require.Len(t, d.Fields, 2)
	require.Equal(t, index.ReservedFieldNameNamespace, d.Fields[0].Name)
	require.Equal(t, "bar", string(d.Fields[0].Value))
	require.Equal(t, "name", string(d.Fields[1].Name))
	require.Equal(t, "value", string(d.Fields[1].Value))
}

func TestDatabaseIndexInsertQuery(t *testing.T) {
	idx, err := newDatabaseIndex(testDatabaseOptions())
	require.NoError(t, err)

	var (
		id   = ident.StringID("foo")
		ns   = ident.StringID("bar")
		tags = ident.Tags{
			ident.StringTag("name", "value"),
		}
		ctx = context.NewContext()
	)
	require.NoError(t, idx.Write(ns, id, tags))
	res, err := idx.Query(ctx, index.Query{
		segment.Query{
			Conjunction: segment.AndConjunction,
			Filters: []segment.Filter{
				segment.Filter{
					FieldName:        []byte("name"),
					FieldValueFilter: []byte("val.*"),
					Regexp:           true,
				},
			},
		},
	}, index.QueryOptions{})
	require.NoError(t, err)

	require.True(t, res.Exhaustive)
	iter := res.Iterator
	require.True(t, iter.Next())

	cNs, cID, cTags := iter.Current()
	require.Equal(t, "foo", cID.String())
	require.Equal(t, "bar", cNs.String())
	require.Len(t, cTags, 1)
	require.Equal(t, "name", cTags[0].Name.String())
	require.Equal(t, "value", cTags[0].Value.String())
	require.False(t, iter.Next())
	require.Nil(t, iter.Err())
}
