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

package namespace

import (
	"testing"

	"github.com/m3db/m3x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestMapEmpty(t *testing.T) {
	_, err := NewMap(nil)
	require.Error(t, err)
}

func TestMapSingleElement(t *testing.T) {
	var (
		opts = NewOptions()
		id   = ident.StringID("someID")
	)
	md1, err := NewMetadata(id, opts)
	require.NoError(t, err)
	metadatas := []Metadata{md1}
	nsMap, err := NewMap(metadatas)
	require.NoError(t, err)
	require.Equal(t, 1, len(nsMap.IDs()))
	require.Equal(t, id.String(), nsMap.IDs()[0].String())
	require.Equal(t, 1, len(nsMap.Metadatas()))
	md := nsMap.Metadatas()[0]
	require.Equal(t, id.String(), md.ID().String())
	require.Equal(t, opts, md.Options())
}

func TestMapMultipleElements(t *testing.T) {
	var (
		opts1 = NewOptions()
		opts2 = opts1.SetRepairEnabled(true)
		id1   = ident.StringID("someID1")
		id2   = ident.StringID("someID2")
	)
	md1, err := NewMetadata(id1, opts1)
	require.NoError(t, err)
	md2, err := NewMetadata(id2, opts2)
	require.NoError(t, err)
	metadatas := []Metadata{md1, md2}
	nsMap, err := NewMap(metadatas)
	require.NoError(t, err)

	require.Equal(t, 2, len(nsMap.IDs()))
	ids := nsMap.IDs()
	require.True(t, ids[0].Equal(id1) || ids[1].Equal(id1))
	require.True(t, ids[0].Equal(id2) || ids[1].Equal(id2))

	require.Equal(t, 2, len(nsMap.Metadatas()))
	mds := nsMap.Metadatas()
	require.True(t, id1.Equal(mds[0].ID()) || id1.Equal(mds[1].ID()))
	require.True(t, id2.Equal(mds[0].ID()) || id2.Equal(mds[1].ID()))
}

func testMap(t *testing.T) Map {
	var (
		opts1 = NewOptions()
		opts2 = opts1.SetRepairEnabled(true)
		id1   = ident.StringID("someID1")
		id2   = ident.StringID("someID2")
	)
	md1, err := NewMetadata(id1, opts1)
	require.NoError(t, err)
	md2, err := NewMetadata(id2, opts2)
	require.NoError(t, err)
	metadatas := []Metadata{md1, md2}
	reg, err := NewMap(metadatas)
	require.NoError(t, err)
	return reg
}

func TestMapEqualsTrue(t *testing.T) {
	r1 := testMap(t)
	require.True(t, r1.Equal(r1))

	r2 := testMap(t)
	require.True(t, r1.Equal(r2))
	require.True(t, r2.Equal(r1))
}

func TestMapValidateDuplicateID(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		opts = NewMockOptions(ctrl)
		id   = ident.StringID("someID")
	)
	opts.EXPECT().Validate().Return(nil).AnyTimes()

	md1, err := NewMetadata(id, opts)
	require.NoError(t, err)

	md2, err := NewMetadata(id, opts)
	require.NoError(t, err)

	metadatas := []Metadata{md1, md2}
	_, err = NewMap(metadatas)
	require.Error(t, err)
}
