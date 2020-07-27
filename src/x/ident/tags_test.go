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

package ident

import (
	"fmt"
	"testing"
	"unsafe"

	"github.com/m3db/m3/src/x/checked"
	"github.com/stretchr/testify/require"
)

func TestTagsID(t *testing.T) {
	tagsA := NewTags(
		StringTag("hello", "there"),
	)
	tagsB := NewTags(
		StringTag("foo", "bar"),
		StringTag("and", "done"),
	)
	// tagsC := NewTags(
	// 	StringTag("namespace", "metrics_0_30m"),
	// 	StringTag("namespace", "metrics_0_30m"),
	// 	StringTag("namespace", "metrics_0_30m"),
	// 	StringTag("namespace", "metrics_0_30m"),
	// 	StringTag("namespace", "metrics_0_30m"),
	// )
	fmt.Println(tagsA.values)
	fmt.Println(tagsA.ToID())
	fmt.Println(tagsB.ToID())
	fmt.Println(ToTags(tagsA.ToID(), nil).Values())
	fmt.Println(ToTags(tagsB.ToID(), nil).Values())
	fmt.Println(IDMatchesTags(StringID("{foo=\"bar\",and=\"done\"}"), &tagsB))
}

func TestBytes(t *testing.T) {
	opts := checked.NewBytesOptions().SetStringTable(checked.NewStringTable())

	a := []byte("abc")
	b := BytesID(checked.NewBytes(a, opts).Bytes())
	c := BytesID(checked.NewBytes(a, opts).Bytes())
	fmt.Println(string(a), b.String(), c.String())
	a[0] = 'A'
	b[1] = 'B'
	c[2] = 'C'
	fmt.Println(string(a), b.String(), c.String())

	d := []byte("abc")
	e := checked.NewBytes(d, nil)
	e.IncRef()
	f := BytesID(e.Bytes())
	fmt.Println(string(d), string(e.Bytes()), f.String())
	d[0] = 'A'
	fmt.Println(string(d), string(e.Bytes()), f.String())

	fmt.Println(unsafe.Sizeof(a))
	fmt.Println(unsafe.Sizeof(b))
	fmt.Println(unsafe.Sizeof(c))
	fmt.Println(unsafe.Sizeof(d))
	fmt.Println(unsafe.Sizeof(e))
	fmt.Println(unsafe.Sizeof(f))

	ts1 := testStruct{}
	ts2 := testStruct{}
	fmt.Printf("%p\n", &ts1)
	fmt.Printf("%p\n", &ts2)
}

type testStruct struct {
	b int
}

func TestTagsUnequalLength(t *testing.T) {
	tagsA := NewTags(
		StringTag("hello", "there"),
	)
	tagsB := NewTags(
		StringTag("foo", "bar"),
		StringTag("and", "done"),
	)

	require.False(t, tagsA.Equal(tagsB))
	require.False(t, tagsB.Equal(tagsA))
}

func TestTagsUnequalOrder(t *testing.T) {
	tagsA := NewTags(
		StringTag("foo", "bar"),
		StringTag("hello", "there"),
	)
	tagsB := NewTags(
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	)

	require.False(t, tagsA.Equal(tagsB))
	require.False(t, tagsB.Equal(tagsA))
}

func TestTagsEqual(t *testing.T) {
	tagsA := NewTags(
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	)
	tagsB := NewTags(
		StringTag("hello", "there"),
		StringTag("foo", "bar"),
	)

	require.True(t, tagsA.Equal(tagsB))
	require.True(t, tagsB.Equal(tagsA))
}
