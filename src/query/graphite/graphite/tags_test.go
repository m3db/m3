// Copyright (c) 2019 Uber Technologies, Inc.
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

package graphite

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestTagName(t *testing.T) {
	for i := 0; i < 2*numPreFormattedTagNames; i++ {
		expected := []byte("__g" + fmt.Sprint(i) + "__")
		require.Equal(t, expected, TagName(i))
	}
}

func TestTagIndex(t *testing.T) {
	for _, test := range []struct {
		tag        []byte
		isGraphite bool
		index      int
	}{
		{
			tag:        []byte("__g0__"),
			isGraphite: true,
		},
		{
			tag:        []byte("__g11__"),
			isGraphite: true,
			index:      11,
		},
		{
			tag: []byte("__g__"),
		},
		{
			tag: []byte("__gabc__"),
		},
		{
			tag: []byte("_g1__"),
		},
		{
			tag: []byte("__g1_"),
		},
	} {
		t.Run(string(test.tag), func(t *testing.T) {
			index, exists := TagIndex(test.tag)
			require.Equal(t, test.isGraphite, exists)
			require.Equal(t, test.index, index)
		})
	}
}
