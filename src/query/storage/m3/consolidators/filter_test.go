// Copyright (c) 2020 Uber Technologies, Inc.
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

package consolidators

import (
	"testing"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/x/ident"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func b(s string) []byte { return []byte(s) }

func TestFilterTagIterator(t *testing.T) {
	it := ident.MustNewTagStringsIterator(
		"foo", "bar",
		"qux", "qaz",
	)

	tests := []struct {
		ex      bool
		filters models.Filters
	}{
		{true, models.Filters{{Name: b("foo")}}},
		{true, models.Filters{{Name: b("qux")}}},
		{false, models.Filters{{Name: b("bar")}}},

		{true, models.Filters{{Name: b("foo"),
			Values: [][]byte{b("bar")}}}},
		{true, models.Filters{{Name: b("foo"),
			Values: [][]byte{b("qaz"), b("bar")}}}},
		{false, models.Filters{{Name: b("foo"),
			Values: [][]byte{b("qaz")}}}},
		{false, models.Filters{{Name: b("foo"),
			Values: [][]byte{b("qaz"), b("quince")}}}},

		{true, models.Filters{{Name: b("qux"),
			Values: [][]byte{b("qaz")}}}},
	}

	for _, tt := range tests {
		assert.Equal(t, tt.ex, filterTagIterator(it, tt.filters))
	}

	type tag struct {
		name  string
		value string
	}

	ex := []tag{
		{name: "foo", value: "bar"},
		{name: "qux", value: "qaz"},
	}

	// NB: assert the iterator is rewinded and iteratable normally.
	for i := 0; it.Next(); i++ {
		tag := it.Current()
		assert.Equal(t, ex[i].name, tag.Name.String())
		assert.Equal(t, ex[i].value, tag.Value.String())
	}

	require.NoError(t, it.Err())
}
