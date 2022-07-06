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

package builder

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"

	"github.com/stretchr/testify/require"
)

func TestTermsReuse(t *testing.T) {
	terms := newTerms(NewOptions())

	require.NoError(t, terms.post([]byte("term"), postings.ID(1), indexJobEntryOptions{}))
	require.Equal(t, terms.size(), 1)
	require.Equal(t, terms.postings.Len(), 1)
	require.Equal(t, terms.postingsListUnion.CountSlow(), 1)

	terms.reset()
	require.Equal(t, terms.size(), 0)
	require.Equal(t, terms.postings.Len(), 0)
	require.Equal(t, terms.postingsListUnion.CountSlow(), 0)
}
