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

package aggregation

import (
	"testing"

	"github.com/m3db/m3x/pool"

	"github.com/stretchr/testify/require"
)

func TestTypesPool(t *testing.T) {
	p := NewTypesPool(pool.NewObjectPoolOptions().SetSize(1))
	p.Init(func() Types {
		return make(Types, 0, maxTypeID)
	})

	aggTypes := p.Get()
	require.Equal(t, maxTypeID, cap(aggTypes))
	require.Equal(t, 0, len(aggTypes))
	aggTypes = append(aggTypes, P9999)

	p.Put(aggTypes)

	aggTypes2 := p.Get()
	require.Equal(t, maxTypeID, cap(aggTypes2))
	require.Equal(t, 0, len(aggTypes2))

	aggTypes2 = append(aggTypes2, Last)
	require.Equal(t, aggTypes[0], aggTypes2[0])
	require.Equal(t, aggTypes, aggTypes2)

	aggTypes3 := p.Get()
	require.NotEqual(t, aggTypes, aggTypes3)
}
