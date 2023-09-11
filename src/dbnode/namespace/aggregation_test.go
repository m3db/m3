// Copyright (c) 2020  Uber Technologies, Inc.
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
	"time"

	"github.com/stretchr/testify/require"
)

func TestAggregationEqual(t *testing.T) {
	attrs, err := NewAggregatedAttributes(5*time.Minute, NewDownsampleOptions(true))
	require.NoError(t, err)

	opts1 := NewAggregationOptions().
		SetAggregations([]Aggregation{
			NewUnaggregatedAggregation(),
			NewAggregatedAggregation(attrs),
		})
	opts2 := NewAggregationOptions().
		SetAggregations([]Aggregation{
			NewUnaggregatedAggregation(),
			NewAggregatedAggregation(attrs),
		})
	opts3 := NewAggregationOptions().
		SetAggregations([]Aggregation{
			NewUnaggregatedAggregation(),
		})

	require.Equal(t, opts1, opts2)
	require.NotEqual(t, opts1, opts3)
}

func TestAggregationAttributesValidation(t *testing.T) {
	_, err := NewAggregatedAttributes(5*time.Minute, NewDownsampleOptions(true))
	require.NoError(t, err)

	_, err = NewAggregatedAttributes(-5*time.Minute, NewDownsampleOptions(true))
	require.Error(t, err)
}
