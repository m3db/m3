// Copyright (c) 2023 Uber Technologies, Inc.
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

package filter

import (
	"testing"
	
	"github.com/m3db/m3/src/msg/producer"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestPercentageFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mm := producer.NewMockMessage(ctrl)

	f0 := NewPercentageFilter(0)
	require.False(t, f0(mm))

	f1 := NewPercentageFilter(1)
	require.True(t, f1(mm))
}

var filterResult bool

/*
benchmark baseline:

goos: darwin
goarch: amd64
pkg: github.com/m3db/m3/src/aggregator/aggregator/handler/filter
cpu: Intel(R) Core(TM) i7-9750H CPU @ 2.60GHz
BenchmarkPercentageFilter
BenchmarkPercentageFilter-12    	280085259	         4.232 ns/op
PASS
*/
func BenchmarkPercentageFilter(b *testing.B) {
	f := NewPercentageFilter(0.5)
	var r bool
	for i := 0; i < b.N; i++ {
		r = f(nil)
	}
	filterResult = r
}
