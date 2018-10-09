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

package filter

import (
	"testing"

	"github.com/m3db/m3/src/aggregator/sharding"
	"github.com/m3db/m3msg/producer"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestShardSetFilter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ss := sharding.MustParseShardSet("0..511")
	f := NewShardSetFilter(ss)

	mm := producer.NewMockMessage(ctrl)
	mm.EXPECT().Shard().Return(uint32(0))
	require.True(t, f(mm))
	mm.EXPECT().Shard().Return(uint32(10))
	require.True(t, f(mm))
	mm.EXPECT().Shard().Return(uint32(511))
	require.True(t, f(mm))
	mm.EXPECT().Shard().Return(uint32(512))
	require.False(t, f(mm))
}
