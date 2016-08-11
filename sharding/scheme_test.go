// Copyright (c) 2016 Uber Technologies, Inc.
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

package sharding

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestShardScheme(t *testing.T) {
	ss, err := NewShardSchemeFromRange(1, 0, func(id string) uint32 { return 0 })
	require.Equal(t, ErrToLessThanFrom, err)
	require.Nil(t, ss)

	ss, err = NewShardScheme([]uint32{}, func(id string) uint32 { return 0 })
	require.Equal(t, ErrNoShards, err)
	require.Nil(t, ss)

	ss, err = NewShardScheme([]uint32{1, 1}, func(id string) uint32 { return 0 })
	require.Equal(t, ErrDuplicateShards, err)
	require.Nil(t, ss)

	staticShard := uint32(1)
	ss, err = NewShardSchemeFromRange(0, 2, func(id string) uint32 { return staticShard })
	require.NoError(t, err)
	require.NotNil(t, ss)

	s := ss.Shard("bla")
	require.Equal(t, staticShard, s)
}

func TestShardSet(t *testing.T) {
	ss, err := NewShardSchemeFromRange(0, 2, func(id string) uint32 { return 0 })
	require.NoError(t, err)
	require.NotNil(t, ss)

	sSet, err := ss.CreateSet([]uint32{})
	require.Equal(t, ErrNoShards, err)

	sSet, err = ss.CreateSet([]uint32{1, 1})
	require.Equal(t, ErrDuplicateShards, err)

	sSet, err = ss.CreateSet([]uint32{1, 2})
	require.NoError(t, err)
	require.Equal(t, []uint32{1, 2}, sSet.Shards())
	require.Equal(t, ss, sSet.Scheme())

	sSet = ss.All()
	require.Equal(t, []uint32{0, 1, 2}, sSet.Shards())
	require.Equal(t, ss, sSet.Scheme())
}
