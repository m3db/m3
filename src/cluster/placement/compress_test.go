// Copyright (c) 2021 Uber Technologies, Inc.
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

package placement

import (
	"testing"

	"github.com/gogo/protobuf/proto"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
)

func TestMarshalAndUnmarshalPlacementProto(t *testing.T) {
	b, err := testLatestPlacementProto.Marshal()
	require.NoError(t, err)

	actual := &placementpb.Placement{}
	err = proto.Unmarshal(b, actual)
	require.NoError(t, err)
	require.Equal(t, testLatestPlacementProto.String(), actual.String())
}

func TestCompressAndDecompressPlacementProto(t *testing.T) {
	compressed, err := compressPlacementProto(testLatestPlacementProto)
	require.NoError(t, err)

	actual, err := decompressPlacementProto(compressed)
	require.NoError(t, err)
	require.Equal(t, testLatestPlacementProto.String(), actual.String())
}

func BenchmarkCompressPlacementProto(b *testing.B) {
	for n := 0; n < b.N; n++ {
		_, err := compressPlacementProto(testLatestPlacementProto)
		if err != nil {
			b.FailNow()
		}
	}
}

func BenchmarkDecompressPlacementProto(b *testing.B) {
	compressed, err := compressPlacementProto(testLatestPlacementProto)
	if err != nil {
		b.FailNow()
	}

	for n := 0; n < b.N; n++ {
		_, err := decompressPlacementProto(compressed)
		if err != nil {
			b.FailNow()
		}
	}
}
