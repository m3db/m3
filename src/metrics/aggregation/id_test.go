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

package aggregation

import (
	"testing"

	"github.com/m3db/m3metrics/generated/proto/aggregationpb"

	"github.com/stretchr/testify/require"
)

var (
	testID      = ID{6}
	testIDProto = aggregationpb.AggregationID{Id: 6}
)

func TestIDToProto(t *testing.T) {
	var pb aggregationpb.AggregationID
	require.NoError(t, testID.ToProto(&pb))
	require.Equal(t, testIDProto, pb)
}

func TestIDFromProto(t *testing.T) {
	var res ID
	require.NoError(t, res.FromProto(testIDProto))
	require.Equal(t, testID, res)
}

func TestIDRoundTrip(t *testing.T) {
	var (
		pb  aggregationpb.AggregationID
		res ID
	)
	require.NoError(t, testID.ToProto(&pb))
	require.NoError(t, res.FromProto(pb))
	require.Equal(t, testID, res)
}
