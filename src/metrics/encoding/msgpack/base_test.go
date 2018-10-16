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

package msgpack

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestAggregationTypesRoundTrip(t *testing.T) {
	inputs := []aggregation.ID{
		aggregation.DefaultID,
		aggregation.ID{5},
		aggregation.ID{100},
		aggregation.ID{12345},
	}

	for _, input := range inputs {
		enc := newBaseEncoder(NewBufferedEncoder()).(*baseEncoder)
		it := newBaseIterator(enc.bufEncoder.Buffer(), 16).(*baseIterator)

		enc.encodeCompressedAggregationTypes(input)
		r := it.decodeCompressedAggregationTypes()
		require.Equal(t, input, r)
	}
}

func TestUnaggregatedPolicyRoundTrip(t *testing.T) {
	inputs := []policy.Policy{
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.DefaultID),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), aggregation.ID{8}),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), aggregation.ID{100}),
	}

	for _, input := range inputs {
		enc := newBaseEncoder(NewBufferedEncoder()).(*baseEncoder)
		enc.encodePolicy(input)

		it := newBaseIterator(enc.bufEncoder.Buffer(), 16).(*baseIterator)
		r := it.decodePolicy()
		require.Equal(t, input, r)
	}
}
