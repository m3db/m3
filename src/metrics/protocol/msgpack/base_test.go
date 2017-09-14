package msgpack

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"
	"github.com/stretchr/testify/require"
)

func TestAggregationTypesRoundTrip(t *testing.T) {
	inputs := []policy.AggregationID{
		policy.DefaultAggregationID,
		policy.AggregationID{5},
		policy.AggregationID{100},
		policy.AggregationID{12345},
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
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.DefaultAggregationID),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 2*24*time.Hour), policy.AggregationID{8}),
		policy.NewPolicy(policy.NewStoragePolicy(10*time.Second, xtime.Second, 24*time.Hour), policy.AggregationID{100}),
	}

	for _, input := range inputs {
		enc := newBaseEncoder(NewBufferedEncoder()).(*baseEncoder)
		enc.encodePolicy(input)

		it := newBaseIterator(enc.bufEncoder.Buffer(), 16).(*baseIterator)
		r := it.decodePolicy()
		require.Equal(t, input, r)
	}
}
