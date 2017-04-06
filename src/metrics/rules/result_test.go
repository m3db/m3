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

package rules

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMatchResult(t *testing.T) {
	var (
		version    = 1
		cutoverNs  = int64(12345)
		expireAtNs = int64(67890)
		mappings   = []policy.Policy{
			policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
			policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
			policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
		}
		rollups = []rollupResult{
			{
				ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
				Policies: []policy.Policy{
					policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			},
			{
				ID:       b("rName2|rtagName1=rtagValue1"),
				Policies: []policy.Policy{},
			},
		}
	)

	res := newMatchResult(version, cutoverNs, expireAtNs, mappings, rollups)
	require.False(t, res.HasExpired(time.Unix(0, 0)))
	require.True(t, res.HasExpired(time.Unix(0, 100000)))

	expectedMappings := policy.CustomVersionedPolicies(version, time.Unix(0, 12345), mappings)
	require.Equal(t, expectedMappings, res.Mappings())

	var (
		expectedRollupIDs = [][]byte{
			b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
			b("rName2|rtagName1=rtagValue1"),
		}
		expectedRollupPolicies = []policy.VersionedPolicies{
			policy.CustomVersionedPolicies(
				1,
				time.Unix(0, 12345),
				[]policy.Policy{
					policy.NewPolicy(10*time.Second, xtime.Second, 12*time.Hour),
					policy.NewPolicy(time.Minute, xtime.Minute, 24*time.Hour),
					policy.NewPolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
				},
			),
			policy.DefaultVersionedPolicies(1, time.Unix(0, 12345)),
		}
		rollupIDs      [][]byte
		rollupPolicies []policy.VersionedPolicies
	)
	require.Equal(t, 2, res.NumRollups())
	for i := 0; i < 2; i++ {
		id, policies := res.Rollups(i)
		rollupIDs = append(rollupIDs, id)
		rollupPolicies = append(rollupPolicies, policies)
	}
	require.Equal(t, expectedRollupIDs, rollupIDs)
	require.Equal(t, expectedRollupPolicies, rollupPolicies)
}
