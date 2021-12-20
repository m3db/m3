// Copyright (c) 2020 Uber Technologies, Inc.
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
	"strconv"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/policy"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testIDWithMetadatas = IDWithMetadatas{
		ID: b("rName2|rtagName1=rtagValue1"),
		Metadatas: metadata.StagedMetadatas{
			{
				CutoverNanos: 12345,
				Tombstoned:   false,
				Metadata: metadata.Metadata{
					Pipelines: []metadata.PipelineMetadata{
						metadata.DefaultPipelineMetadata,
					},
				},
			},
		},
	}
	testExpireAtNanos = int64(67890)
	//nolint:dupl
	testForExistingID = metadata.StagedMetadatas{
		{
			CutoverNanos: 12345,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
							policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
			},
		},
		{
			CutoverNanos: 23456,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: policy.StoragePolicies{
							policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour),
							policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour),
						},
					},
				},
			},
		},
	}
	testForNewRollupIDs = []IDWithMetadatas{
		{
			ID: b("rName1|rtagName1=rtagValue1,rtagName2=rtagValue2"),
			//nolint:dupl
			Metadatas: metadata.StagedMetadatas{
				{
					CutoverNanos: 12345,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(10*time.Second, xtime.Second, 12*time.Hour),
									policy.NewStoragePolicy(time.Minute, xtime.Minute, 24*time.Hour),
									policy.NewStoragePolicy(5*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
				{
					CutoverNanos: 23456,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							{
								AggregationID: aggregation.DefaultID,
								StoragePolicies: policy.StoragePolicies{
									policy.NewStoragePolicy(30*time.Second, xtime.Second, 10*time.Hour),
									policy.NewStoragePolicy(2*time.Minute, xtime.Minute, 48*time.Hour),
								},
							},
						},
					},
				},
			},
		},
		{
			ID: b("rName2|rtagName1=rtagValue1"),
			Metadatas: metadata.StagedMetadatas{
				{
					CutoverNanos: 12345,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
						},
					},
				},
			},
		},
		{
			ID: b("rName3|rtagName1=rtagValue2"),
			Metadatas: metadata.StagedMetadatas{
				{
					CutoverNanos: 12345,
					Tombstoned:   false,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
						},
					},
				},
				{
					CutoverNanos: 27000,
					Tombstoned:   true,
					Metadata: metadata.Metadata{
						Pipelines: []metadata.PipelineMetadata{
							metadata.DefaultPipelineMetadata,
						},
					},
				},
			},
		},
	}
)

func TestMatchResultProperties(t *testing.T) {
	cases := []struct {
		version      int
		expires      int64
		keepOriginal bool
	}{
		{
			version:      1,
			expires:      1000,
			keepOriginal: false,
		},
		{
			version:      2,
			expires:      2000,
			keepOriginal: true,
		},
	}

	for i, tt := range cases {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			r := NewMatchResult(
				tt.version,
				tt.expires,
				nil,
				nil,
				tt.keepOriginal,
			)

			require.Equal(t, tt.version, r.Version())
			require.Equal(t, tt.expires, r.ExpireAtNanos())
			require.False(t, r.HasExpired(tt.expires-1))
			require.True(t, r.HasExpired(tt.expires))
			require.Equal(t, tt.keepOriginal, r.KeepOriginal())
		})
	}
}

func TestMatchResult(t *testing.T) {
	inputs := []struct {
		matchAtNanos            int64
		expectedForExistingID   metadata.StagedMetadatas
		expectedForNewRollupIDs []IDWithMetadatas
	}{
		{
			matchAtNanos:            0,
			expectedForExistingID:   testForExistingID,
			expectedForNewRollupIDs: testForNewRollupIDs,
		},
		{
			matchAtNanos:            23455,
			expectedForExistingID:   testForExistingID,
			expectedForNewRollupIDs: testForNewRollupIDs,
		},
		{
			matchAtNanos:          23456,
			expectedForExistingID: testForExistingID[1:],
			expectedForNewRollupIDs: []IDWithMetadatas{
				{
					ID:        testForNewRollupIDs[0].ID,
					Metadatas: testForNewRollupIDs[0].Metadatas[1:],
				},
				testForNewRollupIDs[1],
				testForNewRollupIDs[2],
			},
		},
		{
			matchAtNanos:          30000,
			expectedForExistingID: testForExistingID[1:],
			expectedForNewRollupIDs: []IDWithMetadatas{
				{
					ID:        testForNewRollupIDs[0].ID,
					Metadatas: testForNewRollupIDs[0].Metadatas[1:],
				},
				testForNewRollupIDs[1],
				{
					ID:        testForNewRollupIDs[2].ID,
					Metadatas: testForNewRollupIDs[2].Metadatas[1:],
				},
			},
		},
	}

	res := NewMatchResult(0, testExpireAtNanos, testForExistingID, testForNewRollupIDs, false)
	for _, input := range inputs {
		require.Equal(t, input.expectedForExistingID, res.ForExistingIDAt(input.matchAtNanos))
		require.Equal(t, len(input.expectedForNewRollupIDs), res.NumNewRollupIDs())
		for i := 0; i < len(input.expectedForNewRollupIDs); i++ {
			forNewRollupID := res.ForNewRollupIDsAt(i, input.matchAtNanos)
			require.Equal(t, input.expectedForNewRollupIDs[i], forNewRollupID)
		}
	}
}

func TestMatchResultClone(t *testing.T) {
	res := NewMatchResult(10, testExpireAtNanos, testForExistingID, testForNewRollupIDs, true)
	clone := res.Clone()
	require.True(t, cmp.Equal(res, clone, cmpopts.EquateEmpty(), cmp.AllowUnexported(MatchResult{})))
}

func TestMatchResultReset(t *testing.T) {
	res := NewMatchResult(10, testExpireAtNanos, testForExistingID, testForNewRollupIDs, true)
	res.Reset()
	require.Equal(t, MatchResult{
		forExistingID:   metadata.StagedMetadatas{},
		forNewRollupIDs: []IDWithMetadatas{},
	}, res)
}

func TestIDWithMetadatasClone(t *testing.T) {
	clone := testIDWithMetadatas.Clone()
	require.True(t, cmp.Equal(testIDWithMetadatas, clone, cmpopts.EquateEmpty()))
}

func TestIDWithMetadatasReset(t *testing.T) {
	clone := testIDWithMetadatas.Clone()
	clone.Reset()
	require.Equal(t, IDWithMetadatas{
		ID:        []byte{},
		Metadatas: metadata.StagedMetadatas{},
	}, clone)
}
