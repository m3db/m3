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

package rules

import (
	"testing"
	"time"

	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestMatchResultProperties(t *testing.T) {
	r := NewMatchResult(1, 1000, nil, nil)
	require.Equal(t, 1, r.Version())
	require.Equal(t, int64(1000), r.ExpireAtNanos())
	require.False(t, r.HasExpired(0))
	require.True(t, r.HasExpired(1000))
}

func TestMatchResult(t *testing.T) {
	var (
		testExpireAtNanos = int64(67890)
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

	res := NewMatchResult(0, testExpireAtNanos, testForExistingID, testForNewRollupIDs)
	for _, input := range inputs {
		require.Equal(t, input.expectedForExistingID, res.ForExistingIDAt(input.matchAtNanos))
		require.Equal(t, len(input.expectedForNewRollupIDs), res.NumNewRollupIDs())
		for i := 0; i < len(input.expectedForNewRollupIDs); i++ {
			forNewRollupID := res.ForNewRollupIDsAt(i, input.matchAtNanos)
			require.Equal(t, input.expectedForNewRollupIDs[i], forNewRollupID)
		}
	}
}
