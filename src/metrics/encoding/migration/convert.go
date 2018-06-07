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

package migration

import (
	"fmt"

	"github.com/m3db/m3metrics/encoding"
	"github.com/m3db/m3metrics/metadata"
	"github.com/m3db/m3metrics/metric/unaggregated"
	"github.com/m3db/m3metrics/policy"
)

func toUnaggregatedMessageUnion(
	metric unaggregated.MetricUnion,
	policiesList policy.PoliciesList,
) (encoding.UnaggregatedMessageUnion, error) {
	metadatas := toStagedMetadatas(policiesList)
	switch metric.Type {
	case unaggregated.CounterType:
		return encoding.UnaggregatedMessageUnion{
			Type: encoding.CounterWithMetadatasType,
			CounterWithMetadatas: unaggregated.CounterWithMetadatas{
				Counter:         metric.Counter(),
				StagedMetadatas: metadatas,
			},
		}, nil
	case unaggregated.BatchTimerType:
		return encoding.UnaggregatedMessageUnion{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      metric.BatchTimer(),
				StagedMetadatas: metadatas,
			},
		}, nil
	case unaggregated.GaugeType:
		return encoding.UnaggregatedMessageUnion{
			Type: encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
				Gauge:           metric.Gauge(),
				StagedMetadatas: metadatas,
			},
		}, nil
	default:
		return encoding.UnaggregatedMessageUnion{}, fmt.Errorf("unknown metric type: %v", metric.Type)
	}
}

// TODO: look into reuse metadatas during conversion.
func toStagedMetadatas(
	policiesList policy.PoliciesList,
) metadata.StagedMetadatas {
	numStagedPolicies := len(policiesList)
	res := make(metadata.StagedMetadatas, 0, numStagedPolicies)
	for _, sp := range policiesList {
		if sp.IsDefault() {
			sm := metadata.DefaultStagedMetadata
			res = append(res, sm)
			continue
		}
		sm := metadata.StagedMetadata{}
		sm.CutoverNanos = sp.CutoverNanos
		sm.Tombstoned = sp.Tombstoned
		policies, _ := sp.Policies()
		sm.Metadata = toMetadata(policies)
		res = append(res, sm)
	}
	return res
}

// TODO: look into reuse metadata during conversion.
func toMetadata(policies []policy.Policy) metadata.Metadata {
	res := metadata.Metadata{}
	for _, p := range policies {
		// Find if there is an existing pipeline in the set of metadatas
		// with the same aggregation ID.
		pipelineIdx := -1
		for i := 0; i < len(res.Pipelines); i++ {
			if p.AggregationID == res.Pipelines[i].AggregationID {
				pipelineIdx = i
				break
			}
		}
		// If there is no existing pipeline with the same aggregation ID,
		// create a new pipeline with the aggregation ID.
		if pipelineIdx == -1 {
			res.Pipelines = append(res.Pipelines, metadata.PipelineMetadata{
				AggregationID: p.AggregationID,
			})
			pipelineIdx = len(res.Pipelines) - 1
		}

		// Find if the storage policy already exists in the set of storage
		// policies in the corresponding pipeline.
		pipelines := res.Pipelines
		policyIdx := -1
		for i := 0; i < len(pipelines[pipelineIdx].StoragePolicies); i++ {
			if pipelines[pipelineIdx].StoragePolicies[i] == p.StoragePolicy {
				policyIdx = i
				break
			}
		}
		// If the policy already exists in the pipeline, nothing to do.
		if policyIdx != -1 {
			continue
		}
		// Otherwise we append the policy to the end. This maintains the original
		// ordering (if any) of the policies within each pipeline.
		pipelines[pipelineIdx].StoragePolicies = append(pipelines[pipelineIdx].StoragePolicies, p.StoragePolicy)
	}
	return res
}
