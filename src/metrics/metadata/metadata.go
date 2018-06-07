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

package metadata

import (
	"github.com/m3db/m3metrics/aggregation"
	"github.com/m3db/m3metrics/generated/proto/metricpb"
	"github.com/m3db/m3metrics/generated/proto/policypb"
	"github.com/m3db/m3metrics/op/applied"
	"github.com/m3db/m3metrics/policy"
)

var (
	// DefaultStagedMetadata is a default staged metadata.
	DefaultStagedMetadata = StagedMetadata{
		Metadata: Metadata{
			Pipelines: []PipelineMetadata{PipelineMetadata{}},
		},
	}

	// DefaultStagedMetadatas represents default staged metadatas.
	DefaultStagedMetadatas = StagedMetadatas{DefaultStagedMetadata}
)

// PipelineMetadata contains pipeline metadata.
type PipelineMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// List of storage policies.
	StoragePolicies []policy.StoragePolicy

	// Pipeline operations.
	Pipeline applied.Pipeline
}

// IsDefault returns whether this is the default standard pipeline metadata.
func (m PipelineMetadata) IsDefault() bool {
	return m.AggregationID.IsDefault() &&
		policy.IsDefaultStoragePolicies(m.StoragePolicies) &&
		m.Pipeline.IsEmpty()
}

// ToProto converts the pipeline metadata to a protobuf message in place.
func (m PipelineMetadata) ToProto(pb *metricpb.PipelineMetadata) error {
	if err := m.AggregationID.ToProto(&pb.AggregationId); err != nil {
		return err
	}
	if err := m.Pipeline.ToProto(&pb.Pipeline); err != nil {
		return err
	}
	numStoragePolicies := len(m.StoragePolicies)
	if cap(pb.StoragePolicies) >= numStoragePolicies {
		pb.StoragePolicies = pb.StoragePolicies[:numStoragePolicies]
	} else {
		pb.StoragePolicies = make([]policypb.StoragePolicy, numStoragePolicies)
	}
	for i := 0; i < numStoragePolicies; i++ {
		if err := m.StoragePolicies[i].ToProto(&pb.StoragePolicies[i]); err != nil {
			return err
		}
	}
	return nil
}

// FromProto converts the protobuf message to a pipeline metadata in place.
func (m *PipelineMetadata) FromProto(pb metricpb.PipelineMetadata) error {
	if err := m.AggregationID.FromProto(pb.AggregationId); err != nil {
		return err
	}
	if err := m.Pipeline.FromProto(pb.Pipeline); err != nil {
		return err
	}
	numStoragePolicies := len(pb.StoragePolicies)
	if cap(m.StoragePolicies) >= numStoragePolicies {
		m.StoragePolicies = m.StoragePolicies[:numStoragePolicies]
	} else {
		m.StoragePolicies = make([]policy.StoragePolicy, numStoragePolicies)
	}
	for i := 0; i < numStoragePolicies; i++ {
		if err := m.StoragePolicies[i].FromProto(pb.StoragePolicies[i]); err != nil {
			return err
		}
	}
	return nil
}

// Metadata represents the metadata associated with a metric.
type Metadata struct {
	Pipelines []PipelineMetadata
}

// IsDefault returns whether this is the default metadata.
func (m Metadata) IsDefault() bool {
	return len(m.Pipelines) == 1 && m.Pipelines[0].IsDefault()
}

// ToProto converts the metadata to a protobuf message in place.
func (m Metadata) ToProto(pb *metricpb.Metadata) error {
	numPipelines := len(m.Pipelines)
	if cap(pb.Pipelines) >= numPipelines {
		pb.Pipelines = pb.Pipelines[:numPipelines]
	} else {
		pb.Pipelines = make([]metricpb.PipelineMetadata, numPipelines)
	}
	for i := 0; i < numPipelines; i++ {
		if err := m.Pipelines[i].ToProto(&pb.Pipelines[i]); err != nil {
			return err
		}
	}
	return nil
}

// FromProto converts the protobuf message to a metadata in place.
func (m *Metadata) FromProto(pb metricpb.Metadata) error {
	numPipelines := len(pb.Pipelines)
	if cap(m.Pipelines) >= numPipelines {
		m.Pipelines = m.Pipelines[:numPipelines]
	} else {
		m.Pipelines = make([]PipelineMetadata, numPipelines)
	}
	for i := 0; i < numPipelines; i++ {
		if err := m.Pipelines[i].FromProto(pb.Pipelines[i]); err != nil {
			return err
		}
	}
	return nil
}

// ForwardMetadata represents the metadata information associated with forwarded metrics.
type ForwardMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// Storage policy.
	StoragePolicy policy.StoragePolicy

	// Pipeline of operations that may be applied to the metric.
	Pipeline applied.Pipeline
}

// StagedMetadata represents metadata with a staged cutover time.
type StagedMetadata struct {
	Metadata

	// Cutover is when the metadata is applicable.
	CutoverNanos int64

	// Tombstoned determines whether the associated metric has been tombstoned.
	Tombstoned bool
}

// IsDefault returns whether this is a default staged metadata.
func (sm StagedMetadata) IsDefault() bool {
	return sm.CutoverNanos == 0 && !sm.Tombstoned && sm.Metadata.IsDefault()
}

// ToProto converts the staged metadata to a protobuf message in place.
func (sm StagedMetadata) ToProto(pb *metricpb.StagedMetadata) error {
	if err := sm.Metadata.ToProto(&pb.Metadata); err != nil {
		return err
	}
	pb.CutoverNanos = sm.CutoverNanos
	pb.Tombstoned = sm.Tombstoned
	return nil
}

// FromProto converts the protobuf message to a staged metadata in place.
func (sm *StagedMetadata) FromProto(pb metricpb.StagedMetadata) error {
	if err := sm.Metadata.FromProto(pb.Metadata); err != nil {
		return err
	}
	sm.CutoverNanos = pb.CutoverNanos
	sm.Tombstoned = pb.Tombstoned
	return nil
}

// StagedMetadatas contains a list of staged metadatas.
type StagedMetadatas []StagedMetadata

// IsDefault determines whether the list of staged metadata is a default list.
func (sms StagedMetadatas) IsDefault() bool {
	return len(sms) == 1 && sms[0].IsDefault()
}

// ToProto converts the staged metadatas to a protobuf message in place.
func (sms StagedMetadatas) ToProto(pb *metricpb.StagedMetadatas) error {
	numMetadatas := len(sms)
	if cap(pb.Metadatas) >= numMetadatas {
		pb.Metadatas = pb.Metadatas[:numMetadatas]
	} else {
		pb.Metadatas = make([]metricpb.StagedMetadata, numMetadatas)
	}
	for i := 0; i < numMetadatas; i++ {
		if err := sms[i].ToProto(&pb.Metadatas[i]); err != nil {
			return err
		}
	}
	return nil
}

// FromProto converts the protobuf message to a staged metadatas in place.
func (sms *StagedMetadatas) FromProto(pb metricpb.StagedMetadatas) error {
	numMetadatas := len(pb.Metadatas)
	if cap(*sms) >= numMetadatas {
		*sms = (*sms)[:numMetadatas]
	} else {
		*sms = make([]StagedMetadata, numMetadatas)
	}
	for i := 0; i < numMetadatas; i++ {
		if err := (*sms)[i].FromProto(pb.Metadatas[i]); err != nil {
			return err
		}
	}
	return nil
}
