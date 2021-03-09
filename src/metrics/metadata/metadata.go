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
	"bytes"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/generated/proto/policypb"
	"github.com/m3db/m3/src/metrics/metric"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
)

var (
	// DefaultPipelineMetadata is a default pipeline metadata.
	DefaultPipelineMetadata PipelineMetadata

	// DefaultPipelineMetadatas is a default list of pipeline metadatas.
	DefaultPipelineMetadatas = PipelineMetadatas{DefaultPipelineMetadata}

	// DefaultMetadata is a default metadata.
	DefaultMetadata = Metadata{Pipelines: DefaultPipelineMetadatas}

	// DefaultStagedMetadata is a default staged metadata.
	DefaultStagedMetadata = StagedMetadata{Metadata: DefaultMetadata}

	// DefaultStagedMetadatas represents default staged metadatas.
	DefaultStagedMetadatas = StagedMetadatas{DefaultStagedMetadata}

	// DropPipelineMetadata is the drop policy pipeline metadata.
	DropPipelineMetadata = PipelineMetadata{DropPolicy: policy.DropMust}

	// DropPipelineMetadatas is the drop policy list of pipeline metadatas.
	DropPipelineMetadatas = []PipelineMetadata{DropPipelineMetadata}

	// DropIfOnlyMatchPipelineMetadata is the drop if only match policy
	// pipeline metadata.
	DropIfOnlyMatchPipelineMetadata = PipelineMetadata{DropPolicy: policy.DropIfOnlyMatch}

	// DropIfOnlyMatchPipelineMetadatas is the drop if only match policy list
	// of pipeline metadatas.
	DropIfOnlyMatchPipelineMetadatas = []PipelineMetadata{DropIfOnlyMatchPipelineMetadata}

	// DropMetadata is the drop policy metadata.
	DropMetadata = Metadata{Pipelines: DropPipelineMetadatas}

	// DropStagedMetadata is the drop policy staged metadata.
	DropStagedMetadata = StagedMetadata{Metadata: DropMetadata}

	// DropStagedMetadatas is the drop policy staged metadatas.
	DropStagedMetadatas = StagedMetadatas{DropStagedMetadata}
)

// PipelineMetadata contains pipeline metadata.
type PipelineMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID `json:"aggregation,omitempty"`

	// List of storage policies.
	StoragePolicies policy.StoragePolicies `json:"storagePolicies,omitempty"`

	// Pipeline operations.
	Pipeline applied.Pipeline `json:"-"` // NB: not needed for JSON marshaling for now.

	// Drop policy.
	DropPolicy policy.DropPolicy `json:"dropPolicy,omitempty"`

	// Tags.
	Tags []models.Tag `json:"tags,omitempty"`

	// GraphitePrefix is the list of graphite prefixes to apply.
	GraphitePrefix [][]byte `json:"graphitePrefix,omitempty"`
}

// Equal returns true if two pipeline metadata are considered equal.
func (m PipelineMetadata) Equal(other PipelineMetadata) bool {
	return m.AggregationID.Equal(other.AggregationID) &&
		m.StoragePolicies.Equal(other.StoragePolicies) &&
		m.Pipeline.Equal(other.Pipeline) &&
		m.DropPolicy == other.DropPolicy
}

// IsDefault returns whether this is the default standard pipeline metadata.
func (m PipelineMetadata) IsDefault() bool {
	return m.AggregationID == aggregation.DefaultID &&
		len(m.StoragePolicies) == 0 &&
		m.Pipeline.IsEmpty() &&
		m.DropPolicy == policy.DefaultDropPolicy
}

// IsMappingRule returns whether this is a mapping rule.
// nolint:gocritic
func (m PipelineMetadata) IsMappingRule() bool {
	return m.Pipeline.IsMappingRule()
}

// IsDropPolicyApplied returns whether this is the default standard pipeline
// but with the drop policy applied.
func (m PipelineMetadata) IsDropPolicyApplied() bool {
	return m.AggregationID.IsDefault() &&
		m.StoragePolicies.IsDefault() &&
		m.Pipeline.IsEmpty() &&
		m.IsDropPolicySet()
}

// IsDropPolicySet returns whether a drop policy is set.
func (m PipelineMetadata) IsDropPolicySet() bool {
	return !m.DropPolicy.IsDefault()
}

// Clone clones the pipeline metadata.
func (m PipelineMetadata) Clone() PipelineMetadata {
	return PipelineMetadata{
		AggregationID:   m.AggregationID,
		StoragePolicies: m.StoragePolicies.Clone(),
		Pipeline:        m.Pipeline.Clone(),
	}
}

// ToProto converts the pipeline metadata to a protobuf message in place.
func (m PipelineMetadata) ToProto(pb *metricpb.PipelineMetadata) error {
	m.AggregationID.ToProto(&pb.AggregationId)
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
	pb.DropPolicy = policypb.DropPolicy(m.DropPolicy)
	return nil
}

// FromProto converts the protobuf message to a pipeline metadata in place.
func (m *PipelineMetadata) FromProto(pb metricpb.PipelineMetadata) error {
	m.AggregationID.FromProto(pb.AggregationId)
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
	m.DropPolicy = policy.DropPolicy(pb.DropPolicy)
	return nil
}

// PipelineMetadatas is a list of pipeline metadatas.
type PipelineMetadatas []PipelineMetadata

// Equal returns true if two pipline metadatas are considered equal.
func (metadatas PipelineMetadatas) Equal(other PipelineMetadatas) bool {
	if len(metadatas) != len(other) {
		return false
	}
	for i := 0; i < len(metadatas); i++ {
		if !metadatas[i].Equal(other[i]) {
			return false
		}
	}
	return true
}

// Clone clones the list of pipeline metadatas.
func (metadatas PipelineMetadatas) Clone() PipelineMetadatas {
	cloned := make(PipelineMetadatas, 0, len(metadatas))
	for i := 0; i < len(metadatas); i++ {
		cloned = append(cloned, metadatas[i].Clone())
	}
	return cloned
}

// IsDropPolicySet returns whether any drop policies are set (but
// does not discriminate if they have been applied or not).
func (metadatas PipelineMetadatas) IsDropPolicySet() bool {
	for i := range metadatas {
		if metadatas[i].IsDropPolicySet() {
			return true
		}
	}
	return false
}

// ApplyOrRemoveDropPoliciesResult is the result of applying or removing
// the drop policies for pipelines.
type ApplyOrRemoveDropPoliciesResult uint

const (
	// NoDropPolicyPresentResult is the result of no drop policies being present.
	NoDropPolicyPresentResult ApplyOrRemoveDropPoliciesResult = iota
	// AppliedEffectiveDropPolicyResult is the result of applying the drop
	// policy and returning just the single drop policy pipeline.
	AppliedEffectiveDropPolicyResult
	// RemovedIneffectiveDropPoliciesResult is the result of no drop policies
	// being effective and returning the pipelines without any drop policies.
	RemovedIneffectiveDropPoliciesResult
)

// ApplyOrRemoveDropPolicies applies or removes any drop policies, if
// effective then just the drop pipeline is returned otherwise if not
// effective it returns the drop policy pipelines that were not effective.
func (metadatas PipelineMetadatas) ApplyOrRemoveDropPolicies() (
	PipelineMetadatas,
	ApplyOrRemoveDropPoliciesResult,
) {
	// Check drop policies
	dropIfOnlyMatchPipelines := 0
	nonDropPipelines := 0
	for i := range metadatas {
		switch metadatas[i].DropPolicy {
		case policy.DropMust:
			// Immediately return, result is a drop
			return DropPipelineMetadatas, AppliedEffectiveDropPolicyResult
		case policy.DropIfOnlyMatch:
			dropIfOnlyMatchPipelines++
			continue
		}
		nonDropPipelines++
	}

	if dropIfOnlyMatchPipelines == 0 {
		// No drop if only match pipelines, no need to remove anything
		return metadatas, NoDropPolicyPresentResult
	}

	if nonDropPipelines == 0 {
		// Drop is effective as no other non drop pipelines, result is a drop
		return DropPipelineMetadatas, AppliedEffectiveDropPolicyResult
	}

	// Remove all non-default drop policies as they must not be effective
	result := metadatas
	for i := len(result) - 1; i >= 0; i-- {
		if !result[i].DropPolicy.IsDefault() {
			// Remove by moving to tail and decrementing length so we can do in
			// place to avoid allocations of a new slice
			if lastElem := i == len(result)-1; lastElem {
				result = result[0:i]
			} else {
				result = append(result[0:i], result[i+1:]...)
			}
		}
	}

	return result, RemovedIneffectiveDropPoliciesResult
}

// ApplyCustomTags applies custom M3 tags.
func (metadatas PipelineMetadatas) ApplyCustomTags() (
	dropTimestamp bool,
) {
	// Go over metadatas and process M3 custom tags.
	for i := range metadatas {
		for j := range metadatas[i].Tags {
			// If any metadata has the drop timestamp tag, then return that we
			// should send untimed metrics to the aggregator.
			if bytes.Equal(metadatas[i].Tags[j].Name, metric.M3MetricsDropTimestamp) {
				dropTimestamp = true
			}
		}
	}
	return
}

// Metadata represents the metadata associated with a metric.
type Metadata struct {
	Pipelines PipelineMetadatas `json:"pipelines,omitempty"`
}

// IsDefault returns whether this is the default metadata.
func (m Metadata) IsDefault() bool {
	return len(m.Pipelines) == 1 && m.Pipelines[0].IsDefault()
}

// IsDropPolicyApplied returns whether this is the default metadata
// but with the drop policy applied.
func (m Metadata) IsDropPolicyApplied() bool {
	return len(m.Pipelines) == 1 && m.Pipelines[0].IsDropPolicyApplied()
}

// IsDropPolicySet returns whether any drop policies are set (but
// does not discriminate if they have been applied or not).
func (m Metadata) IsDropPolicySet() bool {
	for i := range m.Pipelines {
		if m.Pipelines[i].IsDropPolicySet() {
			return true
		}
	}
	return false
}

// Equal returns true if two metadatas are considered equal.
func (m Metadata) Equal(other Metadata) bool {
	return m.Pipelines.Equal(other.Pipelines)
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
		m.Pipelines = make(PipelineMetadatas, numPipelines)
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

	// Metric source id that refers to the unique id of the source producing this metric.
	SourceID uint32

	// Number of times this metric has been forwarded.
	NumForwardedTimes int
}

// ToProto converts the forward metadata to a protobuf message in place.
func (m ForwardMetadata) ToProto(pb *metricpb.ForwardMetadata) error {
	m.AggregationID.ToProto(&pb.AggregationId)
	if err := m.StoragePolicy.ToProto(&pb.StoragePolicy); err != nil {
		return err
	}
	if err := m.Pipeline.ToProto(&pb.Pipeline); err != nil {
		return err
	}
	pb.SourceId = m.SourceID
	pb.NumForwardedTimes = int32(m.NumForwardedTimes)
	return nil
}

// FromProto converts the protobuf message to a forward metadata in place.
func (m *ForwardMetadata) FromProto(pb metricpb.ForwardMetadata) error {
	m.AggregationID.FromProto(pb.AggregationId)
	if err := m.StoragePolicy.FromProto(pb.StoragePolicy); err != nil {
		return err
	}
	if err := m.Pipeline.FromProto(pb.Pipeline); err != nil {
		return err
	}
	m.SourceID = pb.SourceId
	m.NumForwardedTimes = int(pb.NumForwardedTimes)
	return nil
}

// StagedMetadata represents metadata with a staged cutover time.
type StagedMetadata struct {
	Metadata `json:"metadata,omitempty"`

	// Cutover is when the metadata is applicable.
	CutoverNanos int64 `json:"cutoverNanos,omitempty"`

	// Tombstoned determines whether the associated metric has been tombstoned.
	Tombstoned bool `json:"tombstoned,omitempty"`
}

// Equal returns true if two staged metadatas are considered equal.
func (sm StagedMetadata) Equal(other StagedMetadata) bool {
	return sm.Metadata.Equal(other.Metadata) &&
		sm.CutoverNanos == other.CutoverNanos &&
		sm.Tombstoned == other.Tombstoned
}

// IsDefault returns whether this is a default staged metadata.
func (sm StagedMetadata) IsDefault() bool {
	return sm.CutoverNanos == 0 && !sm.Tombstoned && sm.Metadata.IsDefault()
}

// IsDropPolicyApplied returns whether this is the default staged metadata
// but with the drop policy applied.
func (sm StagedMetadata) IsDropPolicyApplied() bool {
	return !sm.Tombstoned && sm.Metadata.IsDropPolicyApplied()
}

// IsDropPolicySet returns whether a drop policy is set.
func (sm StagedMetadata) IsDropPolicySet() bool {
	return sm.Pipelines.IsDropPolicySet()
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

// Equal returns true if two staged metadatas slices are considered equal.
func (sms StagedMetadatas) Equal(other StagedMetadatas) bool {
	if len(sms) != len(other) {
		return false
	}
	for i := range sms {
		if !sms[i].Equal(other[i]) {
			return false
		}
	}
	return true
}

// IsDefault determines whether the list of staged metadata is a default list.
func (sms StagedMetadatas) IsDefault() bool {
	// very ugly but need to help out the go compiler here, as function calls have a high inlining cost
	return len(sms) == 1 && len(sms[0].Pipelines) == 1 &&
		sms[0].CutoverNanos == 0 && !sms[0].Tombstoned &&
		sms[0].Pipelines[0].AggregationID == aggregation.DefaultID &&
		len(sms[0].Pipelines[0].StoragePolicies) == 0 &&
		sms[0].Pipelines[0].Pipeline.IsEmpty() &&
		sms[0].Pipelines[0].DropPolicy == policy.DefaultDropPolicy
}

// IsDropPolicyApplied returns whether the list of staged metadata is the
// default list but with the drop policy applied.
func (sms StagedMetadatas) IsDropPolicyApplied() bool {
	return len(sms) == 1 && sms[0].IsDropPolicyApplied()
}

// IsDropPolicySet returns if the active staged metadata has a drop policy set.
func (sms StagedMetadatas) IsDropPolicySet() bool {
	return len(sms) > 0 && sms[len(sms)-1].IsDropPolicySet()
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
// This is an optimized method that merges some nested steps.
func (sms *StagedMetadatas) FromProto(pb metricpb.StagedMetadatas) error {
	numMetadatas := len(pb.Metadatas)
	if cap(*sms) >= numMetadatas {
		*sms = (*sms)[:numMetadatas]
	} else {
		*sms = make([]StagedMetadata, numMetadatas)
	}

	for i := 0; i < numMetadatas; i++ {
		metadata := &(*sms)[i]
		metadataPb := &pb.Metadatas[i]
		numPipelines := len(metadataPb.Metadata.Pipelines)

		metadata.CutoverNanos = metadataPb.CutoverNanos
		metadata.Tombstoned = metadataPb.Tombstoned

		if cap(metadata.Pipelines) >= numPipelines {
			metadata.Pipelines = metadata.Pipelines[:numPipelines]
		} else {
			metadata.Pipelines = make(PipelineMetadatas, numPipelines)
		}

		for j := 0; j < numPipelines; j++ {
			var (
				pipelinePb         = &metadataPb.Metadata.Pipelines[j]
				pipeline           = &metadata.Pipelines[j]
				numStoragePolicies = len(pipelinePb.StoragePolicies)
				numOps             = len(pipelinePb.Pipeline.Ops)
				err                error
			)

			pipeline.AggregationID[0] = pipelinePb.AggregationId.Id
			pipeline.DropPolicy = policy.DropPolicy(pipelinePb.DropPolicy)

			if len(pipeline.Tags) > 0 {
				pipeline.Tags = pipeline.Tags[:0]
			}
			if len(pipeline.GraphitePrefix) > 0 {
				pipeline.GraphitePrefix = pipeline.GraphitePrefix[:0]
			}

			if cap(pipeline.StoragePolicies) >= numStoragePolicies {
				pipeline.StoragePolicies = pipeline.StoragePolicies[:numStoragePolicies]
			} else {
				pipeline.StoragePolicies = make([]policy.StoragePolicy, numStoragePolicies)
			}

			if cap(pipeline.Pipeline.Operations) >= numOps {
				pipeline.Pipeline.Operations = pipeline.Pipeline.Operations[:numOps]
			} else {
				pipeline.Pipeline.Operations = make([]applied.OpUnion, numOps)
			}

			if len(pipelinePb.Pipeline.Ops) > 0 {
				err = applied.OperationsFromProto(pipelinePb.Pipeline.Ops, pipeline.Pipeline.Operations)
				if err != nil {
					return err
				}
			}
			if len(pipelinePb.StoragePolicies) > 0 {
				err = policy.StoragePoliciesFromProto(pipelinePb.StoragePolicies, pipeline.StoragePolicies)
				if err != nil {
					return err
				}
			}
		}
	}

	return nil
}

// fromProto is a non-optimized in place protobuf conversion method, used as a reference for tests.
func (sms *StagedMetadatas) fromProto(pb metricpb.StagedMetadatas) error {
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

// VersionedStagedMetadatas is a versioned staged metadatas.
type VersionedStagedMetadatas struct {
	Version         int             `json:"version"`
	StagedMetadatas StagedMetadatas `json:"stagedMetadatas"`
}

// TimedMetadata represents the metadata information associated with timed metrics.
type TimedMetadata struct {
	// List of aggregation types.
	AggregationID aggregation.ID

	// Storage policy.
	StoragePolicy policy.StoragePolicy
}

// ToProto converts the timed metadata to a protobuf message in place.
func (m TimedMetadata) ToProto(pb *metricpb.TimedMetadata) error {
	m.AggregationID.ToProto(&pb.AggregationId)
	if err := m.StoragePolicy.ToProto(&pb.StoragePolicy); err != nil {
		return err
	}
	return nil
}

// FromProto converts the protobuf message to a timed metadata in place.
func (m *TimedMetadata) FromProto(pb metricpb.TimedMetadata) error {
	m.AggregationID.FromProto(pb.AggregationId)
	if err := m.StoragePolicy.FromProto(pb.StoragePolicy); err != nil {
		return err
	}
	return nil
}
