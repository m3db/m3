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

package metadata

import (
	"runtime"
	"testing"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/generated/proto/metricpb"
	"github.com/m3db/m3/src/metrics/policy"
)

func isDefault(m StagedMetadatas) bool {
	return m.IsDefault()
}

func BenchmarkMetadata_IsDefault(b *testing.B) {
	m := StagedMetadatas{
		StagedMetadata{
			CutoverNanos: 0,
			Tombstoned:   false,
			Metadata: Metadata{
				Pipelines: []PipelineMetadata{
					{
						AggregationID:   aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{},
					},
				},
			},
		},
	}
	for i := 0; i < b.N; i++ {
		if !isDefault(m) {
			b.Fail()
		}
	}
	runtime.KeepAlive(m)
}

func BenchmarkMetadata_FromProto(b *testing.B) {
	var (
		testAllPayload metricpb.StagedMetadatas
		m              StagedMetadatas
	)

	testAllPayload.Metadatas = append(testAllPayload.Metadatas,
		testSmallStagedMetadatasProto.Metadatas...)
	testAllPayload.Metadatas = append(testAllPayload.Metadatas,
		testLargeStagedMetadatasProto.Metadatas...)
	testAllPayload.Metadatas = append(testAllPayload.Metadatas,
		testSmallStagedMetadatasWithLargeStoragePoliciesProto.Metadatas...)

	b.Run("large metadatas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.FromProto(testLargeStagedMetadatasProto); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("small metadatas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.FromProto(testSmallStagedMetadatasProto); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("storage policies", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.FromProto(
				testSmallStagedMetadatasWithLargeStoragePoliciesProto,
			); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("all", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.FromProto(testAllPayload); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("reference, large metadatas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.fromProto(testLargeStagedMetadatasProto); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("reference, small metadatas", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.fromProto(testSmallStagedMetadatasProto); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("reference, storage policies", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.fromProto(testSmallStagedMetadatasWithLargeStoragePoliciesProto); err != nil {
				b.Fail()
			}
		}
	})

	b.Run("reference, all", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			if err := m.fromProto(testAllPayload); err != nil {
				b.Fail()
			}
		}
	})

	runtime.KeepAlive(m)
}
