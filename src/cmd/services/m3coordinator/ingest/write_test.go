// Copyright (c) 2019 Uber Technologies, Inc.
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

package ingest

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	testm3 "github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xsync "github.com/m3db/m3/src/x/sync"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
	// Created by init().
	testWorkerPool xsync.PooledWorkerPool

	testTags1 = models.NewTags(3, nil).AddTags(
		[]models.Tag{
			{
				Name:  []byte("test_1_key_1"),
				Value: []byte("test_1_value_1"),
			},
			{
				Name:  []byte("test_1_key_2"),
				Value: []byte("test_1_value_2"),
			},
			{
				Name:  []byte("test_1_key_3"),
				Value: []byte("test_1_value_3"),
			},
		},
	)
	testTags2 = models.NewTags(3, nil).AddTags(
		[]models.Tag{
			{
				Name:  []byte("test_2_key_1"),
				Value: []byte("test_2_value_1"),
			},
			{
				Name:  []byte("test_2_key_2"),
				Value: []byte("test_2_value_2"),
			},
			{
				Name:  []byte("test_2_key_3"),
				Value: []byte("test_2_value_3"),
			},
		},
	)

	testDatapoints1 = []ts.Datapoint{
		{
			Timestamp: time.Unix(0, 0),
			Value:     0,
		},
		{
			Timestamp: time.Unix(0, 1),
			Value:     1,
		},
		{
			Timestamp: time.Unix(0, 2),
			Value:     2,
		},
	}
	testDatapoints2 = []ts.Datapoint{
		{
			Timestamp: time.Unix(0, 3),
			Value:     3,
		},
		{
			Timestamp: time.Unix(0, 4),
			Value:     4,
		},
		{
			Timestamp: time.Unix(0, 5),
			Value:     5,
		},
	}

	testAnnotation1 = []byte("first")
	testAnnotation2 = []byte("second")

	testEntries = []testIterEntry{
		{tags: testTags1, datapoints: testDatapoints1, annotation: testAnnotation1},
		{tags: testTags2, datapoints: testDatapoints2, annotation: testAnnotation2},
	}

	defaultOverride = WriteOptions{}

	zeroDownsamplerAppenderOpts = downsample.SampleAppenderOptions{}
)

type testIter struct {
	idx       int
	entries   []testIterEntry
	metadatas []ts.Metadata
}

type testIterEntry struct {
	tags       models.Tags
	datapoints []ts.Datapoint
	annotation []byte
}

func newTestIter(entries []testIterEntry) *testIter {
	return &testIter{
		idx:       -1,
		entries:   entries,
		metadatas: make([]ts.Metadata, 0, len(entries)),
	}
}

func (i *testIter) Next() bool {
	i.idx++
	return i.idx < len(i.entries)
}

func (i *testIter) Current() (models.Tags, ts.Datapoints, xtime.Unit, []byte) {
	if len(i.entries) == 0 || i.idx < 0 || i.idx >= len(i.entries) {
		return models.EmptyTags(), nil, 0, nil
	}

	curr := i.entries[i.idx]
	return curr.tags, curr.datapoints, xtime.Second, curr.annotation
}

func (i *testIter) Reset() error {
	i.idx = -1
	return nil
}

func (i *testIter) Error() error {
	return nil
}

func (i *testIter) SetCurrentMetadata(metadata ts.Metadata) {
	i.metadatas[i.idx] = metadata
}

func (i *testIter) CurrentMetadata() ts.Metadata {
	if len(i.metadatas) == 0 {
		return ts.Metadata{}
	}
	return i.metadatas[i.idx]
}

func TestDownsampleAndWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	expectDefaultDownsampling(ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)
	expectDefaultStorageWrites(session, testDatapoints1, testAnnotation1)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, defaultOverride)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithDownsampleOverridesAndNoMappingRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	// We're overriding the downsampling with zero mapping rules, so we expect no data to be sent
	// to the downsampler, but everything to be written to storage.
	overrides := WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: nil,
	}

	expectDefaultStorageWrites(session, testDatapoints1, testAnnotation1)

	err := downAndWrite.Write(context.Background(),
		testTags1, testDatapoints1, xtime.Second, testAnnotation1, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithDownsampleOverridesAndMappingRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	// We're overriding the downsampling with mapping rules, so we expect data to be
	// sent to the downsampler, as well as everything being written to storage.
	mappingRules := []downsample.AutoMappingRule{
		{
			Aggregations: []aggregation.Type{aggregation.Mean},
			Policies: []policy.StoragePolicy{
				policy.NewStoragePolicy(
					time.Minute, xtime.Second, 48*time.Hour),
			},
		},
	}
	overrides := WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: mappingRules,
	}

	expectedSamplesAppenderOptions := downsample.SampleAppenderOptions{
		Override: true,
		OverrideRules: downsample.SamplesAppenderOverrideRules{
			MappingRules: mappingRules,
		},
	}

	expectDefaultDownsampling(ctrl, testDatapoints1, downsampler, expectedSamplesAppenderOptions)
	expectDefaultStorageWrites(session, testDatapoints1, testAnnotation1)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithDownsampleOverridesAndDropMappingRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	// We're overriding the downsampling with mapping rules, so we expect data to be
	// sent to the downsampler, as well as everything being written to storage.
	mappingRules := []downsample.AutoMappingRule{
		{
			Aggregations: []aggregation.Type{aggregation.Mean},
			Policies: []policy.StoragePolicy{
				policy.NewStoragePolicy(
					time.Minute, xtime.Second, 48*time.Hour),
			},
		},
	}
	overrides := WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: mappingRules,
	}

	expectedSamplesAppenderOptions := downsample.SampleAppenderOptions{
		Override: true,
		OverrideRules: downsample.SamplesAppenderOverrideRules{
			MappingRules: mappingRules,
		},
	}

	expectDefaultDownsampling(ctrl, testDatapoints1, downsampler, expectedSamplesAppenderOptions)
	expectDefaultStorageWrites(session, testDatapoints1, testAnnotation1)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithWriteOverridesAndNoStoragePolicies(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, _ := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	// We're overriding the write with zero storage policies, so we expect no data to be sent
	// to the storage, but everything to be written to the downsampler with the default settings.
	overrides := WriteOptions{
		WriteOverride:        true,
		WriteStoragePolicies: nil,
	}

	expectDefaultDownsampling(ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithWriteOverridesAndStoragePolicies(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	aggregatedNamespaces := []m3.AggregatedClusterNamespaceDefinition{
		{
			NamespaceID: ident.StringID("1m:48h"),
			Resolution:  time.Minute,
			Retention:   48 * time.Hour,
		},
		{
			NamespaceID: ident.StringID("10:24h"),
			Resolution:  10 * time.Second,
			Retention:   24 * time.Hour,
		},
	}
	downAndWrite, downsampler, session := newTestDownsamplerAndWriterWithAggregatedNamespace(
		t, ctrl, aggregatedNamespaces)

	// We're overriding the write with storage policies, so we expect data to be sent to the
	// storage with the specified policies, but everything to be written to the downsampler
	// with the default settings.
	overrides := WriteOptions{
		WriteOverride: true,
		WriteStoragePolicies: []policy.StoragePolicy{
			policy.NewStoragePolicy(
				time.Minute, xtime.Second, 48*time.Hour),
			policy.NewStoragePolicy(
				10*time.Second, xtime.Second, 24*time.Hour),
		},
	}

	expectDefaultDownsampling(ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)

	// All the datapoints will get written for each of the namespaces.
	for range aggregatedNamespaces {
		for _, dp := range testDatapoints1 {
			session.EXPECT().WriteTagged(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
		}
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteNoDownsampler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})
	downAndWrite.downsampler = nil

	expectDefaultStorageWrites(session, testDatapoints1, testAnnotation1)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, testAnnotation1, defaultOverride)
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	var (
		mockSamplesAppender = downsample.NewMockSamplesAppender(ctrl)
		mockMetricsAppender = downsample.NewMockMetricsAppender(ctrl)
	)

	mockMetricsAppender.
		EXPECT().
		SamplesAppender(zeroDownsamplerAppenderOpts).
		Return(downsample.SamplesAppenderResult{SamplesAppender: mockSamplesAppender}, nil).Times(2)
	for _, tag := range testTags1.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}
	for _, dp := range testDatapoints1 {
		mockSamplesAppender.EXPECT().AppendGaugeTimedSample(dp.Timestamp, dp.Value)
	}
	for _, tag := range testTags2.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}
	for _, dp := range testDatapoints2 {
		mockSamplesAppender.EXPECT().AppendGaugeTimedSample(dp.Timestamp, dp.Value)
	}
	downsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	mockMetricsAppender.EXPECT().Reset().Times(2)
	mockMetricsAppender.EXPECT().Finalize()

	for _, entry := range testEntries {
		for _, dp := range entry.datapoints {
			session.EXPECT().WriteTagged(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), entry.annotation,
			)
		}
	}

	iter := newTestIter(testEntries)
	err := downAndWrite.WriteBatch(context.Background(), iter, WriteOptions{})
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatchNoDownsampler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})
	downAndWrite.downsampler = nil

	for _, entry := range testEntries {
		for _, dp := range entry.datapoints {
			session.EXPECT().WriteTagged(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), entry.annotation,
			)
		}
	}

	iter := newTestIter(testEntries)
	err := downAndWrite.WriteBatch(context.Background(), iter, WriteOptions{})
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatchOverrideDownsampleRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, _ := newTestDownsamplerAndWriter(t, ctrl,
		testDownsamplerAndWriterOptions{})

	var (
		mockSamplesAppender  = downsample.NewMockSamplesAppender(ctrl)
		mockMetricsAppender  = downsample.NewMockMetricsAppender(ctrl)
		overrideMappingRules = []downsample.AutoMappingRule{
			downsample.AutoMappingRule{
				Aggregations: []aggregation.Type{
					aggregation.Sum,
				},
				Policies: policy.StoragePolicies{
					policy.MustParseStoragePolicy("1h:30d"),
				},
			},
		}
	)

	mockMetricsAppender.
		EXPECT().
		SamplesAppender(downsample.SampleAppenderOptions{
			Override: true,
			OverrideRules: downsample.SamplesAppenderOverrideRules{
				MappingRules: overrideMappingRules,
			},
		}).
		Return(downsample.SamplesAppenderResult{SamplesAppender: mockSamplesAppender}, nil)

	entries := testEntries[:1]
	for _, entry := range entries {
		for _, tag := range entry.tags.Tags {
			mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
		}
		for _, dp := range entry.datapoints {
			mockSamplesAppender.EXPECT().AppendGaugeTimedSample(dp.Timestamp, dp.Value)
		}
	}
	downsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	mockMetricsAppender.EXPECT().Reset()
	mockMetricsAppender.EXPECT().Finalize()

	iter := newTestIter(entries)
	err := downAndWrite.WriteBatch(context.Background(), iter, WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: overrideMappingRules,
		WriteOverride:          true,
		WriteStoragePolicies:   nil,
	})
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatchOverrideStoragePolicies(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testOpts := testDownsamplerAndWriterOptions{
		aggregatedNamespaces: []m3.AggregatedClusterNamespaceDefinition{
			m3.AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID("namespace_10m_7d"),
				Resolution:  10 * time.Minute,
				Retention:   7 * 24 * time.Hour,
			},
			m3.AggregatedClusterNamespaceDefinition{
				NamespaceID: ident.StringID("namespace_1h_60d"),
				Resolution:  time.Hour,
				Retention:   60 * 24 * time.Hour,
			},
		},
	}
	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl, testOpts)

	entries := testEntries[:1]
	for _, namespace := range testOpts.aggregatedNamespaces {
		for _, entry := range entries {
			for _, dp := range entry.datapoints {
				namespaceMatcher := ident.NewIDMatcher(namespace.NamespaceID.String())
				session.EXPECT().WriteTagged(
					namespaceMatcher, gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), entry.annotation,
				)
			}
		}
	}

	iter := newTestIter(entries)
	err := downAndWrite.WriteBatch(context.Background(), iter, WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: nil,
		WriteOverride:          true,
		WriteStoragePolicies: policy.StoragePolicies{
			policy.MustParseStoragePolicy("10m:7d"),
			policy.MustParseStoragePolicy("1h:60d"),
		},
	})
	require.NoError(t, err)
}

func expectDefaultDownsampling(
	ctrl *gomock.Controller, datapoints []ts.Datapoint,
	downsampler *downsample.MockDownsampler, downsampleOpts downsample.SampleAppenderOptions) {
	var (
		mockSamplesAppender = downsample.NewMockSamplesAppender(ctrl)
		mockMetricsAppender = downsample.NewMockMetricsAppender(ctrl)
	)

	mockMetricsAppender.
		EXPECT().
		SamplesAppender(downsampleOpts).
		Return(downsample.SamplesAppenderResult{SamplesAppender: mockSamplesAppender}, nil)
	for _, tag := range testTags1.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}

	for _, dp := range datapoints {
		mockSamplesAppender.EXPECT().AppendGaugeTimedSample(dp.Timestamp, dp.Value)
	}
	downsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	mockMetricsAppender.EXPECT().Finalize()
}

func expectDefaultStorageWrites(session *client.MockSession, datapoints []ts.Datapoint, annotation []byte) {
	for _, dp := range datapoints {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), annotation)
	}
}

type testDownsamplerAndWriterOptions struct {
	aggregatedNamespaces []m3.AggregatedClusterNamespaceDefinition
}

func newTestDownsamplerAndWriter(
	t *testing.T,
	ctrl *gomock.Controller,
	opts testDownsamplerAndWriterOptions,
) (*downsamplerAndWriter, *downsample.MockDownsampler, *client.MockSession) {
	var (
		storage storage.Storage
		session *client.MockSession
	)
	if ns := opts.aggregatedNamespaces; len(ns) > 0 {
		storage, session = testm3.NewStorageAndSessionWithAggregatedNamespaces(t, ctrl, ns)
	} else {
		storage, session = testm3.NewStorageAndSession(t, ctrl)
	}
	downsampler := downsample.NewMockDownsampler(ctrl)
	return NewDownsamplerAndWriter(instrument.NewOptions(), storage, downsampler, testWorkerPool).(*downsamplerAndWriter), downsampler, session
}

func newTestDownsamplerAndWriterWithAggregatedNamespace(
	t *testing.T,
	ctrl *gomock.Controller,
	aggregatedNamespaces []m3.AggregatedClusterNamespaceDefinition,
) (*downsamplerAndWriter, *downsample.MockDownsampler, *client.MockSession) {
	storage, session := testm3.NewStorageAndSessionWithAggregatedNamespaces(
		t, ctrl, aggregatedNamespaces)
	downsampler := downsample.NewMockDownsampler(ctrl)
	return NewDownsamplerAndWriter(instrument.NewOptions(), storage, downsampler, testWorkerPool).(*downsamplerAndWriter), downsampler, session
}

func init() {
	var err error
	testWorkerPool, err = xsync.NewPooledWorkerPool(
		16,
		xsync.NewPooledWorkerPoolOptions().
			SetGrowOnDemand(true),
	)

	if err != nil {
		panic(fmt.Sprintf("unable to create pooled worker pool: %v", err))
	}

	testWorkerPool.Init()
}
