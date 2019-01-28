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
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/m3"
	testm3 "github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

var (
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

	testEntries = []testIterEntry{
		{tags: testTags1, datapoints: testDatapoints1},
		{tags: testTags2, datapoints: testDatapoints2},
	}

	defaultOverride = WriteOptions{}

	zeroDownsamplerAppenderOpts = downsample.SampleAppenderOptions{}
)

type testIter struct {
	idx     int
	entries []testIterEntry
}

type testIterEntry struct {
	tags       models.Tags
	datapoints []ts.Datapoint
}

func newTestIter(entries []testIterEntry) *testIter {
	return &testIter{
		idx:     -1,
		entries: entries,
	}
}

func (i *testIter) Next() bool {
	i.idx++
	return i.idx < len(i.entries)
}

func (i *testIter) Current() (models.Tags, ts.Datapoints, xtime.Unit) {
	if len(i.entries) == 0 || i.idx < 0 || i.idx >= len(i.entries) {
		return models.EmptyTags(), nil, 0
	}

	curr := i.entries[i.idx]
	return curr.tags, curr.datapoints, xtime.Second
}

func (i *testIter) Reset() error {
	i.idx = -1
	return nil
}

func (i *testIter) Error() error {
	return nil
}

func TestDownsampleAndWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl)

	expectDefaultDownsampling(t, ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, defaultOverride)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithDownsampleOverridesAndNoMappingRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl)

	// We're overriding the downsampling with zero mapping rules, so we expact no data to be sent
	// to the downsampler, but everything to be written to storage.
	overrides := WriteOptions{
		DownsampleOverride:     true,
		DownsampleMappingRules: nil,
	}

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithDownsampleOverridesAndMappingRules(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl)

	// We're overriding the downsampling with mapping rules, so we expact data to be
	// sent to the downsampler, as well as everything being written to storage.
	mappingRules := []downsample.MappingRule{
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

	expectDefaultDownsampling(t, ctrl, testDatapoints1, downsampler, expectedSamplesAppenderOptions)

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteWithWriteOverridesAndNoStoragePolicies(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, _ := newTestDownsamplerAndWriter(t, ctrl)

	// We're overriding the write with zero storage policies, so we expact no data to be sent
	// to the storage, but everything to be written to the downsampler with the default settings.
	overrides := WriteOptions{
		WriteOverride:        true,
		WriteStoragePolicies: nil,
	}

	expectDefaultDownsampling(t, ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, overrides)
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

	// We're overriding the write with storage policies, so we expact data to be sent to the
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

	expectDefaultDownsampling(t, ctrl, testDatapoints1, downsampler, zeroDownsamplerAppenderOpts)

	// All the datapoints will get written for each of the namespaces.
	for range aggregatedNamespaces {
		for _, dp := range testDatapoints1 {
			session.EXPECT().WriteTagged(
				gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
		}
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, overrides)
	require.NoError(t, err)
}

func TestDownsampleAndWriteNoDownsampler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl)
	downAndWrite.downsampler = nil

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	err := downAndWrite.Write(
		context.Background(), testTags1, testDatapoints1, xtime.Second, defaultOverride)
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatch(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, downsampler, session := newTestDownsamplerAndWriter(t, ctrl)

	var (
		mockSamplesAppender = downsample.NewMockSamplesAppender(ctrl)
		mockMetricsAppender = downsample.NewMockMetricsAppender(ctrl)
	)

	mockMetricsAppender.
		EXPECT().
		SamplesAppender(zeroDownsamplerAppenderOpts).
		Return(mockSamplesAppender, nil).Times(2)
	for _, tag := range testTags1.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}
	for _, dp := range testDatapoints1 {
		mockSamplesAppender.EXPECT().AppendGaugeSample(dp.Value)
	}
	for _, tag := range testTags2.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}
	for _, dp := range testDatapoints2 {
		mockSamplesAppender.EXPECT().AppendGaugeSample(dp.Value)
	}
	downsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	mockMetricsAppender.EXPECT().Reset().Times(2)
	mockMetricsAppender.EXPECT().Finalize()

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}
	for _, dp := range testDatapoints2 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	iter := newTestIter(testEntries)
	err := downAndWrite.WriteBatch(context.Background(), iter)
	require.NoError(t, err)
}

func TestDownsampleAndWriteBatchNoDownsampler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	downAndWrite, _, session := newTestDownsamplerAndWriter(t, ctrl)
	downAndWrite.downsampler = nil

	for _, dp := range testDatapoints1 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}
	for _, dp := range testDatapoints2 {
		session.EXPECT().WriteTagged(
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), dp.Value, gomock.Any(), gomock.Any())
	}

	iter := newTestIter(testEntries)
	err := downAndWrite.WriteBatch(context.Background(), iter)
	require.NoError(t, err)
}

func expectDefaultDownsampling(
	t *testing.T, ctrl *gomock.Controller, datapoints []ts.Datapoint,
	downsampler *downsample.MockDownsampler, downsampleOpts downsample.SampleAppenderOptions) {
	var (
		mockSamplesAppender = downsample.NewMockSamplesAppender(ctrl)
		mockMetricsAppender = downsample.NewMockMetricsAppender(ctrl)
	)

	mockMetricsAppender.
		EXPECT().
		SamplesAppender(downsampleOpts).
		Return(mockSamplesAppender, nil)
	for _, tag := range testTags1.Tags {
		mockMetricsAppender.EXPECT().AddTag(tag.Name, tag.Value)
	}

	for _, dp := range testDatapoints1 {
		mockSamplesAppender.EXPECT().AppendGaugeSample(dp.Value)
	}
	downsampler.EXPECT().NewMetricsAppender().Return(mockMetricsAppender, nil)

	mockMetricsAppender.EXPECT().Finalize()
}

func newTestDownsamplerAndWriter(
	t *testing.T,
	ctrl *gomock.Controller,
) (*downsamplerAndWriter, *downsample.MockDownsampler, *client.MockSession) {
	storage, session := testm3.NewStorageAndSession(t, ctrl)
	downsampler := downsample.NewMockDownsampler(ctrl)
	return NewDownsamplerAndWriter(storage, downsampler).(*downsamplerAndWriter), downsampler, session
}

func newTestDownsamplerAndWriterWithAggregatedNamespace(
	t *testing.T,
	ctrl *gomock.Controller,
	aggregatedNamespaces []m3.AggregatedClusterNamespaceDefinition,
) (*downsamplerAndWriter, *downsample.MockDownsampler, *client.MockSession) {
	storage, session := testm3.NewStorageAndSessionWithAggregatedNamespaces(
		t, ctrl, aggregatedNamespaces)
	downsampler := downsample.NewMockDownsampler(ctrl)
	return NewDownsamplerAndWriter(storage, downsampler).(*downsamplerAndWriter), downsampler, session
}
