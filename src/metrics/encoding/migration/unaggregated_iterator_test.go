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
	"bytes"
	"io"
	"testing"
	"time"

	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/metrics/encoding"
	"github.com/m3db/m3/src/metrics/encoding/msgpack"
	"github.com/m3db/m3/src/metrics/encoding/protobuf"
	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/metrics/pipeline"
	"github.com/m3db/m3/src/metrics/pipeline/applied"
	"github.com/m3db/m3/src/metrics/policy"
	"github.com/m3db/m3/src/metrics/transformation"
	xtime "github.com/m3db/m3x/time"

	"github.com/google/go-cmp/cmp"
	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/stretchr/testify/require"
)

var (
	testCounter1 = unaggregated.Counter{
		ID:    []byte("testCounter1"),
		Value: 123,
	}
	testCounter2 = unaggregated.Counter{
		ID:    []byte("testCounter2"),
		Value: 456,
	}
	testBatchTimer1 = unaggregated.BatchTimer{
		ID:     []byte("testBatchTimer1"),
		Values: []float64{3.67, -9.38},
	}
	testBatchTimer2 = unaggregated.BatchTimer{
		ID:     []byte("testBatchTimer2"),
		Values: []float64{4.57, 189234.01},
	}
	testGauge1 = unaggregated.Gauge{
		ID:    []byte("testGauge1"),
		Value: 845.23,
	}
	testGauge2 = unaggregated.Gauge{
		ID:    []byte("testGauge2"),
		Value: 234231.345,
	}
	testPoliciesList1 = policy.DefaultPoliciesList
	testPoliciesList2 = policy.PoliciesList{
		// Default staged policies.
		policy.DefaultStagedPolicies,

		// Single pipeline,
		policy.NewStagedPolicies(
			1234,
			false,
			[]policy.Policy{
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
					aggregation.DefaultID,
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
					aggregation.DefaultID,
				),
			},
		),

		// Multiple pipelines.
		policy.NewStagedPolicies(
			5678,
			true,
			[]policy.Policy{
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
					aggregation.DefaultID,
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
					aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
					aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
				),
				policy.NewPolicy(
					policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
					aggregation.MustCompressTypes(aggregation.Sum),
				),
			},
		),
	}
	testMetadatas1 = metadata.StagedMetadatas{
		{
			CutoverNanos: 1234,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(10*time.Second, xtime.Second, time.Hour),
						},
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("baz"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Mean),
								},
							},
						}),
					},
				},
			},
		},
	}
	testMetadatas2 = metadata.StagedMetadatas{
		{
			CutoverNanos: 4567,
			Tombstoned:   false,
		},
		{
			CutoverNanos: 7890,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Count),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
							policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
						},
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.Absolute,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("foo"),
									AggregationID: aggregation.MustCompressTypes(aggregation.Last, aggregation.Sum),
								},
							},
						}),
					},
				},
			},
		},
		{
			CutoverNanos: 32768,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						Pipeline: applied.NewPipeline([]applied.OpUnion{
							{
								Type: pipeline.TransformationOpType,
								Transformation: pipeline.TransformationOp{
									Type: transformation.PerSecond,
								},
							},
							{
								Type: pipeline.RollupOpType,
								Rollup: applied.RollupOp{
									ID:            []byte("bar"),
									AggregationID: aggregation.MustCompressTypes(aggregation.P99),
								},
							},
						}),
					},
				},
			},
		},
	}
	testConvertedMetadatas1 = metadata.DefaultStagedMetadatas
	testConvertedMetadatas2 = metadata.StagedMetadatas{
		metadata.DefaultStagedMetadata,
		metadata.StagedMetadata{
			CutoverNanos: 1234,
			Tombstoned:   false,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
						},
					},
				},
			},
		},
		metadata.StagedMetadata{
			CutoverNanos: 5678,
			Tombstoned:   true,
			Metadata: metadata.Metadata{
				Pipelines: []metadata.PipelineMetadata{
					{
						AggregationID: aggregation.DefaultID,
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour),
						},
					},
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Count, aggregation.Last),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Minute, xtime.Minute, 6*time.Hour),
							policy.NewStoragePolicy(10*time.Minute, xtime.Minute, 24*time.Hour),
						},
					},
					{
						AggregationID: aggregation.MustCompressTypes(aggregation.Sum),
						StoragePolicies: []policy.StoragePolicy{
							policy.NewStoragePolicy(time.Hour, xtime.Hour, 30*24*time.Hour),
						},
					},
				},
			},
		},
	}
	testCmpOpts = []cmp.Option{
		cmpopts.EquateEmpty(),
		cmp.AllowUnexported(policy.StoragePolicy{}),
	}
)

func TestUnaggregatedIteratorDecodeCounterWithPoliciesList(t *testing.T) {
	inputs := []unaggregated.CounterWithPoliciesList{
		{
			Counter:      testCounter1,
			PoliciesList: testPoliciesList1,
		},
		{
			Counter:      testCounter1,
			PoliciesList: testPoliciesList2,
		},
		{
			Counter:      testCounter2,
			PoliciesList: testPoliciesList1,
		},
		{
			Counter:      testCounter2,
			PoliciesList: testPoliciesList2,
		},
	}
	expected := []unaggregated.CounterWithMetadatas{
		{
			Counter:         testCounter1,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			Counter:         testCounter1,
			StagedMetadatas: testConvertedMetadatas2,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testConvertedMetadatas2,
		},
	}

	encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewBufferedEncoder())
	for _, input := range inputs {
		require.NoError(t, encoder.EncodeCounterWithPoliciesList(input))
	}
	var (
		i      int
		stream = bytes.NewReader(encoder.Encoder().Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.CounterWithMetadatasType, res.Type)
		require.Equal(t, expected[i], res.CounterWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithPoliciesList(t *testing.T) {
	inputs := []unaggregated.BatchTimerWithPoliciesList{
		{
			BatchTimer:   testBatchTimer1,
			PoliciesList: testPoliciesList1,
		},
		{
			BatchTimer:   testBatchTimer1,
			PoliciesList: testPoliciesList2,
		},
		{
			BatchTimer:   testBatchTimer2,
			PoliciesList: testPoliciesList1,
		},
		{
			BatchTimer:   testBatchTimer2,
			PoliciesList: testPoliciesList2,
		},
	}
	expected := []unaggregated.BatchTimerWithMetadatas{
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testConvertedMetadatas2,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testConvertedMetadatas2,
		},
	}

	encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewBufferedEncoder())
	for _, input := range inputs {
		require.NoError(t, encoder.EncodeBatchTimerWithPoliciesList(input))
	}
	var (
		i      int
		stream = bytes.NewReader(encoder.Encoder().Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.BatchTimerWithMetadatasType, res.Type)
		require.Equal(t, expected[i], res.BatchTimerWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeGaugeWithPoliciesList(t *testing.T) {
	inputs := []unaggregated.GaugeWithPoliciesList{
		{
			Gauge:        testGauge1,
			PoliciesList: testPoliciesList1,
		},
		{
			Gauge:        testGauge1,
			PoliciesList: testPoliciesList2,
		},
		{
			Gauge:        testGauge2,
			PoliciesList: testPoliciesList1,
		},
		{
			Gauge:        testGauge2,
			PoliciesList: testPoliciesList2,
		},
	}
	expected := []unaggregated.GaugeWithMetadatas{
		{
			Gauge:           testGauge1,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			Gauge:           testGauge1,
			StagedMetadatas: testConvertedMetadatas2,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testConvertedMetadatas1,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testConvertedMetadatas2,
		},
	}

	encoder := msgpack.NewUnaggregatedEncoder(msgpack.NewBufferedEncoder())
	for _, input := range inputs {
		require.NoError(t, encoder.EncodeGaugeWithPoliciesList(input))
	}
	var (
		i      int
		stream = bytes.NewReader(encoder.Encoder().Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.GaugeWithMetadatasType, res.Type)
		require.Equal(t, expected[i], res.GaugeWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeCounterWithMetadatas(t *testing.T) {
	inputs := []unaggregated.CounterWithMetadatas{
		{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas1,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas1,
		},
		{
			Counter:         testCounter1,
			StagedMetadatas: testMetadatas2,
		},
		{
			Counter:         testCounter2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:                 encoding.CounterWithMetadatasType,
			CounterWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.CounterWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.CounterWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeBatchTimerWithMetadatas(t *testing.T) {
	inputs := []unaggregated.BatchTimerWithMetadatas{
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas1,
		},
		{
			BatchTimer:      testBatchTimer1,
			StagedMetadatas: testMetadatas2,
		},
		{
			BatchTimer:      testBatchTimer2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type: encoding.BatchTimerWithMetadatasType,
			BatchTimerWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.BatchTimerWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.BatchTimerWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeGaugeWithMetadatas(t *testing.T) {
	inputs := []unaggregated.GaugeWithMetadatas{
		{
			Gauge:           testGauge1,
			StagedMetadatas: testMetadatas1,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas1,
		},
		{
			Gauge:           testGauge1,
			StagedMetadatas: testMetadatas2,
		},
		{
			Gauge:           testGauge2,
			StagedMetadatas: testMetadatas2,
		},
	}

	enc := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
	for _, input := range inputs {
		require.NoError(t, enc.EncodeMessage(encoding.UnaggregatedMessageUnion{
			Type:               encoding.GaugeWithMetadatasType,
			GaugeWithMetadatas: input,
		}))
	}
	dataBuf := enc.Relinquish()
	defer dataBuf.Close()

	var (
		i      int
		stream = bytes.NewReader(dataBuf.Bytes())
	)
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		require.Equal(t, encoding.GaugeWithMetadatasType, res.Type)
		require.Equal(t, inputs[i], res.GaugeWithMetadatas)
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, len(inputs), i)
}

func TestUnaggregatedIteratorDecodeStress(t *testing.T) {
	var (
		numIter                    = 1000
		testCounters               = []unaggregated.Counter{testCounter1, testCounter2}
		testBatchTimers            = []unaggregated.BatchTimer{testBatchTimer1, testBatchTimer2}
		testGauges                 = []unaggregated.Gauge{testGauge1, testGauge2}
		testPoliciesLists          = []policy.PoliciesList{testPoliciesList1, testPoliciesList2}
		testMetadataLists          = []metadata.StagedMetadatas{testMetadatas1, testMetadatas2}
		testConvertedMetadataLists = []metadata.StagedMetadatas{testConvertedMetadatas1, testConvertedMetadatas2}
		msgpackInputs              []interface{}
		expectedMsgpackOutputs     []encoding.UnaggregatedMessageUnion
		protobufInputs             []interface{}
	)

	for metricIdx := 0; metricIdx < 2; metricIdx++ {
		for metadataIdx := 0; metadataIdx < 2; metadataIdx++ {
			msgpackInputs = append(msgpackInputs, unaggregated.CounterWithPoliciesList{
				Counter:      testCounters[metricIdx],
				PoliciesList: testPoliciesLists[metadataIdx],
			})
			msgpackInputs = append(msgpackInputs, unaggregated.BatchTimerWithPoliciesList{
				BatchTimer:   testBatchTimers[metricIdx],
				PoliciesList: testPoliciesLists[metadataIdx],
			})
			msgpackInputs = append(msgpackInputs, unaggregated.GaugeWithPoliciesList{
				Gauge:        testGauges[metricIdx],
				PoliciesList: testPoliciesLists[metadataIdx],
			})
			expectedMsgpackOutputs = append(expectedMsgpackOutputs, encoding.UnaggregatedMessageUnion{
				Type: encoding.CounterWithMetadatasType,
				CounterWithMetadatas: unaggregated.CounterWithMetadatas{
					Counter:         testCounters[metricIdx],
					StagedMetadatas: testConvertedMetadataLists[metadataIdx],
				},
			})
			expectedMsgpackOutputs = append(expectedMsgpackOutputs, encoding.UnaggregatedMessageUnion{
				Type: encoding.BatchTimerWithMetadatasType,
				BatchTimerWithMetadatas: unaggregated.BatchTimerWithMetadatas{
					BatchTimer:      testBatchTimers[metricIdx],
					StagedMetadatas: testConvertedMetadataLists[metadataIdx],
				},
			})
			expectedMsgpackOutputs = append(expectedMsgpackOutputs, encoding.UnaggregatedMessageUnion{
				Type: encoding.GaugeWithMetadatasType,
				GaugeWithMetadatas: unaggregated.GaugeWithMetadatas{
					Gauge:           testGauges[metricIdx],
					StagedMetadatas: testConvertedMetadataLists[metadataIdx],
				},
			})
		}
	}

	for metricIdx := 0; metricIdx < 2; metricIdx++ {
		for metadataIdx := 0; metadataIdx < 2; metadataIdx++ {
			protobufInputs = append(protobufInputs, unaggregated.CounterWithMetadatas{
				Counter:         testCounters[metricIdx],
				StagedMetadatas: testMetadataLists[metadataIdx],
			})
			protobufInputs = append(protobufInputs, unaggregated.BatchTimerWithMetadatas{
				BatchTimer:      testBatchTimers[metricIdx],
				StagedMetadatas: testMetadataLists[metadataIdx],
			})
			protobufInputs = append(protobufInputs, unaggregated.GaugeWithMetadatas{
				Gauge:           testGauges[metricIdx],
				StagedMetadatas: testMetadataLists[metadataIdx],
			})
		}
	}

	var stream bytes.Buffer
	for iter := 0; iter < numIter; iter++ {
		msgpackEncoder := msgpack.NewUnaggregatedEncoder(msgpack.NewBufferedEncoder())
		protobufEncoder := protobuf.NewUnaggregatedEncoder(protobuf.NewUnaggregatedOptions())
		for _, input := range msgpackInputs {
			switch input := input.(type) {
			case unaggregated.CounterWithPoliciesList:
				require.NoError(t, msgpackEncoder.EncodeCounterWithPoliciesList(input))
			case unaggregated.BatchTimerWithPoliciesList:
				require.NoError(t, msgpackEncoder.EncodeBatchTimerWithPoliciesList(input))
			case unaggregated.GaugeWithPoliciesList:
				require.NoError(t, msgpackEncoder.EncodeGaugeWithPoliciesList(input))
			default:
				require.Fail(t, "unrecognized type %T", input)
			}
		}
		for _, input := range protobufInputs {
			var msg encoding.UnaggregatedMessageUnion
			switch input := input.(type) {
			case unaggregated.CounterWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:                 encoding.CounterWithMetadatasType,
					CounterWithMetadatas: input,
				}
			case unaggregated.BatchTimerWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type: encoding.BatchTimerWithMetadatasType,
					BatchTimerWithMetadatas: input,
				}
			case unaggregated.GaugeWithMetadatas:
				msg = encoding.UnaggregatedMessageUnion{
					Type:               encoding.GaugeWithMetadatasType,
					GaugeWithMetadatas: input,
				}
			default:
				require.Fail(t, "unrecognized type %T", input)
			}
			require.NoError(t, protobufEncoder.EncodeMessage(msg))
		}
		_, err := stream.Write(msgpackEncoder.Encoder().Bytes())
		require.NoError(t, err)
		dataBuf := protobufEncoder.Relinquish()
		_, err = stream.Write(dataBuf.Bytes())
		require.NoError(t, err)
		dataBuf.Close()
	}

	var (
		i      int
		reader = bytes.NewReader(stream.Bytes())
	)
	it := NewUnaggregatedIterator(
		reader,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	for it.Next() {
		res := it.Current()
		j := i % (len(msgpackInputs) + len(protobufInputs))
		if j < len(msgpackInputs) {
			require.True(t, cmp.Equal(expectedMsgpackOutputs[j], res, testCmpOpts...))
		} else {
			// Protobuf encoded data.
			j -= len(msgpackInputs)
			switch expectedRes := protobufInputs[j].(type) {
			case unaggregated.CounterWithMetadatas:
				require.Equal(t, encoding.CounterWithMetadatasType, res.Type)
				require.True(t, cmp.Equal(expectedRes, res.CounterWithMetadatas, testCmpOpts...))
			case unaggregated.BatchTimerWithMetadatas:
				require.Equal(t, encoding.BatchTimerWithMetadatasType, res.Type)
				require.True(t, cmp.Equal(expectedRes, res.BatchTimerWithMetadatas, testCmpOpts...))
			case unaggregated.GaugeWithMetadatas:
				require.Equal(t, encoding.GaugeWithMetadatasType, res.Type)
				require.True(t, cmp.Equal(expectedRes, res.GaugeWithMetadatas, testCmpOpts...))
			default:
				require.Fail(t, "unknown input type: %T", protobufInputs[j])
			}
		}
		i++
	}
	require.Equal(t, io.EOF, it.Err())
	require.Equal(t, (len(msgpackInputs)+len(protobufInputs))*numIter, i)
}

func TestUnaggregatedIteratorDecodeMsgpackError(t *testing.T) {
	reader := bytes.NewReader([]byte{0x1, 0x2})
	it := NewUnaggregatedIterator(
		reader,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	i := 0
	for it.Next() {
		i++
	}
	require.NotEqual(t, io.EOF, it.Err())
	require.Equal(t, it.Err(), it.(*unaggregatedIterator).msgpackIt.Err())
	require.Equal(t, 0, i)

	// Verify calling Next() still returns false.
	require.False(t, it.Next())
	require.Equal(t, it.Err(), it.(*unaggregatedIterator).msgpackIt.Err())
}

func TestUnaggregatedIteratorDecodeProtobufError(t *testing.T) {
	reader := bytes.NewReader([]byte{0x2, 0x2})
	it := NewUnaggregatedIterator(
		reader,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	defer it.Close()
	i := 0
	for it.Next() {
		i++
	}
	require.NotEqual(t, io.EOF, it.Err())
	require.Equal(t, it.Err(), it.(*unaggregatedIterator).protobufIt.Err())
	require.Equal(t, 0, i)

	// Verify calling Next() still returns false.
	require.False(t, it.Next())
	require.Equal(t, it.Err(), it.(*unaggregatedIterator).protobufIt.Err())
}

func TestUnaggregatedIteratorNextOnClose(t *testing.T) {
	stream := bytes.NewReader([]byte{0x1, 0x2, 0x3})
	it := NewUnaggregatedIterator(
		stream,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	iterator := it.(*unaggregatedIterator)
	require.False(t, iterator.closed)
	require.Nil(t, it.Err())

	// Verify that closing the iterator cleans up the state.
	it.Close()
	require.False(t, it.Next())
	require.False(t, it.Next())
	require.True(t, iterator.closed)
	require.Equal(t, encoding.UnaggregatedMessageUnion{}, iterator.msg)

	// Verify that closing a second time is a no op.
	it.Close()
}

func TestUnaggregatedIteratorClose(t *testing.T) {
	it := NewUnaggregatedIterator(
		nil,
		msgpack.NewUnaggregatedIteratorOptions(),
		protobuf.NewUnaggregatedOptions(),
	)
	require.False(t, it.(*unaggregatedIterator).closed)
	require.NotNil(t, it.(*unaggregatedIterator).msgpackIt)
	require.NotNil(t, it.(*unaggregatedIterator).protobufIt)

	it.Close()
	require.True(t, it.(*unaggregatedIterator).closed)
	require.Nil(t, it.(*unaggregatedIterator).msgpackIt)
	require.Nil(t, it.(*unaggregatedIterator).protobufIt)

	// Verify that closing a second time is a no op.
	it.Close()
	require.True(t, it.(*unaggregatedIterator).closed)
	require.Nil(t, it.(*unaggregatedIterator).msgpackIt)
	require.Nil(t, it.(*unaggregatedIterator).protobufIt)
}
