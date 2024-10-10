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

package client

import (
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/uber-go/tally"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
	"github.com/m3db/m3/src/msg/producer"
	"github.com/m3db/m3/src/x/instrument"
)

func TestNewM3MsgClient(t *testing.T) {
	ctrl := gomock.NewController(t)
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))

	opts := NewM3MsgOptions().
		SetProducer(p)

	c, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NotNil(t, c)
	assert.NoError(t, err)
}
func TestWriteUntimedCounter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mock dependencies
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))
	p.EXPECT().Produce(gomock.Any()).Return(nil).AnyTimes()

	opts := NewM3MsgOptions().SetProducer(p)
	client, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NoError(t, err)

	// Mock metric and metadata
	counter := unaggregated.Counter{
		ID:    id.RawID("testCounter"),
		Value: 123,
	}
	metadatas := metadata.StagedMetadatas{}

	// Mock time function
	now := time.Now()
	client.(*M3MsgClient).nowFn = func() time.Time { return now }

	// Mock metrics
	client.(*M3MsgClient).metrics = m3msgClientMetrics{
		writeUntimedCounter: instrument.NewMethodMetrics(tally.NoopScope, "writeUntimedCounter", instrument.TimerOptions{}),
		totalBytesSent:      tally.NoopScope.Counter("bytesAdded"),
	}

	// Call the method
	err = client.WriteUntimedCounter(counter, metadatas)
	assert.NoError(t, err)
}

func TestWriteUntimedBatchTimer(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mock dependencies
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))
	p.EXPECT().Produce(gomock.Any()).Return(nil).AnyTimes()

	opts := NewM3MsgOptions().SetProducer(p)
	client, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NoError(t, err)

	// Mock metric and metadata
	batchTimer := unaggregated.BatchTimer{
		ID:     id.RawID("testBatchTimer"),
		Values: []float64{1, 2, 3},
	}
	metadatas := metadata.StagedMetadatas{}

	// Mock time function
	now := time.Now()
	client.(*M3MsgClient).nowFn = func() time.Time { return now }

	// Mock metrics
	client.(*M3MsgClient).metrics = m3msgClientMetrics{
		writeUntimedBatchTimer: instrument.NewMethodMetrics(tally.NoopScope, "writeUntimedBatchTimer", instrument.TimerOptions{}),
		totalBytesSent:         tally.NoopScope.Counter("bytesAdded"),
	}

	// Call the method
	err = client.WriteUntimedBatchTimer(batchTimer, metadatas)
	assert.NoError(t, err)
}

func TestWriteUntimedGauge(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mock dependencies
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))
	p.EXPECT().Produce(gomock.Any()).Return(nil).AnyTimes()

	opts := NewM3MsgOptions().SetProducer(p)
	client, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NoError(t, err)

	// Mock metric and metadata
	gauge := unaggregated.Gauge{
		ID:    id.RawID("testGauge"),
		Value: 123,
	}
	metadatas := metadata.StagedMetadatas{}

	// Mock time function
	now := time.Now()
	client.(*M3MsgClient).nowFn = func() time.Time { return now }

	// Mock metrics
	client.(*M3MsgClient).metrics = m3msgClientMetrics{
		writeUntimedGauge: instrument.NewMethodMetrics(tally.NoopScope, "writeUntimedGauge", instrument.TimerOptions{}),
		totalBytesSent:    tally.NoopScope.Counter("bytesAdded"),
	}

	// Call the method
	err = client.WriteUntimedGauge(gauge, metadatas)
	assert.NoError(t, err)
}

func TestTotalBytesAdded(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	// Mock dependencies
	p := producer.NewMockProducer(ctrl)
	p.EXPECT().Init()
	p.EXPECT().NumShards().Return(uint32(1))
	p.EXPECT().Produce(gomock.Any()).Return(nil).AnyTimes()

	opts := NewM3MsgOptions().SetProducer(p)
	client, err := NewM3MsgClient(NewOptions().SetM3MsgOptions(opts))
	assert.NoError(t, err)

	// Mock metric and metadata
	counter := unaggregated.Counter{
		ID:    id.RawID("testCounter"),
		Value: 123,
	}
	metadatas := metadata.StagedMetadatas{}

	// Mock time function
	now := time.Now()
	client.(*M3MsgClient).nowFn = func() time.Time { return now }

	testScope := tally.NewTestScope("", make(map[string]string))
	// Mock metrics
	client.(*M3MsgClient).metrics = m3msgClientMetrics{
		writeUntimedCounter: instrument.NewMethodMetrics(testScope, "writeUntimedCounter", instrument.TimerOptions{}),
		totalBytesSent:      testScope.Counter("total-bytes-sent"),
	}

	// Call the method
	err = client.WriteUntimedCounter(counter, metadatas)
	assert.NoError(t, err)

	// Verify the total bytes added
	assert.Equal(t, int64(23), testScope.Snapshot().Counters()["total-bytes-sent+"].Value())

}
