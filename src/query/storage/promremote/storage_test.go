// Copyright (c) 2021  Uber Technologies, Inc.
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

package promremote

import (
	"context"
	"io"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/storage/promremote/promremotetest"
	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/tallytest"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	logger, _ = zap.NewDevelopment()
	scope     = tally.NewTestScope("test_scope", map[string]string{})
)

func TestWrite(t *testing.T) {
	fakeProm := promremotetest.NewServer(t)
	defer fakeProm.Close()

	promStorage, err := NewStorage(Options{
		endpoints: []EndpointOptions{{name: "testEndpoint", address: fakeProm.WriteAddr()}},
		scope:     scope,
		logger:    logger,
	})
	require.NoError(t, err)
	defer closeWithCheck(t, promStorage)

	now := xtime.Now()
	wq, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: models.Tags{
			Opts: models.NewTagOptions(),
			Tags: []models.Tag{{
				Name:  []byte("test_tag_name"),
				Value: []byte("test_tag_value"),
			}},
		},
		Datapoints: ts.Datapoints{{
			Timestamp: now,
			Value:     42,
		}},
		Unit: xtime.Millisecond,
	})
	require.NoError(t, err)
	err = promStorage.Write(context.TODO(), wq)
	require.NoError(t, err)

	promWrite := fakeProm.GetLastWriteRequest()

	expectedLabel := prompb.Label{
		Name:  "test_tag_name",
		Value: "test_tag_value",
	}
	expectedSample := prompb.Sample{
		Value:     42,
		Timestamp: now.ToNormalizedTime(time.Millisecond),
	}
	require.Len(t, promWrite.Timeseries, 1)
	require.Len(t, promWrite.Timeseries[0].Labels, 1)
	require.Len(t, promWrite.Timeseries[0].Samples, 1)
	assert.Equal(t, expectedLabel, promWrite.Timeseries[0].Labels[0])
	assert.Equal(t, expectedSample, promWrite.Timeseries[0].Samples[0])

	tallytest.AssertCounterValue(
		t, 1, scope.Snapshot(), "test_scope.prom_remote_storage.writeSingle.success",
		map[string]string{"endpoint_name": "testEndpoint"},
	)
	tallytest.AssertCounterValue(
		t, 0, scope.Snapshot(), "test_scope.prom_remote_storage.writeSingle.errors",
		map[string]string{"endpoint_name": "testEndpoint"},
	)
}

func TestWriteBasedOnRetention(t *testing.T) {
	promShortRetention := promremotetest.NewServer(t)
	defer promShortRetention.Close()
	promMediumRetention := promremotetest.NewServer(t)
	defer promMediumRetention.Close()
	promLongRetention := promremotetest.NewServer(t)
	defer promLongRetention.Close()
	promLongRetention2 := promremotetest.NewServer(t)
	defer promLongRetention2.Close()
	reset := func() {
		promShortRetention.Reset()
		promMediumRetention.Reset()
		promLongRetention.Reset()
		promLongRetention2.Reset()
	}

	mediumRetentionAttr := storagemetadata.Attributes{
		MetricsType: storagemetadata.AggregatedMetricsType,
		Retention:   720 * time.Hour,
		Resolution:  5 * time.Minute,
	}
	shortRetentionAttr := storagemetadata.Attributes{
		MetricsType: storagemetadata.AggregatedMetricsType,
		Retention:   120 * time.Hour,
		Resolution:  15 * time.Second,
	}
	longRetentionAttr := storagemetadata.Attributes{
		Resolution: 10 * time.Minute,
		Retention:  8760 * time.Hour,
	}
	promStorage, err := NewStorage(Options{
		endpoints: []EndpointOptions{
			{
				address:    promShortRetention.WriteAddr(),
				attributes: shortRetentionAttr,
			},
			{
				address:    promMediumRetention.WriteAddr(),
				attributes: mediumRetentionAttr,
			},
			{
				address:    promLongRetention.WriteAddr(),
				attributes: longRetentionAttr,
			},
			{
				address:    promLongRetention2.WriteAddr(),
				attributes: longRetentionAttr,
			},
		},
		scope:  scope,
		logger: logger,
	})
	require.NoError(t, err)
	defer closeWithCheck(t, promStorage)

	t.Run("send short retention write", func(t *testing.T) {
		reset()
		err := writeTestMetric(t, promStorage, shortRetentionAttr)
		require.NoError(t, err)
		assert.NotNil(t, promShortRetention.GetLastWriteRequest())
		assert.Nil(t, promMediumRetention.GetLastWriteRequest())
		assert.Nil(t, promLongRetention.GetLastWriteRequest())
	})

	t.Run("send medium retention write", func(t *testing.T) {
		reset()
		err := writeTestMetric(t, promStorage, mediumRetentionAttr)
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastWriteRequest())
		assert.NotNil(t, promMediumRetention.GetLastWriteRequest())
		assert.Nil(t, promLongRetention.GetLastWriteRequest())
	})

	t.Run("send write to multiple instances configured with same retention", func(t *testing.T) {
		reset()
		err := writeTestMetric(t, promStorage, longRetentionAttr)
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastWriteRequest())
		assert.Nil(t, promMediumRetention.GetLastWriteRequest())
		assert.NotNil(t, promLongRetention.GetLastWriteRequest())
		assert.NotNil(t, promLongRetention2.GetLastWriteRequest())
	})

	t.Run("send unconfigured retention write", func(t *testing.T) {
		reset()
		err := writeTestMetric(t, promStorage, storagemetadata.Attributes{
			Resolution: mediumRetentionAttr.Resolution + 1,
			Retention:  mediumRetentionAttr.Retention,
		})
		require.Error(t, err)
		err = writeTestMetric(t, promStorage, storagemetadata.Attributes{
			Resolution: mediumRetentionAttr.Resolution,
			Retention:  mediumRetentionAttr.Retention + 1,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "write did not match any of known endpoints")
		assert.Nil(t, promShortRetention.GetLastWriteRequest())
		assert.Nil(t, promMediumRetention.GetLastWriteRequest())
		assert.Nil(t, promLongRetention.GetLastWriteRequest())
		const droppedWrites = "test_scope.prom_remote_storage.dropped_writes"
		tallytest.AssertCounterValue(t, 2, scope.Snapshot(), droppedWrites, map[string]string{})
	})

	t.Run("error should not prevent sending to other instances", func(t *testing.T) {
		reset()
		promLongRetention.SetError("test err", http.StatusInternalServerError)
		err := writeTestMetric(t, promStorage, longRetentionAttr)
		require.Error(t, err)
		assert.Contains(t, err.Error(), "test err")
		assert.NotNil(t, promLongRetention2.GetLastWriteRequest())
	})
}

func TestErrorHandling(t *testing.T) {
	svr := promremotetest.NewServer(t)
	defer svr.Close()

	attr := storagemetadata.Attributes{
		MetricsType: storagemetadata.AggregatedMetricsType,
		Retention:   720 * time.Hour,
		Resolution:  5 * time.Minute,
	}
	promStorage, err := NewStorage(Options{
		endpoints: []EndpointOptions{{address: svr.WriteAddr(), attributes: attr}},
		scope:     scope,
		logger:    logger,
	})
	require.NoError(t, err)
	defer closeWithCheck(t, promStorage)

	t.Run("wrap non 5xx errors as invalid params error", func(t *testing.T) {
		svr.Reset()
		svr.SetError("test err", http.StatusForbidden)
		err := writeTestMetric(t, promStorage, attr)
		require.Error(t, err)
		assert.True(t, xerrors.IsInvalidParams(err))
	})

	t.Run("429 should not be wrapped as invalid params", func(t *testing.T) {
		svr.Reset()
		svr.SetError("test err", http.StatusTooManyRequests)
		err := writeTestMetric(t, promStorage, attr)
		require.Error(t, err)
		assert.False(t, xerrors.IsInvalidParams(err))
	})
}

func closeWithCheck(t *testing.T, c io.Closer) {
	require.NoError(t, c.Close())
}

func writeTestMetric(t *testing.T, s storage.Storage, attr storagemetadata.Attributes) error {
	//nolint: gosec
	datapoint := ts.Datapoint{Value: rand.Float64(), Timestamp: xtime.Now()}
	wq, err := storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: models.Tags{
			Opts: models.NewTagOptions(),
			Tags: []models.Tag{{
				Name:  []byte("test_tag_name"),
				Value: []byte("test_tag_value"),
			}},
		},
		Datapoints: ts.Datapoints{datapoint},
		Unit:       xtime.Millisecond,
		Attributes: attr,
	})
	require.NoError(t, err)
	return s.Write(context.TODO(), wq)
}
