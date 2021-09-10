package promremotewrite

import (
	"context"
	"errors"
	"math/rand"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/storage/promremotewrite/promremotewritetest"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"
	"github.com/prometheus/prometheus/prompb"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWrite(t *testing.T) {
	tcs := []struct {
		name       string
		tags       []models.Tag
		datapoints ts.Datapoints

		expectedLabels  []prompb.Label
		expectedSamples []prompb.Sample
	}{
		{
			name: "write single datapoint with labels",
			tags: []models.Tag{{
				Name:  []byte("test_tag_name"),
				Value: []byte("test_tag_value"),
			}},
			datapoints: ts.Datapoints{{
				Timestamp: xtime.UnixNano(time.Second),
				Value:     42,
			}},

			expectedLabels: []prompb.Label{{
				Name:  "test_tag_name",
				Value: "test_tag_value",
			}},
			expectedSamples: []prompb.Sample{{
				Value:     42,
				Timestamp: int64(1000),
			}},
		},
	}

	fakeProm, closeFn := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn()

	for _, tc := range tcs {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			promStorage, closeFn, err := NewStorage(Options{endpoints: []EndpointOptions{{address: fakeProm.HTTPAddr()}}})
			require.NoError(t, err)
			defer closeFn()

			wq, err := storage.NewWriteQuery(storage.WriteQueryOptions{
				Tags: models.Tags{
					Opts: models.NewTagOptions(),
					Tags: tc.tags,
				},
				Datapoints: tc.datapoints,
				// TODO what is the meaning of this?
				Unit: xtime.Millisecond,
			})
			require.NoError(t, err)
			err = promStorage.Write(context.TODO(), wq)
			require.NoError(t, err)

			promWrite := fakeProm.GetLastRequest()
			require.Len(t, promWrite.Timeseries, 1)
			require.Len(t, promWrite.Timeseries[0].Labels, len(tc.expectedLabels))
			require.Len(t, promWrite.Timeseries[0].Samples, len(tc.expectedSamples))

			for i := 0; i < len(tc.expectedLabels); i++ {
				assert.Equal(t, promWrite.Timeseries[0].Labels[i], tc.expectedLabels[i])
			}
			for i := 0; i < len(tc.expectedSamples); i++ {
				assert.Equal(t, promWrite.Timeseries[0].Samples[i], tc.expectedSamples[i])
			}
			assertRemoteWriteHeadersSetCorrectly(t, fakeProm.GetLastHTTPRequest())
		})
	}
}

func TestWriteBasedOnRetention(t *testing.T) {
	promShortRetention, closeFn1 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn1()
	promMediumRetention, closeFn2 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn2()
	promLongRetention, closeFn3 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn3()
	promLongRetention2, closeFn4 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn4()
	reset := func() {
		promShortRetention.Reset()
		promMediumRetention.Reset()
		promLongRetention.Reset()
	}

	promStorage, storageCloseFn, err := NewStorage(Options{endpoints: []EndpointOptions{
		{
			address:    promShortRetention.HTTPAddr(),
			retention:  120 * time.Hour,
			resolution: 15 * time.Second,
		},
		{
			address:    promMediumRetention.HTTPAddr(),
			retention:  720 * time.Hour,
			resolution: 5 * time.Minute,
		},
		{
			address:    promLongRetention.HTTPAddr(),
			retention:  8760 * time.Hour,
			resolution: 10 * time.Minute,
		},
		{
			address:    promLongRetention2.HTTPAddr(),
			retention:  8760 * time.Hour,
			resolution: 10 * time.Minute,
		},
	}})
	require.NoError(t, err)
	defer storageCloseFn()

	sendWrite := func(attr storagemetadata.Attributes) error {
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
		return promStorage.Write(context.TODO(), wq)
	}

	t.Run("send short retention write", func(t *testing.T) {
		reset()
		err := sendWrite(storagemetadata.Attributes{
			Retention:  120 * time.Hour,
			Resolution: 15 * time.Second,
		})
		require.NoError(t, err)
		assert.NotNil(t, promShortRetention.GetLastRequest())
		assert.Nil(t, promMediumRetention.GetLastRequest())
		assert.Nil(t, promLongRetention.GetLastRequest())
	})

	t.Run("send medium retention write", func(t *testing.T) {
		reset()
		err := sendWrite(storagemetadata.Attributes{
			Resolution: 5 * time.Minute,
			Retention:  720 * time.Hour,
		})
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastRequest())
		assert.NotNil(t, promMediumRetention.GetLastRequest())
		assert.Nil(t, promLongRetention.GetLastRequest())
	})

	t.Run("send long retention write", func(t *testing.T) {
		reset()
		err := sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 10 * time.Minute,
			Retention:  8760 * time.Hour,
		})
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastRequest())
		assert.Nil(t, promMediumRetention.GetLastRequest())
		assert.NotNil(t, promLongRetention.GetLastRequest())
	})

	t.Run("send write to multiple instances configured with same retention", func(t *testing.T) {
		reset()
		err := sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 10 * time.Minute,
			Retention:  8760 * time.Hour,
		})
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastRequest())
		assert.Nil(t, promMediumRetention.GetLastRequest())
		assert.NotNil(t, promLongRetention.GetLastRequest())
		assert.NotNil(t, promLongRetention2.GetLastRequest())
	})

	t.Run("send unconfigured retention write", func(t *testing.T) {
		reset()
		err := sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 5*time.Minute + 1,
			Retention:  720 * time.Hour,
		})
		require.NoError(t, err)
		err = sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 5 * time.Minute,
			Retention:  720*time.Hour + 1,
		})
		require.NoError(t, err)
		assert.Nil(t, promShortRetention.GetLastRequest())
		assert.Nil(t, promMediumRetention.GetLastRequest())
		assert.Nil(t, promLongRetention.GetLastRequest())
	})

	t.Run("error should not prevent sending to other instances", func(t *testing.T) {
		reset()
		promLongRetention.SetError(errors.New("test err"))
		err := sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 10 * time.Minute,
			Retention:  8760 * time.Hour,
		})
		require.Error(t, err)
		assert.Contains(t, err.Error(), "remote write endpoint returned non 200 response: 500, test err")
		assert.NotNil(t, promLongRetention2.GetLastRequest())
	})
}

func assertRemoteWriteHeadersSetCorrectly(t *testing.T, r *http.Request) {
	assert.Equal(t, r.Header.Get("content-encoding"), "snappy")
	assert.Equal(t, r.Header.Get("content-type"), "application/x-protobuf")
}
