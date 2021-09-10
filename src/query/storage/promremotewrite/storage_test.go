package promremotewrite

import (
	"context"
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
			appender, closeFn, err := NewStorage(Options{endpoints: []EndpointOptions{{address: fakeProm.HTTPAddr()}}})
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
			err = appender.Write(context.TODO(), wq)
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

func TestMetricTypeSupport(t *testing.T) {
	promUnagg, closeFn1 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn1()
	prom120x1, closeFn2 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn2()
	prom720x5, closeFn3 := promremotewritetest.NewFakePromRemoteWriteServer(t)
	defer closeFn3()
	reset := func() {
		promUnagg.Reset()
		prom120x1.Reset()
		prom720x5.Reset()
	}

	appender, storageCloseFn, err := NewStorage(Options{endpoints: []EndpointOptions{
		{
			address: promUnagg.HTTPAddr(),
			storageMetadata: storagemetadata.Attributes{
				MetricsType: storagemetadata.UnaggregatedMetricsType,
			},
		},
		{
			address: prom120x1.HTTPAddr(),
			storageMetadata: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   120 * time.Hour,
				Resolution:  1 * time.Minute,
			},
		},
		{
			address: prom720x5.HTTPAddr(),
			storageMetadata: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Retention:   720 * time.Hour,
				Resolution:  5 * time.Minute,
			},
		},
	}})
	require.NoError(t, err)
	defer storageCloseFn()

	sendWrite := func(attr storagemetadata.Attributes) {
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
		err = appender.Write(context.TODO(), wq)
		require.NoError(t, err)
	}

	t.Run("send unnagregated write", func(t *testing.T) {
		reset()
		sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: time.Second,
			Retention:  time.Hour,
		})
		assert.Nil(t, prom120x1.GetLastRequest())
		assert.Nil(t, prom720x5.GetLastRequest())
		assert.NotNil(t, promUnagg.GetLastRequest())
	})

	t.Run("send aggregated write for shortest retention", func(t *testing.T) {
		reset()
		sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: time.Minute,
			Retention:  120 * time.Hour,
		})
		assert.NotNil(t, prom120x1.GetLastRequest())
		assert.Nil(t, prom720x5.GetLastRequest())
		assert.NotNil(t, promUnagg.GetLastRequest())
	})

	t.Run("send aggregated write for longest retention", func(t *testing.T) {
		reset()
		sendWrite(storagemetadata.Attributes{
			MetricsType: storagemetadata.AggregatedMetricsType,
			// Should be ignored when type is unagg
			Resolution: 5 * time.Minute,
			Retention:  720 * time.Hour,
		})
		assert.Nil(t, prom120x1.GetLastRequest())
		assert.NotNil(t, prom720x5.GetLastRequest())
		assert.NotNil(t, promUnagg.GetLastRequest())
	})
}

func assertRemoteWriteHeadersSetCorrectly(t *testing.T, r *http.Request) {
	assert.Equal(t, r.Header.Get("content-encoding"), "snappy")
	assert.Equal(t, r.Header.Get("content-type"), "application/x-protobuf")
}
