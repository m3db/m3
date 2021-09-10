package promremotewrite

import (
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/stretchr/testify/assert"
)

func TestNewFromConfiguration(t *testing.T) {
	opts := NewFromConfiguration(config.PrometheusRemoteWriteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteWriteBackendEndpointConfiguration{{
			Address:    "testAddress",
			Resolution: time.Second,
			Retention:  time.Millisecond,
			Type:       storagemetadata.AggregatedMetricsType,
		}},
		RequestTimeout:  time.Nanosecond,
		ConnectTimeout:  time.Microsecond,
		KeepAlive:       time.Millisecond,
		IdleConnTimeout: time.Second,
		MaxIdleConns:    1,
	})

	assert.Equal(t, opts, Options{
		endpoints: []EndpointOptions{{
			address: "testAddress",
			storageMetadata: storagemetadata.Attributes{
				MetricsType: storagemetadata.AggregatedMetricsType,
				Resolution:  time.Second,
				Retention:   time.Millisecond,
			},
		}},
		requestTimeout:  time.Nanosecond,
		connectTimeout:  time.Microsecond,
		keepAlive:       time.Millisecond,
		idleConnTimeout: time.Second,
		maxIdleConns:    1,
	})
}

func TestUnaggregatedRetentionAndReslutionIgnored(t *testing.T) {
	opts := NewFromConfiguration(config.PrometheusRemoteWriteBackendConfiguration{
		Endpoints: []config.PrometheusRemoteWriteBackendEndpointConfiguration{{
			Resolution: time.Second,
			Retention:  time.Millisecond,
			Type:       storagemetadata.UnaggregatedMetricsType,
		}},
	})

	assert.Equal(t, opts.endpoints[0].storageMetadata,
		storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
			Resolution:  0,
			Retention:   0,
		})
}
