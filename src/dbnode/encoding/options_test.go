package encoding

import (
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/pool"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestOptionsSettersAndGetters(t *testing.T) {
	opts := NewOptions()

	newTimeUnit := xtime.Minute
	newLRUSize := 10
	newM3TSZSize := 64
	newProtoSize := 256
	newMetrics := NewMetrics(instrument.NewOptions().MetricsScope())

	bytesPool := pool.NewCheckedBytesPool(
		[]pool.Bucket{{
			Capacity: 1024,
			Count:    10,
		}}, nil, func(s []pool.Bucket) pool.BytesPool {
			return pool.NewBytesPool(s, nil)
		})
	bytesPool.Init()

	opts2 := opts.
		SetDefaultTimeUnit(newTimeUnit).
		SetByteFieldDictionaryLRUSize(newLRUSize).
		SetIStreamReaderSizeM3TSZ(newM3TSZSize).
		SetIStreamReaderSizeProto(newProtoSize).
		SetMetrics(newMetrics).
		SetBytesPool(bytesPool)

	require.Equal(t, newTimeUnit, opts2.DefaultTimeUnit())
	require.Equal(t, newLRUSize, opts2.ByteFieldDictionaryLRUSize())
	require.Equal(t, newM3TSZSize, opts2.IStreamReaderSizeM3TSZ())
	require.Equal(t, newProtoSize, opts2.IStreamReaderSizeProto())
	require.Equal(t, newMetrics, opts2.Metrics())
	require.Equal(t, bytesPool, opts2.BytesPool())

	require.Equal(t, defaultDefaultTimeUnit, opts.DefaultTimeUnit())
	require.Equal(t, defaultByteFieldDictLRUSize, opts.ByteFieldDictionaryLRUSize())
}

func TestOptionsDefaults(t *testing.T) {
	opts := NewOptions()

	require.Equal(t, defaultDefaultTimeUnit, opts.DefaultTimeUnit())
	require.Equal(t, defaultByteFieldDictLRUSize, opts.ByteFieldDictionaryLRUSize())
	require.Equal(t, defaultIStreamReaderSizeM3TSZ, opts.IStreamReaderSizeM3TSZ())
	require.Equal(t, defaultIStreamReaderSizeProto, opts.IStreamReaderSizeProto())
	require.NotNil(t, opts.TimeEncodingSchemes())
	require.Equal(t, defaultMarkerEncodingScheme, opts.MarkerEncodingScheme())
	require.NotNil(t, opts.Metrics())
}
