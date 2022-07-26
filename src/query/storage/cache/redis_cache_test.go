// Basic unit tests for Redis cache (not comprehensive, just a quick sanity check)
// This test suite requires setting up a Redis instance at
// 127.0.0.1:6379 (localhost)

package cache

import (
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	radix "github.com/mediocregopher/radix/v3"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"
)

var (
	cache RedisCache
)

func TestMain(m *testing.M) {
	cache = *NewRedisCache("127.0.0.1:6379", zap.NewExample())
	var response string
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
	m.Run()
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
}

func labelsEqual(a, b []prompb.Label) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		val := (string(a[i].Name) == string(b[i].Name) &&
			string(a[i].Value) == string(b[i].Value))
		if !val {
			return false
		}
	}
	return true
}

func samplesEqual(a, b []prompb.Sample) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		val := (a[i].Timestamp == b[i].Timestamp &&
			a[i].Value == b[i].Value)
		if !val {
			return false
		}
	}
	return true
}

func timeseriesEqual(a, b []*prompb.TimeSeries) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		val := (labelsEqual(a[i].Labels, b[i].Labels) &&
			samplesEqual(a[i].Samples, b[i].Samples))
		if !val {
			return false
		}
	}
	return true
}

func resultEqual(a, b *storage.PromResult) bool {
	return (a.Metadata.Equals(b.Metadata) && timeseriesEqual(a.PromResult.Timeseries, b.PromResult.Timeseries))
}

func TestSingle(t *testing.T) {
	tags := []models.Matcher{
		{Type: models.MatchEqual, Name: []byte("fieldA"), Value: []byte("{")},
		{Type: models.MatchEqual, Name: []byte("fieldB"), Value: []byte("}")},
		{Type: models.MatchEqual, Name: []byte("__name__"), Value: []byte("3")},
	}

	query := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(0, 0),
		End:         time.Unix(3600, 0),
		Interval:    0,
	}

	result := storage.PromResult{}
	result.Metadata = block.NewResultMetadata()
	result.PromResult = &prompb.QueryResult{}
	result.PromResult.Timeseries = make([]*prompb.TimeSeries, 2)

	result.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 1},
		{Value: 1.5, Timestamp: 2},
		{Value: 2, Timestamp: 3},
	}

	result.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
		{Name: []byte("fieldC"), Value: []byte("4")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	result.PromResult.Timeseries[1] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[1].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 2},
		{Value: 1.5, Timestamp: 3},
		{Value: 2, Timestamp: 4},
	}

	result.PromResult.Timeseries[1].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
		{Name: []byte("fieldC"), Value: []byte("5")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	err := cache.Set([]*storage.FetchQuery{&query}, []*storage.PromResult{&result})
	require.Nil(t, err, fmt.Sprintf("SET failure: Got %s instead of OK as response", err))

	// Test if right values
	res := cache.Get([]*storage.FetchQuery{&query})
	require.NotNil(t, res[0], "Error in decoding PromResult, result is not nil")
	require.True(t, resultEqual(res[0], &result), "Results did not equal expected")

	// Test for correct empty
	query.End = time.Unix(7200, 0)
	res = cache.Get([]*storage.FetchQuery{&query})
	require.Nil(t, res[0], "Result should've been nil for empty key")
}

// Test for multiple buckets
func TestMulti(t *testing.T) {
	tags := []models.Matcher{
		{Type: models.MatchEqual, Name: []byte("fieldA"), Value: []byte("1")},
		{Type: models.MatchEqual, Name: []byte("fieldB"), Value: []byte("2")},
		{Type: models.MatchEqual, Name: []byte("__name__"), Value: []byte("3")},
	}

	query1 := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(0, 0),
		End:         time.Unix(3600, 0),
		Interval:    0,
	}

	query2 := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(3600, 0),
		End:         time.Unix(7200, 0),
		Interval:    0,
	}

	result1 := storage.PromResult{}
	result1.Metadata = block.NewResultMetadata()
	result1.PromResult = &prompb.QueryResult{}
	result1.PromResult.Timeseries = make([]*prompb.TimeSeries, 1)

	result1.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	result1.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 1},
		{Value: 1.5, Timestamp: 2},
		{Value: 2, Timestamp: 3},
	}

	result1.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("1")},
		{Name: []byte("fieldB"), Value: []byte("2")},
		{Name: []byte("fieldC"), Value: []byte("4")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	result2 := storage.PromResult{}
	result2.Metadata = block.NewResultMetadata()
	result2.PromResult = &prompb.QueryResult{}
	result2.PromResult.Timeseries = make([]*prompb.TimeSeries, 1)

	result2.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	result2.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 2, Timestamp: 1},
		{Value: 2.5, Timestamp: 2},
		{Value: 3, Timestamp: 3},
	}

	result2.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("1")},
		{Name: []byte("fieldB"), Value: []byte("2")},
		{Name: []byte("fieldC"), Value: []byte("4")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	err := cache.Set([]*storage.FetchQuery{&query1, &query2}, []*storage.PromResult{&result1, &result2})
	require.Nil(t, err, fmt.Sprintf("SET failure: Got %s instead of OK as response", err))

	// Test if right values
	res := cache.Get([]*storage.FetchQuery{&query1, &query2})
	require.NotNil(t, res[0], "Error in decoding PromResult, result is not nil")
	require.True(t, resultEqual(res[0], &result1), "Results did not equal expected")
	require.NotNil(t, res[1], "Error in decoding PromResult, result is not nil")
	require.True(t, resultEqual(res[1], &result2), "Results did not equal expected")

	// Test empty is correct
	query3 := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(0, 0),
		End:         time.Unix(7200, 0),
		Interval:    0,
	}
	query4 := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(0, 0),
		End:         time.Unix(1800, 0),
		Interval:    0,
	}
	query5 := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(1800, 0),
		End:         time.Unix(5400, 0),
		Interval:    0,
	}
	res = cache.Get([]*storage.FetchQuery{&query3, &query4, &query5})
	require.Nil(t, res[0], "Result should've been nil for empty key")
	require.Nil(t, res[1], "Result should've been nil for empty key")
	require.Nil(t, res[2], "Result should've been nil for empty key")
}
