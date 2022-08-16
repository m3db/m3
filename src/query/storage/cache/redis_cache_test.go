// Simple unit tests for Redis cache
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
	"github.com/uber-go/tally"
	"go.uber.org/zap"
)

var (
	cache RedisCache
)

func TestMain(m *testing.M) {
	cache = *NewRedisCache(&RedisCacheSpec{RedisCacheAddress: "127.0.0.1:6379"}, zap.NewExample(), tally.NewTestScope("", make(map[string]string, 0)))
	var response string
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
	m.Run()
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
}

func resultEqual(a, b *storage.PromResult) bool {
	return (a.Metadata.Equals(b.Metadata) && TimeseriesEqual(a.PromResult.Timeseries, b.PromResult.Timeseries, 0))
}

func queryEqual(a, b *storage.FetchQuery) bool {
	return (a.Start.Unix() == b.Start.Unix()) && (a.End.Unix() == b.End.Unix())
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

	err := cache.Set([]*storage.FetchQuery{&query}, []*storage.PromResult{&result}, SimpleKeyPrefix)
	require.Nil(t, err, fmt.Sprintf("SET failure: Got %s instead of OK as response", err))

	// Test if right values
	res := cache.Get([]*storage.FetchQuery{&query}, SimpleKeyPrefix)
	require.NotNil(t, res[0], "Error in decoding PromResult, result is not nil")
	require.True(t, resultEqual(res[0], &result), "Results did not equal expected")

	// Test for correct empty
	query.End = time.Unix(7200, 0)
	res = cache.Get([]*storage.FetchQuery{&query}, SimpleKeyPrefix)
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

	err := cache.Set([]*storage.FetchQuery{&query1, &query2}, []*storage.PromResult{&result1, &result2}, BucketKeyPrefix)
	require.Nil(t, err, fmt.Sprintf("SET failure: Got %s instead of OK as response", err))

	// Test if right values
	res := cache.Get([]*storage.FetchQuery{&query1, &query2}, BucketKeyPrefix)
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
	res = cache.Get([]*storage.FetchQuery{&query3, &query4, &query5}, BucketKeyPrefix)
	require.Nil(t, res[0], "Result should've been nil for empty key")
	require.Nil(t, res[1], "Result should've been nil for empty key")
	require.Nil(t, res[2], "Result should've been nil for empty key")
}

func TestCheck(t *testing.T) {
	tags := []models.Matcher{
		{Type: models.MatchEqual, Name: []byte("fieldA"), Value: []byte("1")},
	}

	var queries []*storage.FetchQuery
	var results []*storage.PromResult
	tm := 0
	i := 0
	for tm < 100 {
		queries = append(queries, &storage.FetchQuery{
			Start:       time.Unix(int64(tm), 0),
			End:         time.Unix(int64(tm+10), 0),
			TagMatchers: tags,
			Interval:    0,
		})
		results = append(results, createEmptyPromResult())
		i += 1
		tm += 10
	}

	sets := []*storage.FetchQuery{
		queries[0], queries[1], queries[2],
		queries[7], queries[8], queries[9],
	}

	cache.Set(sets, results[:6], BucketKeyPrefix)

	count := cache.Check(queries[:7], BucketKeyPrefix)
	require.Equal(t, count, 3, "Check didn't match")

	count = cache.Check(queries[4:6], BucketKeyPrefix)
	require.Equal(t, count, 0, "Check didn't match")

	count = cache.Check(queries[5:9], BucketKeyPrefix)
	require.Equal(t, count, 2, "Check didn't match")

	count = cache.Check(queries[:10], BucketKeyPrefix)
	require.Equal(t, count, 6, "Check didn't match")
}

func TestSplit(t *testing.T) {
	tags := []models.Matcher{
		{Type: models.MatchEqual, Name: []byte("fieldA"), Value: []byte("1")},
	}

	start := int64(150)
	end := int64(1050)
	bucket := int64(BucketSize.Seconds())

	query := storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(int64(start), 0),
		End:         time.Unix(int64(end), 0),
		Interval:    0,
	}

	var expected []*storage.FetchQuery
	tm := int64(0)
	for tm < end {
		cur_end := int64((int64(tm) + bucket) / bucket * bucket)
		if cur_end > end {
			cur_end = end
		}
		expected = append(expected, &storage.FetchQuery{
			TagMatchers: tags,
			Start:       time.Unix(int64(tm), 0),
			End:         time.Unix(int64(cur_end), 0),
			Interval:    0,
		})
		tm += bucket
	}

	splits := splitQueryToBuckets(&query, BucketSize)
	require.Equal(t, len(splits), 4, "Didn't split into the expected number of buckets")

	for i := range splits {
		require.True(t, queryEqual(splits[i], expected[i]), "Split queries didn't equal expected")
	}
}

func TestFilter(t *testing.T) {
	result := createEmptyPromResult()
	result.PromResult.Timeseries = make([]*prompb.TimeSeries, 3)

	result.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 100000},
		{Value: 1.5, Timestamp: 110000},
		{Value: 2, Timestamp: 250000},
	}

	result.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
	}

	result.PromResult.Timeseries[1] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[1].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 150000},
		{Value: 1.5, Timestamp: 210000},
		{Value: 2, Timestamp: 250000},
	}

	result.PromResult.Timeseries[2] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[2].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 100000},
		{Value: 1.5, Timestamp: 210000},
		{Value: 2, Timestamp: 250000},
	}

	result.PromResult.Timeseries[1].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
		{Name: []byte("fieldC"), Value: []byte("5")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	expected := createEmptyPromResult()
	expected.PromResult.Timeseries = make([]*prompb.TimeSeries, 3)

	expected.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	expected.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 2, Timestamp: 250000},
	}

	expected.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
	}

	expected.PromResult.Timeseries[1] = &prompb.TimeSeries{}
	expected.PromResult.Timeseries[1].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 150000},
		{Value: 1.5, Timestamp: 210000},
		{Value: 2, Timestamp: 250000},
	}

	expected.PromResult.Timeseries[2] = &prompb.TimeSeries{}
	expected.PromResult.Timeseries[2].Samples = []prompb.Sample{
		{Value: 1.5, Timestamp: 210000},
		{Value: 2, Timestamp: 250000},
	}

	expected.PromResult.Timeseries[1].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
		{Name: []byte("fieldC"), Value: []byte("5")},
		{Name: []byte("__name__"), Value: []byte("3")},
	}

	filterResult(result, 130*1000)
	require.True(t, resultEqual(expected, result), "Filtered result not equal")
}

func TestSplitAndCombine(t *testing.T) {
	result := createEmptyPromResult()
	result.PromResult.Timeseries = make([]*prompb.TimeSeries, 2)

	result.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 110000},
		{Value: 1.5, Timestamp: 120001},
		{Value: 1.5, Timestamp: 149997},
		{Value: 1.5, Timestamp: 149998},
		{Value: 1.5, Timestamp: 149999},
		{Value: 2, Timestamp: 470000},
	}

	result.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
	}

	result.PromResult.Timeseries[1] = &prompb.TimeSeries{}
	result.PromResult.Timeseries[1].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 150000},
		{Value: 1.5, Timestamp: 170000},
		{Value: 2, Timestamp: 250000},
	}

	result.PromResult.Timeseries[1].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
	}

	expected := createEmptyPromResult()
	expected.PromResult.Timeseries = make([]*prompb.TimeSeries, 2)

	expected.PromResult.Timeseries[0] = &prompb.TimeSeries{}
	expected.PromResult.Timeseries[0].Samples = []prompb.Sample{
		{Value: 1.5, Timestamp: 120001},
		{Value: 1.5, Timestamp: 149997},
		{Value: 1.5, Timestamp: 149998},
		{Value: 1.5, Timestamp: 149999},
		{Value: 2, Timestamp: 470000},
	}

	expected.PromResult.Timeseries[0].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
	}

	expected.PromResult.Timeseries[1] = &prompb.TimeSeries{}
	expected.PromResult.Timeseries[1].Samples = []prompb.Sample{
		{Value: 1, Timestamp: 150000},
		{Value: 1.5, Timestamp: 170000},
		{Value: 2, Timestamp: 250000},
	}

	expected.PromResult.Timeseries[1].Labels = []prompb.Label{
		{Name: []byte("fieldA"), Value: []byte("{")},
		{Name: []byte("fieldB"), Value: []byte("}")},
	}

	tags := []models.Matcher{
		{Type: models.MatchEqual, Name: []byte("fieldA"), Value: []byte("{")},
	}

	query := &storage.FetchQuery{
		TagMatchers: tags,
		Start:       time.Unix(120, 0),
		End:         time.Unix(750, 0),
		Interval:    0,
	}

	buckets := splitQueryToBuckets(query, BucketSize)
	cache.SetAsBuckets(result, buckets)

	res := combineResult(cache.Get(buckets, BucketKeyPrefix))
	filterResult(res, query.Start.UnixMilli())
	require.True(t, resultEqual(expected, res), "Filtered result not equal")
}
