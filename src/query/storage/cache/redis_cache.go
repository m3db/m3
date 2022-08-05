package cache

import (
	"context"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	radix "github.com/mediocregopher/radix/v3"
	"github.com/uber-go/tally"

	"go.uber.org/zap"
)

const (
	// Time that keys expire after (in seconds)
	ExpirationTime string = "1200"
	// Blank result used to denote an empty PromResult
	EmptyResult string = "{}"
	// Minimum number of connections pools to Redis to keep open
	MinPools int = 10
	// Time to wait before creating a new Redis connection pool (in ms)
	CreateAfterTime time.Duration = 100 * time.Millisecond
	// Bucket size (in s) to split window up into
	BucketSize time.Duration = 300 * time.Second

	SimpleKeyPrefix = "simple"
	BucketKeyPrefix = "bucket"
)

type RedisCache struct {
	client       radix.Client
	redisAddress string
	logger       *zap.Logger
	cacheMetrics CacheMetrics
}

// Struct for tracking stats for cache
type CacheMetrics struct {
	hitCounter        tally.Counter
	hitSamplesCounter tally.Counter
	hitBytesCounter   tally.Counter

	missCounter        tally.Counter
	missSamplesCounter tally.Counter
	missBytesCounter   tally.Counter
}

func NewCacheMetrics(scope tally.Scope) CacheMetrics {
	subScope := scope.SubScope("cache")
	return CacheMetrics{
		hitCounter:        subScope.Counter("hit"),
		hitSamplesCounter: subScope.Counter("hit-samples"),
		hitBytesCounter:   subScope.Counter("hit-bytes"),

		missCounter:        subScope.Counter("miss"),
		missSamplesCounter: subScope.Counter("miss-samples"),
		missBytesCounter:   subScope.Counter("miss-bytes"),
	}
}

// Update metrics for a cache hit
func (cm CacheMetrics) CacheMetricsHit(result storage.PromResult) {
	// cm.hitCounter.Inc(1)
	tot_samples := 0
	for _, ts := range result.PromResult.Timeseries {
		tot_samples += len(ts.Samples)
	}
	cm.hitSamplesCounter.Inc(int64(tot_samples))
	cm.hitBytesCounter.Inc(int64(result.PromResult.Size()))
}

// Update metrics for a hit of buckets
func (cm CacheMetrics) CacheMetricsBucketHit(results []*storage.PromResult) {
	// cm.hitCounter.Inc(1)
	tot_samples := 0
	tot_size := 0
	for _, result := range results {
		for _, ts := range result.PromResult.Timeseries {
			tot_samples += len(ts.Samples)
		}
		tot_size += result.PromResult.Size()
	}
	cm.hitSamplesCounter.Inc(int64(tot_samples))
	cm.hitBytesCounter.Inc(int64(tot_size))
}

// Update metrics for a cache miss
func (cm CacheMetrics) CacheMetricsMiss(result storage.PromResult) {
	// cm.missCounter.Inc(1)
	tot_samples := 0
	for _, ts := range result.PromResult.Timeseries {
		tot_samples += len(ts.Samples)
	}
	cm.missSamplesCounter.Inc(int64(tot_samples))
	cm.missBytesCounter.Inc(int64(result.PromResult.Size()))
}

// Create new RedisCache
// If redisAddress is "" or a connection can't be made, returns nil
func NewRedisCache(redisAddress string, logger *zap.Logger, scope tally.Scope) *RedisCache {
	logger.Info("New Cache", zap.String("address", redisAddress))
	if redisAddress == "" {
		logger.Info("Not using cache since address is empty")
		return nil
	}
	pool, err := radix.NewPool("tcp", redisAddress, MinPools, radix.PoolOnEmptyCreateAfter(CreateAfterTime))
	if err != nil {
		logger.Error("Failed to connect to Redis", zap.String("address", redisAddress))
		return nil
	}
	logger.Info("Connection to Redis established", zap.String("address", redisAddress))
	return &RedisCache{
		client:       pool,
		redisAddress: redisAddress,
		logger:       logger,
		cacheMetrics: NewCacheMetrics(scope),
	}
}

// Return the number of entries are actually in Redis
func (cache *RedisCache) Check(entries []*storage.FetchQuery, prefix string) int {
	var count int
	var keys []string
	for _, e := range entries {
		keys = append(keys, keyEncode(e, prefix))
	}
	cache.client.Do(radix.Cmd(&count, "EXISTS", keys...))
	return count
}

// For each entry in {entries}, gives the PromResult or nil if failed in array
// Array parameter used to handle multiple bucket requests in the future
func (cache *RedisCache) Get(entries []*storage.FetchQuery, prefix string) []*storage.PromResult {
	keys := make([]string, len(entries))
	results := make([]*storage.PromResult, len(entries))

	for i, b := range entries {
		keys[i] = keyEncode(b, prefix)
	}

	// Consume as bytes to avoid having to casting to string later on (avoids duplicate memory)
	// var expire string
	responses := make([][]byte, len(entries))
	if err := cache.client.Do(radix.Cmd(&responses, "MGET", keys...)); err != nil {
		cache.logger.Error("Failed to execute Redis batch get", zap.Error(err))
		return results
	}
	for i, r := range responses {
		// If the response is nil (not EmptyResult), then it doesn't exist
		if len(r) == 0 {
			results[i] = nil
			cache.logger.Info("cache didn't get", zap.String("key", keys[i]))
			continue
		}
		// Otherwise we did find it
		res, err := resultDecode(r)
		if err != nil {
			cache.logger.Error("Redis decode error", zap.Error(err))
		}
		results[i] = res
	}
	return results
}

// For each entry in {entries} and associated value in {values},
// sets entry -> value mapping in Redis
// Array parameter used to handle multiple bucket requests in the future
func (cache *RedisCache) Set(
	entries []*storage.FetchQuery,
	values []*storage.PromResult,
	prefix string,
) error {
	var args []string
	// Place holder variable to fit radix.Cmd() functions
	var response string

	for i := range entries {
		entry := keyEncode(entries[i], prefix)
		value, err := resultEncode(values[i])
		cache.logger.Info("Prom result size", zap.Int("length", len(value)), zap.Int("ts_len", len(values[i].PromResult.Timeseries)))
		if err != nil {
			cache.logger.Error("Redis encode error", zap.Error(err))
			continue
		}

		args = append(args, entry, value)
		// commands = append(commands, radix.Cmd(&response, "SET", entry, value), radix.Cmd(&response, "EXPIRE", entry, ExpirationTime))
	}

	// Use MSET and don't set expiration, let Redis LRU determine the process
	if err := cache.client.Do(radix.Cmd(&response, "MSET", args...)); err != nil {
		cache.logger.Error("Failed to execute Redis set", zap.Error(err))
		return err
	}

	return nil
}

// Function that checks whether the query data is in Redis, and returns that result if present
// If it is not, it gets the result from M3DB
//
// Currently, this check is done by aligning queries to a minute so that queries asking for the
// same timeseries over the same duration in the same minute are given the same result
// (i.e. this is the key for Redis)
func WindowGetOrFetch(
	ctx context.Context,
	st storage.Storage,
	fetchOptions *storage.FetchOptions,
	q *storage.FetchQuery,
	cache *RedisCache,
) (storage.PromResult, error) {
	if cache == nil || !EnableCache {
		return st.FetchProm(ctx, q, fetchOptions)
	}
	query_range := int64(q.End.Sub(q.Start).Seconds())
	// Align to the current minute
	align_end := int64(q.End.Unix()/60) * 60
	// Set start to aligned end minus the query range
	align_start := align_end - query_range

	align_q := &storage.FetchQuery{
		TagMatchers: q.TagMatchers,
		Start:       time.Unix(align_start, 0),
		End:         time.Unix(align_end, 0),
	}

	res := cache.Get([]*storage.FetchQuery{align_q}, SimpleKeyPrefix)
	if res[0] == nil {
		promRes, err := st.FetchProm(ctx, q, fetchOptions)
		if err == nil {
			cache.Set([]*storage.FetchQuery{align_q}, []*storage.PromResult{&promRes}, SimpleKeyPrefix)
		}

		cache.logger.Info("cache miss", zap.String("key", keyEncode(align_q, SimpleKeyPrefix)), zap.Int("size", len(promRes.PromResult.Timeseries)))
		cache.cacheMetrics.CacheMetricsMiss(promRes)
		return promRes, err
	}
	cache.logger.Info("cache hit", zap.String("key", keyEncode(align_q, SimpleKeyPrefix)))
	cache.cacheMetrics.CacheMetricsHit(*res[0])
	return *res[0], nil
}

func (cache *RedisCache) SetAsBuckets(result *storage.PromResult, buckets []*storage.FetchQuery) {
	results := make([]*storage.PromResult, len(buckets))
	for i := range buckets {
		results[i] = createEmptyPromResult()
	}
	for _, ts := range result.PromResult.Timeseries {
		idx := 0
		for i, b := range buckets {
			start := b.Start.Unix()
			end := b.End.Unix()
			prev := idx
			// Convert millisecond timestamp to second timestamp
			for idx < len(ts.Samples) && ts.Samples[idx].Timestamp/1000 >= start && ts.Samples[idx].Timestamp/1000 < end {
				idx += 1
			}
			// Nothing to add
			if prev == idx {
				continue
			}
			results[i].PromResult.Timeseries = append(results[i].PromResult.Timeseries,
				&prompb.TimeSeries{
					Labels:  ts.Labels,
					Samples: ts.Samples[prev:idx],
				},
			)
			// No need to continue
			if idx == len(ts.Samples) {
				break
			}
		}
	}
	cache.Set(buckets, results, BucketKeyPrefix)
}

func BucketWindowGetOrFetch(
	ctx context.Context,
	st storage.Storage,
	fetchOptions *storage.FetchOptions,
	q *storage.FetchQuery,
	cache *RedisCache,
) (storage.PromResult, error) {
	if cache == nil || !EnableCache {
		return st.FetchProm(ctx, q, fetchOptions)
	}
	queryRange := int64(q.End.Sub(q.Start).Seconds())
	// If size is <= bucket size, use simple caching
	if queryRange <= int64(BucketSize.Seconds()) {
		return WindowGetOrFetch(ctx, st, fetchOptions, q, cache)
	}

	last_start := q.End.Truncate(BucketSize)
	// If we have more than one bucket
	if q.Start.Before(last_start) {
		// Split into not-last and last
		exclude_last_query := &storage.FetchQuery{
			TagMatchers: q.TagMatchers,
			Start:       q.Start,
			End:         last_start,
			Interval:    q.Interval,
		}
		last_query := &storage.FetchQuery{
			TagMatchers: q.TagMatchers,
			Start:       last_start,
			End:         q.End,
			Interval:    q.Interval,
		}
		buckets := splitQueryToBuckets(exclude_last_query, BucketSize)
		// Check if we have to make a request to M3DB, in which case we just ask for all
		// This is so we don't have to convert if we need to ask M3DB for all of it anyways
		count := cache.Check(buckets, BucketKeyPrefix)
		cache.logger.Info("num_hit", zap.Int("check", count), zap.Int("length", len(buckets)))
		if count < len(buckets)-1 || count == 0 {
			cache.logger.Info("cache miss", zap.String("key", keyEncode(q, BucketKeyPrefix)))
			res, err := st.FetchProm(ctx, q, fetchOptions)
			cache.cacheMetrics.CacheMetricsMiss(res)
			if err == nil {
				cache.SetAsBuckets(&res, buckets)
			}
			return res, err
		}

		results := cache.Get(buckets, BucketKeyPrefix)
		cnt := 0
		// Check again after getting just in case some of the keys disappeared so we don't make many M3DB requests
		for _, r := range results {
			if r == nil {
				cnt++
				if cnt > 1 {
					cache.logger.Info("cache miss", zap.String("key", keyEncode(q, BucketKeyPrefix)))
					res, err := st.FetchProm(ctx, q, fetchOptions)
					cache.cacheMetrics.CacheMetricsMiss(res)
					if err == nil {
						cache.SetAsBuckets(&res, buckets)
					}
					return res, err
				}
			}
		}
		for i, r := range results {
			if r == nil {
				res, err := st.FetchProm(ctx, buckets[i], fetchOptions)
				if err != nil {
					cache.logger.Error("M3DB Fetch Error of Bucket", zap.Error(err))
					return storage.PromResult{}, nil
				}
				cache.cacheMetrics.CacheMetricsMiss(res)
				results[i] = &res
				cache.Set([]*storage.FetchQuery{buckets[i]}, []*storage.PromResult{results[i]}, BucketKeyPrefix)
			}
			// If it's the first bucket, we need to filter it to the actual start of query
			if i == 0 {
				filterResult(results[i], q.Start.Unix())
			}
		}
		cache.cacheMetrics.CacheMetricsBucketHit(results)

		// Get last bucket (don't set this one)
		res, err := st.FetchProm(ctx, last_query, fetchOptions)
		cache.cacheMetrics.CacheMetricsMiss(res)
		if err != nil {
			return storage.PromResult{}, nil
		}
		return *combineResult(append(results, &res)), nil
	}
	res, err := st.FetchProm(ctx, q, fetchOptions)
	cache.cacheMetrics.CacheMetricsMiss(res)
	return res, err
}
