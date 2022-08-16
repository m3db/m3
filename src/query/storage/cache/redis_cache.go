package cache

import (
	"context"
	"math/rand"
	"time"

	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	radix "github.com/mediocregopher/radix/v3"
	"github.com/uber-go/tally"

	"go.uber.org/zap"
)

/*
Redis cache implementation
Data is cached in one of two ways
1) For larger queries, larger than BucketSize, we split result from M3DB into buckets of size BucketSize
We then store the data in Redis via those buckets (i.e. we store data in 9:25-9:30, 9:30-9:35, etc.)
This is done for all but the last bucket, since this last bucket may not be completely filled in
For example, if the query end is 9:28, the last bucket is 9:25-9:30 and, at the time of evaluation, we
may not have seen 9:28-9:30 data, so we shouldn't set that bucket since that would be giving incomplete info

2) For smaller queries, less than BucketSize, we cannot split the results into buckets of size BucketSize.
However, we use the fact that queries asking for the same data (same labels, same duration) that come within
the same minute can use the same data. As such, for these queries, we first align the query to the minute
(or the closest multiple of 60s) so different requests at the same minute may become the same request.
By caching the whole result from one query, we can cut out some requests to M3DB.
 Note: this may mean results from m3coordinator for these smaller queries may not match up with the exact timestamp of evaluation
*/

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

	// Prefix used for keys that cache data for smaller queries coming in at the same time
	// This caches using the notion that queries coming in at around the same time
	//  are asking for the same thing
	// Small means <= BucketSize
	// WindowGetOrFetch() will use this key, see function for more details
	SimpleKeyPrefix = "same_window"
	// Prefix used for keys that cache data for larger queries over a larger period of time
	// This caches using a sliding window to be able to reuse previously queried data
	// Large means > BucketSize
	// BucketWindowGetOrFetch() will use this key, see function for more details
	BucketKeyPrefix = "sliding_window"

	AllowedMissingBuckets = 1
)

type RedisCacheSpec struct {
	// RedisAddress is the Redis address for caching.
	RedisCacheAddress string `yaml:"redisCacheAddress"`

	// The % of queries we check against the result using only M3DB
	CheckSampleRate float32 `yaml:"checkSampleRate"`

	// The % diff threshold to declare inequality
	ComparePercentThreshold float32 `yaml:"comparePercentThreshold"`
}

type RedisCache struct {
	client         radix.Client
	redisAddress   string
	redisCacheSpec *RedisCacheSpec
	logger         *zap.Logger
	cacheMetrics   CacheMetrics
}

// Struct for tracking stats for cache
type CacheMetrics struct {
	hitCounter        tally.Counter
	hitSamplesCounter tally.Counter
	hitBytesCounter   tally.Counter

	missCounter        tally.Counter
	missSamplesCounter tally.Counter
	missBytesCounter   tally.Counter

	redisTimer tally.Timer

	checkMismatchCounter tally.Counter
	checkTotalCounter    tally.Counter
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

		redisTimer: subScope.Timer("latency"),

		checkMismatchCounter: subScope.Counter("check-mismatches"),
		checkTotalCounter:    subScope.Counter("check-total"),
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
func NewRedisCache(redisCacheSpec *RedisCacheSpec, logger *zap.Logger, scope tally.Scope) *RedisCache {
	if redisCacheSpec == nil {
		return nil
	}
	redisAddress := redisCacheSpec.RedisCacheAddress
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
		client:         pool,
		redisAddress:   redisAddress,
		redisCacheSpec: redisCacheSpec,
		logger:         logger,
		cacheMetrics:   NewCacheMetrics(scope),
	}
}

// Return the number of entries are actually in Redis
func (cache *RedisCache) Check(entries []*storage.FetchQuery, prefix string) int {
	var count int
	var keys []string
	for _, e := range entries {
		keys = append(keys, KeyEncode(e, prefix))
	}
	cache.client.Do(radix.Cmd(&count, "EXISTS", keys...))
	return count
}

// Function to clear Redis cache, currently only used for testing
func (cache *RedisCache) FlushAll() {
	var response string
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
}

// For each entry in {entries}, gives the PromResult or nil if failed in array
// Array parameter used to handle multiple bucket requests in the future
func (cache *RedisCache) Get(entries []*storage.FetchQuery, prefix string) []*storage.PromResult {
	keys := make([]string, len(entries))
	results := make([]*storage.PromResult, len(entries))

	for i, b := range entries {
		keys[i] = KeyEncode(b, prefix)
	}

	// Consume as bytes to avoid having to casting to string later on (avoids duplicate memory)
	// var expire string
	responses := make([][]byte, len(entries))
	if err := cache.client.Do(radix.Cmd(&responses, "MGET", keys...)); err != nil {
		cache.logger.Error("Failed to execute Redis batch get", zap.Error(err))
		return results
	}
	// Go over all responses, and convert them from []byte back to PromResults
	// If they don't exist, then the cache wasn't able to get them
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
		entry := KeyEncode(entries[i], prefix)
		// Encode PromResult into string for Redis to store
		value, err := resultEncode(values[i])
		// cache.logger.Info("Prom result size", zap.Int("length", len(value)), zap.Int("ts_len", len(values[i].PromResult.Timeseries)))
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

	sw := cache.cacheMetrics.redisTimer.Start()
	defer sw.Stop()

	query_range := int64(q.End.Sub(q.Start).Seconds())
	// Align to the current minute
	// We align this query to help condense queries that ask for the same thing in the same minute
	// We assume they all want roughly similar responses
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
		promRes, err := st.FetchProm(ctx, align_q, fetchOptions)
		if err == nil {
			cache.Set([]*storage.FetchQuery{align_q}, []*storage.PromResult{&promRes}, SimpleKeyPrefix)
		}

		// cache.logger.Info("cache miss", zap.String("key", KeyEncode(align_q, SimpleKeyPrefix)), zap.Int("size", len(promRes.PromResult.Timeseries)))
		cache.cacheMetrics.CacheMetricsMiss(promRes)
		return promRes, err
	}
	// cache.logger.Info("cache hit", zap.String("key", KeyEncode(align_q, SimpleKeyPrefix)))
	cache.cacheMetrics.CacheMetricsHit(*res[0])
	CheckWithM3DB(ctx, st, fetchOptions, align_q, cache, res[0])
	return *res[0], nil
}

// Splits a result into buckets as defined by {buckets} and sets them in Redis
// Buckets are assumed to be in order + non-overlapping
func (cache *RedisCache) SetAsBuckets(result *storage.PromResult, buckets []*storage.FetchQuery) {
	results := make([]*storage.PromResult, len(buckets))
	for i := range buckets {
		results[i] = createEmptyPromResult()
	}
	for _, ts := range result.PromResult.Timeseries {
		idx := 0
		for i, b := range buckets {
			start := b.Start.UnixMilli()
			end := b.End.UnixMilli()
			prev := idx
			// Convert millisecond timestamp to second timestamp
			// Get all samples within the bucket
			for idx < len(ts.Samples) && ts.Samples[idx].Timestamp >= start && ts.Samples[idx].Timestamp < end {
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

// Function that checks whether the query data is in Redis using a sliding window
// If it is not, it gets the result from M3DB
//
// Currently, this is done by splitting data into buckets
// and checking if we have enough buckets that we can retrieve from Redis to recreate the result
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

	// WindowGetOrFetch already records timing, no need re-record it in this function
	sw := cache.cacheMetrics.redisTimer.Start()
	defer sw.Stop()

	last_start := q.End.Truncate(BucketSize)
	// We cache by breaking down data into buckets of size BucketSize
	// based on multiplies of BucketSize (in Unix time)
	// The last bucket we always get from M3DB to ensure that we do not store partial data
	// Since otherwise getting the last bucket and storing it could avoid storing new data

	// If we have more than one bucket, we want to get every bucket from the last from M3DB
	if q.Start.Before(last_start) {
		// Split into queries of the last bucket and everything else
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
		// cache.logger.Info("num_hit", zap.Int("check", count), zap.Int("length", len(buckets)))
		// We need to get from M3DB when we are missing more than total allowed missing buckets
		// Or when we have no buckets (in the case where we only want 1 bucket, and nothing is there)
		if count < len(buckets)-AllowedMissingBuckets || count == 0 {
			cache.logger.Info("cache miss", zap.String("key", KeyEncode(q, BucketKeyPrefix)))
			// Update query so it contains the expanded start of query
			// It's expanded so we can properly set the whole bucket
			// For example, a query from 9:22-9:32, needs to start at 9:20, so we can set the 9:20-9:25 bucket correctly
			expanded_query := &storage.FetchQuery{
				TagMatchers: q.TagMatchers,
				Start:       buckets[0].Start,
				End:         q.End,
				Interval:    q.Interval,
			}
			res, err := st.FetchProm(ctx, expanded_query, fetchOptions)
			cache.cacheMetrics.CacheMetricsMiss(res)
			if err == nil {
				// Don't set last bucket if it's within a minute of the end of query to ensure M3DB is filled out when we set
				if buckets[len(buckets)-1].End.Add(1 * time.Minute).After(q.End) {
					buckets = buckets[:len(buckets)-1]
				}
				cache.SetAsBuckets(&res, buckets)
			}
			return res, err
		}

		// Get the data for each bucket
		results := cache.Get(buckets, BucketKeyPrefix)
		cnt := 0
		// Check again after getting just in case some of the keys disappeared so we don't make many M3DB requests
		for i, r := range results {
			if r == nil {
				cnt++
				// In case we end up missing more than the allowed amount of missing buckets
				if cnt > AllowedMissingBuckets {
					cache.logger.Info(
						"cache miss", zap.String("key", KeyEncode(q, BucketKeyPrefix)),
						zap.Int64("start", buckets[i].Start.Unix()),
						zap.Int64("start", buckets[i].End.Unix()),
					)
					// Update query so it contains the expanded start of query
					// It's expanded so we can properly set the whole bucket
					// For example, a query from 9:22-9:32, needs to start at 9:20, so we can set the 9:20-9:25 bucket correctly
					expanded_query := &storage.FetchQuery{
						TagMatchers: q.TagMatchers,
						Start:       buckets[0].Start,
						End:         q.End,
						Interval:    q.Interval,
					}
					res, err := st.FetchProm(ctx, expanded_query, fetchOptions)
					cache.cacheMetrics.CacheMetricsMiss(res)
					if err == nil {
						// Don't set last bucket if it's within a minute of the end of query to ensure M3DB is filled out when we set
						if buckets[len(buckets)-1].End.Add(1 * time.Minute).After(q.End) {
							buckets = buckets[:len(buckets)-1]
						}
						cache.SetAsBuckets(&res, buckets)
					}
					return res, err
				}
			}
		}
		// We iterate again since in the above iteration, we just do an iteration to check if all the data is still there
		// Allows us to avoid making more than the acceptable number of M3DB requests
		for i, r := range results {
			if r == nil {
				res, err := st.FetchProm(ctx, buckets[i], fetchOptions)
				if err != nil {
					cache.logger.Error("M3DB Fetch Error of Bucket", zap.Error(err))
					return storage.PromResult{}, nil
				}
				cache.cacheMetrics.CacheMetricsMiss(res)
				results[i] = &res
				// Only set after a minute to ensure all data has properly loaded in M3DB for that bucket
				if buckets[i].End.Add(1 * time.Minute).Before(q.End) {
					cache.Set([]*storage.FetchQuery{buckets[i]}, []*storage.PromResult{results[i]}, BucketKeyPrefix)
				}
			}
			// If it's the first bucket, then we may not need all the data from it
			// For example, if we want 9:22-9:32, and we get bucket 9:20-9:25, we don't need the data 9:20-9:22
			// So we remove data prior to the start of the query
			if i == 0 {
				filterResult(results[i], q.Start.UnixMilli())
			}
		}
		cache.cacheMetrics.CacheMetricsBucketHit(results)

		// Get last bucket (don't set this one)
		res, err := st.FetchProm(ctx, last_query, fetchOptions)
		cache.cacheMetrics.CacheMetricsMiss(res)
		if err != nil {
			return storage.PromResult{}, nil
		}
		// Combine all the results together
		result := combineResult(append(results, &res))
		CheckWithM3DB(ctx, st, fetchOptions, q, cache, result)
		return *result, nil
	}
	res, err := st.FetchProm(ctx, q, fetchOptions)
	cache.cacheMetrics.CacheMetricsMiss(res)
	return res, err
}

func CheckWithM3DB(
	ctx context.Context,
	st storage.Storage,
	fetchOptions *storage.FetchOptions,
	q *storage.FetchQuery,
	cache *RedisCache,
	cacheResult *storage.PromResult,
) {
	if rand.Float32() < float32(cache.redisCacheSpec.CheckSampleRate) {
		m3dbResult, err := st.FetchProm(ctx, q, fetchOptions)
		if err == nil {
			// Compare up to 30s ago to account for M3DB potentially not having been fully updated
			equals := TimeseriesEqual(cacheResult.PromResult.Timeseries, m3dbResult.PromResult.Timeseries, q.End.Add(-30*time.Second).UnixMilli())
			if !equals {
				cache.logger.Info("Mismatch", zap.String("tags", q.TagMatchers.String()))
				cache.cacheMetrics.checkMismatchCounter.Inc(1)
			}
			cache.cacheMetrics.checkTotalCounter.Inc(1)
		}
	}
}
