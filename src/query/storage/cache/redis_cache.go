package cache

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/storage"
	radix "github.com/mediocregopher/radix/v3"
	"github.com/uber-go/tally"

	"go.uber.org/zap"
)

const (
	// Time that keys expire after (in seconds)
	ExpirationTime string = "300"
	// Blank result used to denote an empty PromResult
	EmptyResult string = "{}"
	// Minimum number of connections pools to Redis to keep open
	MinPools int = 10
	// Time to wait before creating a new Redis connection pool (in ms)
	CreateAfterTime time.Duration = 100 * time.Millisecond
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

// Update metrcis for a cache hit
func (cm CacheMetrics) CacheMetricsHit(result storage.PromResult) {
	cm.hitCounter.Inc(1)
	tot_samples := 0
	for _, ts := range result.PromResult.Timeseries {
		tot_samples += len(ts.Samples)
	}
	cm.hitSamplesCounter.Inc(int64(tot_samples))
	cm.hitBytesCounter.Inc(int64(result.PromResult.Size()))
}

// Update metrics for a cache miss
func (cm CacheMetrics) CacheMetricsMiss(result storage.PromResult) {
	cm.missCounter.Inc(1)
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
	cache := &RedisCache{
		client:       pool,
		redisAddress: redisAddress,
		logger:       logger,
		cacheMetrics: NewCacheMetrics(scope),
	}
	return cache
}

// Given a fetch query, converts it into a key for Redis
func keyEncode(query *storage.FetchQuery) string {
	res := make([]string, len(query.TagMatchers))
	for i, m := range query.TagMatchers {
		res[i] = m.String()
	}
	// Sort to guarantee we have the same order every time
	sort.Strings(res)

	label_key := strings.Join(res, ";")
	queryRange := int64(query.End.Sub(query.Start).Seconds())
	// Key becomes {labels}::{endTime}::{duration}
	// For example, a key might look like
	// "fieldA=\"1\";fieldB=\"2\"::1659459470::300"
	return fmt.Sprintf("%s::%d::%d", label_key, query.End.Unix(), queryRange)
}

// Given a result, encodes it into string (or "" if error)
// Encodes an empty PromResult to {} to differentiate from ""
//
// We only encode the QueryResult portion of the PromResult struct
// We omit the metadata since we do not use it in computation
// and also this metadata also isn't accurate since we aren't getting it from M3DB
func resultEncode(result *storage.PromResult) (string, error) {
	if len(result.PromResult.Timeseries) == 0 {
		return EmptyResult, nil
	}
	// Custom Marshal function for QueryResults
	res, err := result.PromResult.Marshal()
	if err != nil {
		return "", err
	}
	return string(res), nil
}

// Given a string, decodes it into a result (or nil if failed)
//
// Sets the metadata to a blank as we don't use it
// and we don't actually have metadata from M3DB for this result
func resultDecode(val []byte) (*storage.PromResult, error) {
	var result storage.PromResult
	result.PromResult = &prompb.QueryResult{}

	// Check length first so we don't convert to string every time
	if len(val) == len([]byte(EmptyResult)) && string(val) == EmptyResult {
		result.Metadata = block.NewResultMetadata()
		return &result, nil
	}
	// Custom Unmarshal function for QueryResults
	if err := result.PromResult.Unmarshal(val); err != nil {
		return nil, err
	}
	result.Metadata = block.NewResultMetadata()
	return &result, nil
}

func (cache *RedisCache) FlushAll() {
	var response string
	cache.client.Do(radix.Cmd(&response, "FLUSHALL"))
}

// For each entry in {entries}, gives the PromResult or nil if failed in array
// Array parameter used to handle multiple bucket requests in the future
func (cache *RedisCache) Get(entries []*storage.FetchQuery) []*storage.PromResult {
	keys := make([]string, len(entries))
	results := make([]*storage.PromResult, len(entries))

	for i, b := range entries {
		keys[i] = keyEncode(b)
	}

	// Consume as bytes to avoid having to casting to string later on (avoids duplicate memory)
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
) error {
	var commands []radix.CmdAction
	// Place holder variable to fit radix.Cmd() functions
	var response string

	for i := range entries {
		// Skip over non-existent results
		if entries[i] == nil {
			continue
		}
		entry := keyEncode(entries[i])
		value, err := resultEncode(values[i])
		cache.logger.Info("Prom result size", zap.Int("length", len(value)), zap.Int("ts_len", len(values[i].PromResult.Timeseries)))
		if err != nil {
			cache.logger.Error("Redis encode error", zap.Error(err))
			continue
		}

		commands = append(commands, radix.Cmd(&response, "SET", entry, value), radix.Cmd(&response, "EXPIRE", entry, ExpirationTime))
	}

	if err := cache.client.Do(radix.Pipeline(commands...)); err != nil {
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

	res := cache.Get([]*storage.FetchQuery{align_q})
	if res[0] == nil {
		promRes, err := st.FetchProm(ctx, q, fetchOptions)
		if err == nil {
			cache.Set([]*storage.FetchQuery{align_q}, []*storage.PromResult{&promRes})
		}

		cache.logger.Info("cache miss", zap.String("key", keyEncode(align_q)), zap.Int("size", len(promRes.PromResult.Timeseries)))
		cache.cacheMetrics.CacheMetricsMiss(promRes)
		return promRes, err
	}
	cache.logger.Info("cache hit", zap.String("key", keyEncode(align_q)))
	cache.cacheMetrics.CacheMetricsHit(*res[0])
	return *res[0], nil
}
