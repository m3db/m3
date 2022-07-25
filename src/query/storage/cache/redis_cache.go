package cache

import (
	"time"

	radix "github.com/mediocregopher/radix/v3"

	"go.uber.org/zap"
)

type RedisCache struct {
	client       radix.Client
	redisAddress string
	logger       *zap.Logger
}

const (
	// Minimum number of connections pools to Redis to keep open
	MinPools int = 10
	// Time to wait before creating a new Redis connection pool (in ms)
	CreateAfterTime time.Duration = 100 * time.Millisecond
)

// Create new RedisCache
// If redisAddress is "" or a connection can't be made, returns nil
func NewRedisCache(redisAddress string, logger *zap.Logger) *RedisCache {
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
	}
}
