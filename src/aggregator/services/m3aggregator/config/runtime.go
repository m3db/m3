// Copyright (c) 2017 Uber Technologies, Inc.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

package config

import (
	"math"
	"time"

	"github.com/m3db/m3aggregator/aggregator"
	"github.com/m3db/m3aggregator/runtime"
	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	kvutil "github.com/m3db/m3cluster/kv/util"
	"github.com/m3db/m3x/log"
)

// RuntimeOptionsConfiguration configures runtime options.
type RuntimeOptionsConfiguration struct {
	KVConfig                               kv.Configuration `yaml:"kvConfig"`
	WriteValuesPerMetricLimitPerSecondKey  string           `yaml:"writeValuesPerMetricLimitPerSecondKey" validate:"nonzero"`
	WriteValuesPerMetricLimitPerSecond     int64            `yaml:"writeValuesPerMetricLimitPerSecond"`
	WriteNewMetricLimitClusterPerSecondKey string           `yaml:"writeNewMetricLimitClusterPerSecondKey" validate:"nonzero"`
	WriteNewMetricLimitClusterPerSecond    int64            `yaml:"writeNewMetricLimitClusterPerSecond"`
	WriteNewMetricNoLimitWarmupDuration    time.Duration    `yaml:"writeNewMetricNoLimitWarmupDuration"`
}

// NewRuntimeOptionsManager creates a new runtime options manager.
func (c RuntimeOptionsConfiguration) NewRuntimeOptionsManager() runtime.OptionsManager {
	initRuntimeOpts := runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(c.WriteValuesPerMetricLimitPerSecond).
		SetWriteNewMetricNoLimitWarmupDuration(c.WriteNewMetricNoLimitWarmupDuration)
	return runtime.NewOptionsManager(initRuntimeOpts)
}

// WatchRuntimeOptionChanges watches runtime option updates.
func (c RuntimeOptionsConfiguration) WatchRuntimeOptionChanges(
	client client.Client,
	runtimeOptsManager runtime.OptionsManager,
	placementManager aggregator.PlacementManager,
	logger log.Logger,
) {
	kvOpts, err := c.KVConfig.NewOptions()
	if err != nil {
		logger.Errorf("unable to create kv config options: %v", err)
		return
	}
	store, err := client.Store(kvOpts)
	if err != nil {
		logger.Errorf("unable to create kv store: %v", err)
		return
	}

	var (
		valueLimitKey                = c.WriteValuesPerMetricLimitPerSecondKey
		defaultValueLimit            = c.WriteValuesPerMetricLimitPerSecond
		valueLimit                   int64
		valueLimitCh                 <-chan struct{}
		newMetricClusterLimitKey     = c.WriteNewMetricLimitClusterPerSecondKey
		defaultNewMetricClusterLimit = c.WriteNewMetricLimitClusterPerSecond
		newMetricClusterLimit        int64
		newMetricPerShardLimit       int64
		newMetricLimitCh             <-chan struct{}
	)
	valueLimit, err = retrieveLimit(valueLimitKey, store, defaultValueLimit)
	if err != nil {
		logger.Errorf("unable to retrieve per-metric write value limit from kv: %v", err)
	}
	logger.Infof("current write value limit per second is: %d", valueLimit)

	newMetricClusterLimit, err = retrieveLimit(newMetricClusterLimitKey, store, defaultNewMetricClusterLimit)
	if err == nil {
		newMetricPerShardLimit, err = clusterLimitToPerShardLimit(newMetricClusterLimit, placementManager)
	}
	if err != nil {
		logger.Errorf("unable to determine per-shard write new metric limit: %v", err)
	}
	logger.Infof("current write new metric limit per shard per second is: %d", newMetricPerShardLimit)

	runtimeOpts := runtime.NewOptions().
		SetWriteNewMetricNoLimitWarmupDuration(c.WriteNewMetricNoLimitWarmupDuration).
		SetWriteValuesPerMetricLimitPerSecond(valueLimit).
		SetWriteNewMetricLimitPerShardPerSecond(newMetricPerShardLimit)
	runtimeOptsManager.SetRuntimeOptions(runtimeOpts)

	valueLimitWatch, err := store.Watch(valueLimitKey)
	if err != nil {
		logger.Errorf("unable to watch per-metric write value limit: %v", err)
	} else {
		valueLimitCh = valueLimitWatch.C()
	}
	newMetricLimitWatch, err := store.Watch(newMetricClusterLimitKey)
	if err != nil {
		logger.Errorf("unable to watch cluster-wide write new metric limit: %v", err)
	} else {
		newMetricLimitCh = newMetricLimitWatch.C()
	}
	// If watch creation failed for both, we return immediately.
	if valueLimitCh == nil && newMetricLimitCh == nil {
		return
	}

	go func() {
		for {
			select {
			case <-valueLimitCh:
				valueLimitVal := valueLimitWatch.Get()
				newValueLimit, err := kvutil.Int64FromValue(valueLimitVal, valueLimitKey, defaultValueLimit, nil)
				if err != nil {
					logger.Errorf("unable to determine per-metric write value limit: %v", err)
					continue
				}
				currValueLimit := runtimeOpts.WriteValuesPerMetricLimitPerSecond()
				if newValueLimit == currValueLimit {
					logger.Infof("per-metric write value limit %d is unchanged, skipping", newValueLimit)
					continue
				}
				logger.Infof("updating per-metric write value limit from %d to %d", currValueLimit, newValueLimit)
				runtimeOpts = runtimeOpts.SetWriteValuesPerMetricLimitPerSecond(newValueLimit)
				runtimeOptsManager.SetRuntimeOptions(runtimeOpts)
			case <-newMetricLimitCh:
				newMetricLimitVal := newMetricLimitWatch.Get()
				var newNewMetricPerShardLimit int64
				newNewMetricClusterLimit, err := kvutil.Int64FromValue(newMetricLimitVal, newMetricClusterLimitKey, defaultNewMetricClusterLimit, nil)
				if err == nil {
					newNewMetricPerShardLimit, err = clusterLimitToPerShardLimit(newNewMetricClusterLimit, placementManager)
				}
				if err != nil {
					logger.Errorf("unable to determine per-shard new metric limit: %v", err)
					continue
				}
				currNewMetricPerShardLimit := runtimeOpts.WriteNewMetricLimitPerShardPerSecond()
				if newNewMetricPerShardLimit == currNewMetricPerShardLimit {
					logger.Infof("per-shard write new metric limit %d is unchanged, skipping", newNewMetricPerShardLimit)
					continue
				}
				logger.Infof("updating per-shard write new metric limit from %d to %d", currNewMetricPerShardLimit, newNewMetricPerShardLimit)
				runtimeOpts = runtimeOpts.SetWriteNewMetricLimitPerShardPerSecond(newNewMetricPerShardLimit)
				runtimeOptsManager.SetRuntimeOptions(runtimeOpts)
			}
		}
	}()
}

func clusterLimitToPerShardLimit(
	clusterLimit int64,
	placementManager aggregator.PlacementManager,
) (int64, error) {
	if clusterLimit < 1 {
		return 0, nil
	}
	_, placement, err := placementManager.Placement()
	if err != nil {
		return clusterLimit, err
	}
	numShardsPerReplica := placement.NumShards()
	numShards := numShardsPerReplica * placement.ReplicaFactor()
	if numShards < 1 {
		return clusterLimit, nil
	}
	perShardLimit := int64(math.Ceil(float64(clusterLimit) / float64(numShards)))
	return perShardLimit, nil
}

func retrieveLimit(key string, store kv.Store, defaultLimit int64) (int64, error) {
	limit := defaultLimit
	value, err := store.Get(key)
	if err == nil {
		limit, err = kvutil.Int64FromValue(value, key, defaultLimit, nil)
	}
	return limit, err
}
