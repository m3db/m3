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
	"testing"
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator"
	"github.com/m3db/m3/src/aggregator/runtime"
	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv/mem"
	"github.com/m3db/m3/src/cluster/placement"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestRuntimeOptionsConfigurationNewRuntimeOptionsManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := `
kvConfig:
  zone: test
  environment: production
writeValuesPerMetricLimitPerSecondKey: rate-limit-key
writeValuesPerMetricLimitPerSecond: 0
writeNewMetricLimitClusterPerSecondKey: new-metric-limit-key
writeNewMetricLimitClusterPerSecond: 0
writeNewMetricNoLimitWarmupDuration: 10m
`
	var cfg RuntimeOptionsConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(config), &cfg))
	require.Equal(t, "test", cfg.KVConfig.Zone)
	require.Equal(t, "production", cfg.KVConfig.Environment)
	require.Equal(t, "rate-limit-key", cfg.WriteValuesPerMetricLimitPerSecondKey)
	require.Equal(t, int64(0), cfg.WriteValuesPerMetricLimitPerSecond)
	require.Equal(t, "new-metric-limit-key", cfg.WriteNewMetricLimitClusterPerSecondKey)
	require.Equal(t, int64(0), cfg.WriteNewMetricLimitClusterPerSecond)
	require.Equal(t, 10*time.Minute, cfg.WriteNewMetricNoLimitWarmupDuration)

	initialValueLimit := int64(100)
	proto := &commonpb.Int64Proto{Value: initialValueLimit}
	memStore := mem.NewStore()
	_, err := memStore.Set("rate-limit-key", proto)
	require.NoError(t, err)

	initialNewMetricLimit := int64(32)
	proto = &commonpb.Int64Proto{Value: initialNewMetricLimit}
	_, err = memStore.Set("new-metric-limit-key", proto)
	require.NoError(t, err)

	runtimeOptsManager := cfg.NewRuntimeOptionsManager()
	testShards := []uint32{0, 1, 2, 3}
	testPlacement := placement.NewPlacement().SetReplicaFactor(2).SetShards(testShards)
	testPlacementManager := aggregator.NewMockPlacementManager(ctrl)
	testPlacementManager.EXPECT().Placement().Return(testPlacement, nil).AnyTimes()

	mockClient := client.NewMockClient(ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(memStore, nil)
	logger := xtest.NewLogger(t)
	cfg.WatchRuntimeOptionChanges(mockClient, runtimeOptsManager, testPlacementManager, logger)
	runtimeOpts := runtimeOptsManager.RuntimeOptions()
	expectedOpts := runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(initialValueLimit).
		SetWriteNewMetricLimitPerShardPerSecond(4).
		SetWriteNewMetricNoLimitWarmupDuration(10 * time.Minute)
	require.Equal(t, expectedOpts, runtimeOpts)

	// Set a new value limit.
	newValueLimit := int64(1000)
	proto.Value = newValueLimit
	_, err = memStore.Set("rate-limit-key", proto)
	require.NoError(t, err)
	expectedOpts = runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(1000).
		SetWriteNewMetricLimitPerShardPerSecond(4).
		SetWriteNewMetricNoLimitWarmupDuration(10 * time.Minute)
	for {
		runtimeOpts = runtimeOptsManager.RuntimeOptions()
		if compareRuntimeOptions(expectedOpts, runtimeOpts) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Revert value limit to initial limit.
	newValueLimit = 100
	proto.Value = newValueLimit
	_, err = memStore.Set("rate-limit-key", proto)
	require.NoError(t, err)
	expectedOpts = runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(100).
		SetWriteNewMetricLimitPerShardPerSecond(4).
		SetWriteNewMetricNoLimitWarmupDuration(10 * time.Minute)
	for {
		runtimeOpts = runtimeOptsManager.RuntimeOptions()
		if compareRuntimeOptions(expectedOpts, runtimeOpts) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Set a new new-metric limit.
	newNewMetricLimit := int64(128)
	proto.Value = newNewMetricLimit
	_, err = memStore.Set("new-metric-limit-key", proto)
	require.NoError(t, err)
	expectedOpts = runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(100).
		SetWriteNewMetricLimitPerShardPerSecond(16).
		SetWriteNewMetricNoLimitWarmupDuration(10 * time.Minute)
	for {
		runtimeOpts = runtimeOptsManager.RuntimeOptions()
		if compareRuntimeOptions(expectedOpts, runtimeOpts) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	// Revert to default new-metric limit.
	newNewMetricLimit = int64(32)
	proto.Value = newNewMetricLimit
	_, err = memStore.Set("new-metric-limit-key", proto)
	require.NoError(t, err)
	expectedOpts = runtime.NewOptions().
		SetWriteValuesPerMetricLimitPerSecond(100).
		SetWriteNewMetricLimitPerShardPerSecond(4).
		SetWriteNewMetricNoLimitWarmupDuration(10 * time.Minute)
	for {
		runtimeOpts = runtimeOptsManager.RuntimeOptions()
		if compareRuntimeOptions(expectedOpts, runtimeOpts) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func compareRuntimeOptions(expected, actual runtime.Options) bool {
	return expected.WriteNewMetricLimitPerShardPerSecond() == actual.WriteNewMetricLimitPerShardPerSecond() &&
		expected.WriteNewMetricNoLimitWarmupDuration() == actual.WriteNewMetricNoLimitWarmupDuration() &&
		expected.WriteValuesPerMetricLimitPerSecond() == actual.WriteValuesPerMetricLimitPerSecond()
}
