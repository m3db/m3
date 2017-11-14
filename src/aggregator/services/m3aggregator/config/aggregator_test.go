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

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/commonpb"
	"github.com/m3db/m3cluster/kv/mem"
	"github.com/m3db/m3x/log"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	yaml "gopkg.in/yaml.v2"
)

func TestJitterBuckets(t *testing.T) {
	config := `
    - flushInterval: 1m
      maxJitterPercent: 1.0
    - flushInterval: 10m
      maxJitterPercent: 0.5
    - flushInterval: 1h
      maxJitterPercent: 0.25`

	var buckets jitterBuckets
	require.NoError(t, yaml.Unmarshal([]byte(config), &buckets))

	maxJitterFn, err := buckets.NewMaxJitterFn()
	require.NoError(t, err)

	inputs := []struct {
		interval          time.Duration
		expectedMaxJitter time.Duration
	}{
		{interval: time.Second, expectedMaxJitter: time.Second},
		{interval: 10 * time.Second, expectedMaxJitter: 10 * time.Second},
		{interval: time.Minute, expectedMaxJitter: time.Minute},
		{interval: 10 * time.Minute, expectedMaxJitter: 5 * time.Minute},
		{interval: time.Hour, expectedMaxJitter: 15 * time.Minute},
		{interval: 6 * time.Hour, expectedMaxJitter: 90 * time.Minute},
	}
	for _, input := range inputs {
		require.Equal(t, input.expectedMaxJitter, maxJitterFn(input.interval))
	}
}

func TestRuntimeOptionsConfigurationNewRuntimeOptionsManager(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	config := `
kvConfig:
  environment: production
writeValuesPerMetricLimitPerSecondKey: rate-limit-key
writeValuesPerMetricLimitPerSecond: 0
`
	var runtimeOptionsCfg runtimeOptionsConfiguration
	require.NoError(t, yaml.Unmarshal([]byte(config), &runtimeOptionsCfg))

	initialLimit := int64(100)
	proto := &commonpb.Int64Proto{Value: initialLimit}
	memStore := mem.NewStore()
	_, err := memStore.Set("rate-limit-key", proto)
	require.NoError(t, err)

	logger := log.NewLevelLogger(log.SimpleLogger, log.LevelInfo)
	mockClient := client.NewMockClient(ctrl)
	mockClient.EXPECT().Store(gomock.Any()).Return(memStore, nil)
	runtimeOptsManager, err := runtimeOptionsCfg.NewRuntimeOptionsManager(mockClient, logger)
	require.NoError(t, err)
	require.Equal(t, initialLimit, runtimeOptsManager.RuntimeOptions().WriteValuesPerMetricLimitPerSecond())

	newLimit := int64(1000)
	proto.Value = newLimit
	_, err = memStore.Set("rate-limit-key", proto)
	require.NoError(t, err)
	for {
		if runtimeOptsManager.RuntimeOptions().WriteValuesPerMetricLimitPerSecond() == newLimit {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	_, err = memStore.Delete("rate-limit-key")
	require.NoError(t, err)
	for {
		if runtimeOptsManager.RuntimeOptions().WriteValuesPerMetricLimitPerSecond() == 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
}
