// Copyright (c) 2021  Uber Technologies, Inc.
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

// Package config contains configuration for the aggregator.
package config

import (
	"time"

	"github.com/m3db/m3/src/aggregator/aggregator/handler"
	aggclient "github.com/m3db/m3/src/aggregator/client"
	"github.com/m3db/m3/src/aggregator/sharding"
	etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/metrics/aggregation"
	"github.com/m3db/m3/src/msg/consumer"
	producerconfig "github.com/m3db/m3/src/msg/producer/config"
	"github.com/m3db/m3/src/x/config/hostid"
	"github.com/m3db/m3/src/x/instrument"
	"github.com/m3db/m3/src/x/log"
	"github.com/m3db/m3/src/x/pool"
	"github.com/m3db/m3/src/x/retry"
	xserver "github.com/m3db/m3/src/x/server"
)

// This file contains a set of sane defaults for spinning up the aggregator.
// This is mostly useful for spinning up an aggregator for local development
// or tests.

const (
	defaultZone      = "embedded"
	defaultEnv       = "default_env"
	defaultService   = "m3aggregator"
	defaultNamespace = "/placement"
)

var (
	defaultLogging = log.Configuration{
		Level: "info",
	}
	defaultMetricsSanitization        = instrument.PrometheusMetricSanitization
	defaultMetricsExtendedMetricsType = instrument.NoExtendedMetrics
	defaultMetrics                    = instrument.MetricsConfiguration{
		PrometheusReporter: &instrument.PrometheusConfiguration{
			OnError:       "none",
			HandlerPath:   "/metrics",
			ListenAddress: "0.0.0.0:6002",
			TimerType:     "histogram",
		},
		Sanitization:    &defaultMetricsSanitization,
		SamplingRate:    1.0,
		ExtendedMetrics: &defaultMetricsExtendedMetricsType,
	}
	defaultJitter = true
	defaultM3Msg  = M3MsgServerConfiguration{
		Server: xserver.Configuration{
			ListenAddress: "0.0.0.0:6000",
			Retry: retry.Configuration{
				MaxBackoff: 10 * time.Second,
				Jitter:     &defaultJitter,
			},
		},
		Consumer: consumer.Configuration{
			MessagePool: &consumer.MessagePoolConfiguration{
				Size: 16384,
				Watermark: pool.WatermarkConfiguration{
					RefillLowWatermark:  0.2,
					RefillHighWatermark: 0.5,
				},
			},
		},
	}
	defaultHTTP = HTTPServerConfiguration{
		ListenAddress: "0.0.0.0:6001",
		ReadTimeout:   60 * time.Second,
		WriteTimeout:  60 * time.Second,
	}
	defaultKV = KVClientConfiguration{
		Etcd: &etcdclient.Configuration{
			Zone:     defaultZone,
			Env:      defaultEnv,
			Service:  defaultService,
			CacheDir: "/var/lib/m3kv",
			ETCDClusters: []etcdclient.ClusterConfig{
				{
					Zone: defaultZone,
					Endpoints: []string{
						"0.0.0.0:2379",
					},
				},
			},
		},
	}
	defaultEmptyPrefix         = ""
	defaultHashType            = sharding.Murmur32Hash
	defaultIsStaged            = true
	defaultForever             = true
	defaultNumCachedSourceSets = 2
	defaultDiscardNanValues    = true
	defaultRuntimeOptions      = RuntimeOptionsConfiguration{
		WriteValuesPerMetricLimitPerSecondKey:  "write-values-per-metric-limit-per-second",
		WriteNewMetricLimitClusterPerSecondKey: "write-new-metric-limit-cluster-per-second",
	}

	defaultAggregator = AggregatorConfiguration{
		HostID: &hostid.Configuration{
			Resolver: hostid.ConfigResolver,
			Value:    &defaultHostID,
		},
		InstanceID:    InstanceIDConfiguration{HostIDInstanceIDType},
		MetricPrefix:  &defaultEmptyPrefix,
		CounterPrefix: &defaultEmptyPrefix,
		TimerPrefix:   &defaultEmptyPrefix,
		GaugePrefix:   &defaultEmptyPrefix,
		AggregationTypes: aggregation.TypesConfiguration{
			CounterTransformFnType: &aggregation.EmptyTransformType,
			TimerTransformFnType:   &aggregation.SuffixTransformType,
			GaugeTransformFnType:   &aggregation.EmptyTransformType,
			AggregationTypesPool: pool.ObjectPoolConfiguration{
				Size: 1024,
			},
			QuantilesPool: pool.BucketizedPoolConfiguration{
				Buckets: []pool.BucketConfiguration{
					{
						Count:    256,
						Capacity: 4,
					},
					{
						Count:    128,
						Capacity: 8,
					},
				},
			},
		},
		Stream: streamConfiguration{
			Eps:        0.001,
			Capacity:   32,
			StreamPool: pool.ObjectPoolConfiguration{Size: 4096},
			SamplePool: &pool.ObjectPoolConfiguration{Size: 4096},
			FloatsPool: pool.BucketizedPoolConfiguration{
				Buckets: []pool.BucketConfiguration{
					{Count: 4096, Capacity: 16},
					{Count: 2048, Capacity: 32},
					{Count: 1024, Capacity: 64},
				},
			},
		},
		Client: aggclient.Configuration{
			Type: aggclient.M3MsgAggregatorClient,
			M3Msg: &aggclient.M3MsgConfiguration{
				Producer: producerconfig.ProducerConfiguration{
					Writer: producerconfig.WriterConfiguration{
						TopicName: "aggregator_ingest",
						PlacementOptions: placement.Configuration{
							IsStaged: &defaultIsStaged,
						},
						PlacementServiceOverride: services.OverrideConfiguration{
							Namespaces: services.NamespacesConfiguration{
								Placement: defaultNamespace,
							},
						},
						MessagePool: &pool.ObjectPoolConfiguration{
							Size: 16384,
							Watermark: pool.WatermarkConfiguration{
								RefillLowWatermark:  0.2,
								RefillHighWatermark: 0.5,
							},
						},
					},
				},
			},
		},
		PlacementManager: placementManagerConfiguration{
			KVConfig: kv.OverrideConfiguration{
				Namespace: defaultNamespace,
			},
			Watcher: placement.WatcherConfiguration{
				Key:              defaultService,
				InitWatchTimeout: 10 * time.Second,
			},
		},
		HashType:                           &defaultHashType,
		BufferDurationBeforeShardCutover:   10 * time.Minute,
		BufferDurationAfterShardCutoff:     10 * time.Minute,
		BufferDurationForFutureTimedMetric: 10 * time.Minute,
		BufferDurationForPastTimedMetric:   10 * time.Minute,
		ResignTimeout:                      1 * time.Minute,
		FlushTimesManager: flushTimesManagerConfiguration{
			FlushTimesKeyFmt: "shardset/%d/flush",
			FlushTimesPersistRetrier: retry.Configuration{
				InitialBackoff: 100 * time.Millisecond,
				BackoffFactor:  2.0,
				MaxBackoff:     2 * time.Second,
				MaxRetries:     3,
			},
		},
		ElectionManager: electionManagerConfiguration{
			Election: electionConfiguration{
				LeaderTimeout: 10 * time.Second,
				ResignTimeout: 10 * time.Second,
				TTLSeconds:    10,
			},
			ServiceID: serviceIDConfiguration{
				Name:        defaultService,
				Environment: defaultEnv,
				Zone:        defaultZone,
			},
			ElectionKeyFmt: "shardset/%d/lock",
			CampaignRetrier: retry.Configuration{
				InitialBackoff: 100 * time.Millisecond,
				BackoffFactor:  2.0,
				MaxBackoff:     2 * time.Second,
				Forever:        &defaultForever,
				Jitter:         &defaultJitter,
			},
			ChangeRetrier: retry.Configuration{
				InitialBackoff: 100 * time.Millisecond,
				BackoffFactor:  2.0,
				MaxBackoff:     5 * time.Second,
				Forever:        &defaultForever,
				Jitter:         &defaultJitter,
			},
			ResignRetrier: retry.Configuration{
				InitialBackoff: 100 * time.Millisecond,
				BackoffFactor:  2.0,
				MaxBackoff:     5 * time.Second,
				Forever:        &defaultForever,
				Jitter:         &defaultJitter,
			},
			CampaignStateCheckInterval: 1 * time.Second,
			ShardCutoffCheckOffset:     30 * time.Second,
		},
		FlushManager: flushManagerConfiguration{
			CheckEvery:    1 * time.Second,
			JitterEnabled: &defaultJitter,
			MaxJitters: []jitterBucket{
				{
					FlushInterval:    5 * time.Second,
					MaxJitterPercent: 1.0,
				},
				{
					FlushInterval:    10 * time.Second,
					MaxJitterPercent: 0.5,
				},
				{
					FlushInterval:    1 * time.Minute,
					MaxJitterPercent: 0.5,
				},
				{
					FlushInterval:    10 * time.Minute,
					MaxJitterPercent: 0.5,
				},
				{
					FlushInterval:    1 * time.Hour,
					MaxJitterPercent: 0.25,
				},
			},
			NumWorkersPerCPU:       0.5,
			FlushTimesPersistEvery: 10 * time.Second,
			MaxBufferSize:          5 * time.Minute,
			ForcedFlushWindowSize:  10 * time.Second,
		},
		Flush: handler.FlushConfiguration{
			Handlers: []handler.FlushHandlerConfiguration{
				{
					DynamicBackend: &handler.DynamicBackendConfiguration{
						Name:     "m3msg",
						HashType: sharding.Murmur32Hash,
						Producer: producerconfig.ProducerConfiguration{
							Writer: producerconfig.WriterConfiguration{
								TopicName: "aggregated_metrics",
								MessagePool: &pool.ObjectPoolConfiguration{
									Size: 16384,
									Watermark: pool.WatermarkConfiguration{
										RefillLowWatermark:  0.2,
										RefillHighWatermark: 0.5,
									},
								},
							},
						},
					},
				},
			},
		},
		Passthrough:                &passthroughConfiguration{Enabled: true},
		Forwarding:                 forwardingConfiguration{MaxConstDelay: 1 * time.Minute},
		EntryTTL:                   1 * time.Hour,
		EntryCheckInterval:         10 * time.Minute,
		MaxTimerBatchSizePerWrite:  140,
		MaxNumCachedSourceSets:     &defaultNumCachedSourceSets,
		DiscardNaNAggregatedValues: &defaultDiscardNanValues,
		EntryPool:                  pool.ObjectPoolConfiguration{Size: 4096},
		CounterElemPool:            pool.ObjectPoolConfiguration{Size: 4096},
		TimerElemPool:              pool.ObjectPoolConfiguration{Size: 4096},
		GaugeElemPool:              pool.ObjectPoolConfiguration{Size: 4096},
	}
)
