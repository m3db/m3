package main

import (
    "context"
    "flag"
    "fmt"
    "math/rand"
    "os"
    "os/signal"
    "syscall"
    "time"

    yaml "gopkg.in/yaml.v2"

    "go.uber.org/zap"
    "github.com/uber-go/tally"

    m3aggclient "github.com/m3db/m3/src/aggregator/client"
    etcdclient "github.com/m3db/m3/src/cluster/client/etcd"
    "github.com/m3db/m3/src/cluster/services"
    "github.com/m3db/m3/src/metrics/metadata"
    "github.com/m3db/m3/src/metrics/metric"
    // "github.com/m3db/m3/src/metrics/metric/aggregated"
    "github.com/m3db/m3/src/metrics/metric/unaggregated"
    "github.com/m3db/m3/src/metrics/policy"
    "github.com/m3db/m3/src/x/clock"
    xio "github.com/m3db/m3/src/x/io"
    xtime "github.com/m3db/m3/src/x/time"
    "github.com/m3db/m3/src/x/instrument"
)

type config struct {
    // Type: "tcp" or "m3msg" (defaults to legacy tcp if empty)
    Client m3aggclient.Configuration `yaml:"client"`

    // Metric ID to write, e.g. "foo,tagA=bar,tagB=baz"
    MetricID string `yaml:"metricID"`

    // Metric type: counter|gauge|timer
    MetricType string `yaml:"metricType"`

    // Value to write for gauge/timer. For counter, increments by 1 unless specified.
    Value float64 `yaml:"value"`

    // Passthrough storage policy for direct write (resolution, retention), e.g. "1s:24h".
    // If empty, will use staged metadatas with typical defaults (1s, 1m) if provided.
    PassthroughPolicy string `yaml:"passthroughPolicy"`

    // Interval between metric sends, e.g. "1s" or "5s".
    Interval time.Duration `yaml:"interval"`

    // Random jitter added to the interval (optional)
    Jitter time.Duration `yaml:"jitter"`

    // Optional: annotation to attach to metric
    Annotation string `yaml:"annotation"`

    // Optional: tally metrics scope prefix (for client internal metrics)
    MetricsPrefix string `yaml:"metricsPrefix"`
}

func main() {
    cfgPath := flag.String("config", "./client.yml", "Path to YAML config")
    once := flag.Bool("once", false, "Send a single sample then exit")
    flag.Parse()

    logger, _ := zap.NewProduction()
    defer logger.Sync() //nolint:errcheck
    iopts := instrument.NewOptions().SetLogger(logger)

    b, err := os.ReadFile(*cfgPath)
    if err != nil {
        panic(fmt.Errorf("failed to read config: %w", err))
    }

    var cfg config
    if err := yaml.Unmarshal(b, &cfg); err != nil {
        panic(fmt.Errorf("failed to parse yaml: %w", err))
    }

    if cfg.MetricsPrefix != "" {
        scope := tally.NewTestScope(cfg.MetricsPrefix, map[string]string{})
        iopts = iopts.SetMetricsScope(scope)
    }

    // Build an etcd-backed cluster client used by aggregator client for placement/topic
    // In local dev, etcd is available at localhost:2379 with env default_env and zone embedded.
    // The aggregator client.Configuration embedded inside cfg.Client references placementKV and/or m3msg producer config
    // which themselves need a cluster client when creating options.
    etcdOpts := etcdclient.NewOptions().
        SetEnv("default_env").
        SetZone("embedded").
        SetService("m3aggregator").
        SetClusters([]etcdclient.Cluster{etcdclient.NewCluster().SetZone("embedded").SetEndpoints([]string{"localhost:2379"})}).
        SetServicesOptions(services.NewOptions()).
        SetInstrumentOptions(iopts)

    clusterClient, err := etcdclient.NewConfigServiceClient(etcdOpts)
    if err != nil {
        panic(fmt.Errorf("failed creating etcd cluster client: %w", err))
    }

    // Create aggregator client
    clientOpts, err := cfg.Client.NewClientOptions(clusterClient, clock.NewOptions(), iopts, xio.NewOptions())
    if err != nil {
        panic(fmt.Errorf("failed creating aggregator client options: %w", err))
    }
    aggClient, err := m3aggclient.NewClient(clientOpts)
    if err != nil {
        panic(fmt.Errorf("failed creating aggregator client: %w", err))
    }
    if err := aggClient.Init(); err != nil {
        panic(fmt.Errorf("failed initializing aggregator client: %w", err))
    }
    defer aggClient.Close() //nolint:errcheck

    // Build metric template
    if cfg.MetricID == "" {
        cfg.MetricID = "test.metric,app=m3dev,env=local"
    }
    // Force counter type for untimed counter writes
    if cfg.MetricType == "" {
        cfg.MetricType = metric.CounterType.String()
    }
    // Ensure parseable type, but we will always use Counter below
    _ = metric.MustParseType(cfg.MetricType)
    if cfg.Interval <= 0 {
        cfg.Interval = time.Second
    }

    // PassthroughPolicy is ignored when using WriteUntimedCounter

    // Signal handling for graceful shutdown
    ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
    defer cancel()

    sendOnce := func(now time.Time) error {
        ts := now.UnixNano()
        // Default to increment by 1 unless a positive value is provided
        val := int64(1)
        if cfg.Value > 0 {
            val = int64(cfg.Value)
        }
        counter := unaggregated.Counter{
            ID:              []byte(cfg.MetricID),
            Value:           val,
            ClientTimeNanos: xtime.UnixNano(ts),
        }
        if cfg.Annotation != "" {
            counter.Annotation = []byte(cfg.Annotation)
        }

        // Default staged metadatas: write to common storage policy (1s for 1h).
        metas := metadata.StagedMetadatas{
            {
                Metadata: metadata.Metadata{
                    Pipelines: metadata.PipelineMetadatas{
                        {
                            StoragePolicies: []policy.StoragePolicy{
                                policy.NewStoragePolicy(time.Second * 10, 5 * xtime.Second, time.Hour * 48),
                            },
							RoutingPolicy: policy.RoutingPolicy{
								TrafficTypes: 1 << 0,
							},
                        },
                    },
                },
                CutoverNanos: 0,
                Tombstoned:   false,
            },
        }
        // // Build proto directly and inspect before marshal
        // cmProto := metricpb.CounterWithMetadatas{}
        // _ = (unaggregated.CounterWithMetadatas{Counter: counter, StagedMetadatas: metas}).ToProto(&cmProto)
        // if len(cmProto.Metadatas.Metadatas) > 0 && len(cmProto.Metadatas.Metadatas[0].Metadata.Pipelines) > 0 {
        //     fmt.Println("client direct proto routing_policy:", cmProto.Metadatas.Metadatas[0].Metadata.Pipelines[0].RoutingPolicy)
        // }

        // // Encode using the same generated encoder that client uses, then decode with the iterator
        // enc := pbenc.NewUnaggregatedEncoder(pbenc.NewUnaggregatedOptions())
        // msg := encoding.UnaggregatedMessageUnion{
        //     Type: encoding.CounterWithMetadatasType,
        //     CounterWithMetadatas: unaggregated.CounterWithMetadatas{
        //         Counter:         counter,
        //         StagedMetadatas: metas,
        //     },
        // }
        // if err := enc.EncodeMessage(msg); err != nil {
        //     panic(fmt.Errorf("failed to encode message: %w", err))
        // }
        // buf := enc.Relinquish().Bytes()
        // fmt.Println("buf start")
        // fmt.Println(buf)
        // fmt.Println("buf end")
        // it := pbenc.NewUnaggregatedIterator(bytes.NewReader(buf), pbenc.NewUnaggregatedOptions())
        // defer it.Close()
        // if it.Next() {
        //     cur := it.Current()
        //     if cur.Type == encoding.CounterWithMetadatasType {
        //         sms := cur.CounterWithMetadatas.StagedMetadatas
        //         if len(sms) > 0 && len(sms[0].Metadata.Pipelines) > 0 {
        //             fmt.Println("client selfcheck decoded routing_policy:", sms[0].Metadata.Pipelines[0].RoutingPolicy)
        //         } else {
        //             fmt.Println("client selfcheck decoded routing_policy: <none>")
        //         }
        //     } else {
        //         fmt.Println("client selfcheck unexpected message type:", cur.Type)
        //     }
        // } else if err := it.Err(); err != nil {
        //     fmt.Println("selfcheck decode error:", err)
        // }
        // Debug: verify routing policy encoded in protobuf before send -- This piece is fine
        // var metasPB metricpb.StagedMetadatas
        // _ = metas.ToProto(&metasPB)
        // if len(metasPB.Metadatas) > 0 && len(metasPB.Metadatas[0].Metadata.Pipelines) > 0 {
        //     fmt.Println("client pb routing_policy:", metasPB.Metadatas[0].Metadata.Pipelines[0].RoutingPolicy)
        // }
        return aggClient.WriteUntimedCounter(counter, metas)
        // return nil
    }

    ticker := time.NewTicker(cfg.Interval)
    defer ticker.Stop()

    // Fire immediately unless --once
    if *once {
        if err := sendOnce(time.Now()); err != nil {
            fmt.Fprintf(os.Stderr, "send error: %v\n", err)
            os.Exit(1)
        }
        return
    }

    // Initial send
    if err := sendOnce(time.Now()); err != nil {
        fmt.Fprintf(os.Stderr, "send error: %v\n", err)
    }

    for {
        select {
        case <-ctx.Done():
            // best-effort flush
            _ = aggClient.Flush()
            return
        case t := <-ticker.C:
            // jitter if configured
            if cfg.Jitter > 0 {
                j := time.Duration(rand.Int63n(int64(cfg.Jitter)))
                time.Sleep(j)
            }
            if err := sendOnce(t); err != nil {
                fmt.Fprintf(os.Stderr, "send error: %v\n", err)
            }
        }
    }
}
