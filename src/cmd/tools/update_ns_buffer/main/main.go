// Tool to update bufferPast / bufferFuture on a namespace.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/cluster/client/etcd"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/x/instrument"
)

var flags struct {
	namespace     string
	etcdAddr      string
	m3Environment string
	bufferPast    time.Duration
	bufferFuture  time.Duration
}

func main() {
	flag.StringVar(&flags.namespace, "namespace", "", "namespace to update")
	flag.StringVar(&flags.etcdAddr, "etcd-addr", "127.0.0.1:2379", "address of etcd server")
	flag.StringVar(&flags.m3Environment, "m3-env", "", "name of environment to update")
	flag.DurationVar(&flags.bufferPast, "buffer-past", 0, "new buffer past value (will not be updated if 0)")
	flag.DurationVar(&flags.bufferFuture, "buffer-future", 0, "new buffer future value (will not be updated if 0)")
	flag.Parse()

	logger, err := zap.NewDevelopment()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Could not construct logger: %v", err)
		os.Exit(1)
	}

	if flags.namespace == "" {
		logger.Fatal("must pass -namespace")
	}

	if flags.m3Environment == "" {
		logger.Fatal("must set -m3-env")
	}

	clientConfig := etcd.Configuration{
		Zone:    "zone",
		Env:     flags.m3Environment,
		Service: "m3db",
		ETCDClusters: []etcd.ClusterConfig{
			{
				Zone: "zone",
				Endpoints: []string{
					flags.etcdAddr,
				},
			},
		},
	}

	client, err := clientConfig.NewClient(instrument.NewOptions())
	if err != nil {
		logger.Fatal("new client error", zap.Error(err))
	}

	kv, err := client.KV()
	if err != nil {
		logger.Fatal("kv error", zap.Error(err))
	}

	val, err := kv.Get(kvconfig.NamespacesKey)
	if err != nil {
		logger.Fatal("could not get namespaces", zap.Error(err))
	}

	logger.Info("fetched namespace value", zap.Any("value", val))

	var registry nsproto.Registry
	if err := val.Unmarshal(&registry); err != nil {
		logger.Fatal("could not unmarshal registry", zap.Error(err))
	}

	logger.Info("constructed registry", zap.Any("registry", registry))

	nsOpts, ok := registry.Namespaces[flags.namespace]
	if !ok {
		logger.Fatal("did not find namespace in registry", zap.String("namespace", flags.namespace))
	}

	logger.Info("current ns", zap.Any("ns", nsOpts))
	logger.Info("current buffers",
		zap.Duration("bufferPast", time.Duration(nsOpts.RetentionOptions.BufferPastNanos)),
		zap.Duration("bufferFuture", time.Duration(nsOpts.RetentionOptions.BufferFutureNanos)),
	)

	if flags.bufferFuture > 0 {
		logger.Info("setting new value for buffer future",
			zap.Duration("duration", flags.bufferFuture),
			zap.Int64("nanos", flags.bufferFuture.Nanoseconds()),
		)
		nsOpts.RetentionOptions.BufferFutureNanos = flags.bufferFuture.Nanoseconds()
	}

	if flags.bufferPast > 0 {
		logger.Info("setting new value for buffer past",
			zap.Duration("duration", flags.bufferPast),
			zap.Int64("nanos", flags.bufferPast.Nanoseconds()),
		)
		nsOpts.RetentionOptions.BufferPastNanos = flags.bufferPast.Nanoseconds()
	}

	logger.Info("press Enter to continue. ctrl-c to cancel")

	reader := bufio.NewReader(os.Stdin)
	if _, err := reader.ReadString('\n'); err != nil {
		logger.Fatal("error reading input", zap.Error(err))
	}

	newVer, err := kv.Set(kvconfig.NamespacesKey, &registry)
	if err != nil {
		logger.Fatal("error updating namespace", zap.Error(err))
	}

	logger.Info("successfully updated namespace", zap.Int("version", newVer))
}
