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

package resources

import (
	"fmt"
	"time"

	protobuftypes "github.com/gogo/protobuf/types"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"
)

const (
	retention = "6h"

	// AggName is the name of the aggregated namespace.
	AggName = "aggregated"
	// UnaggName is the name of the unaggregated namespace.
	UnaggName = "default"
	// ColdWriteNsName is the name for cold write namespace.
	ColdWriteNsName = "coldWritesRepairAndNoIndex"
)

// SetupCluster setups m3 cluster on provided docker containers.
func SetupCluster(cluster M3Resources, opts *ClusterOptions) error { // nolint: gocyclo
	coordinator := cluster.Coordinator()
	iOpts := instrument.NewOptions()
	logger := iOpts.Logger().With(zap.String("source", "harness"))
	hosts := make([]*admin.Host, 0, len(cluster.Nodes()))
	ids := make([]string, 0, len(cluster.Nodes()))
	for i, n := range cluster.Nodes() {
		h, err := n.HostDetails(9000)
		if err != nil {
			logger.Error("could not get host details", zap.Error(err))
			return err
		}
		if opts.NumIsolationGroups > 0 {
			h.IsolationGroup = fmt.Sprintf("isogroup-%d", int32(i)%opts.NumIsolationGroups)
		}

		hosts = append(hosts, h)
		ids = append(ids, h.GetId())
	}

	replicationFactor := int32(1)
	numShards := int32(4)
	if opts != nil {
		if opts.ReplicationFactor > 0 {
			replicationFactor = opts.ReplicationFactor
		}
		if opts.NumShards > 0 {
			numShards = opts.NumShards
		}
	}

	var (
		aggDatabase = admin.DatabaseCreateRequest{
			Type:              "cluster",
			NamespaceName:     AggName,
			RetentionTime:     retention,
			NumShards:         numShards,
			ReplicationFactor: replicationFactor,
			Hosts:             hosts,
		}

		unaggDatabase = admin.DatabaseCreateRequest{
			NamespaceName: UnaggName,
			RetentionTime: retention,
		}

		coldWriteNamespace = admin.NamespaceAddRequest{
			Name: ColdWriteNsName,
			Options: &namespace.NamespaceOptions{
				BootstrapEnabled:      true,
				FlushEnabled:          true,
				WritesToCommitLog:     true,
				CleanupEnabled:        true,
				SnapshotEnabled:       true,
				RepairEnabled:         true,
				ColdWritesEnabled:     true,
				CacheBlocksOnRetrieve: &protobuftypes.BoolValue{Value: true},
				RetentionOptions: &namespace.RetentionOptions{
					RetentionPeriodNanos:                     int64(4 * time.Hour),
					BlockSizeNanos:                           int64(time.Hour),
					BufferFutureNanos:                        int64(time.Minute * 10),
					BufferPastNanos:                          int64(time.Minute * 10),
					BlockDataExpiry:                          true,
					BlockDataExpiryAfterNotAccessPeriodNanos: int64(time.Minute * 5),
				},
			},
		}
	)

	logger.Info("waiting for coordinator")
	if err := coordinator.WaitForNamespace(""); err != nil {
		return err
	}

	logger.Info("creating database", zap.Any("request", aggDatabase))
	if _, err := coordinator.CreateDatabase(aggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for placements", zap.Strings("placement ids", ids))
	if err := coordinator.WaitForInstances(ids); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", AggName))
	if err := coordinator.WaitForNamespace(AggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", UnaggName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return err
	}

	logger.Info("creating namespace", zap.Any("request", coldWriteNamespace))
	if _, err := coordinator.AddNamespace(coldWriteNamespace); err != nil {
		return err
	}

	logger.Info("waiting for namespace", zap.String("name", ColdWriteNsName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return err
	}

	logger.Info("waiting for healthy")
	if err := cluster.Nodes().WaitForHealthy(); err != nil {
		return err
	}

	logger.Info("waiting for shards ready")
	if err := coordinator.WaitForShardsReady(); err != nil {
		return err
	}

	logger.Info("all healthy")
	return nil
}
