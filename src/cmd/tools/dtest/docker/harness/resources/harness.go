// Copyright (c) 2020 Uber Technologies, Inc.
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

// Package resources contains resources needed to setup docker containers for M3 tests.
package resources

import (
	"time"

	"github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	protobuftypes "github.com/gogo/protobuf/types"
	"github.com/ory/dockertest"
	"go.uber.org/zap"
)

const (
	timeout   = time.Second * 60
	retention = "6h"

	// AggName is the name of the aggregated namespace.
	AggName = "aggregated"
	// UnaggName is the name of the unaggregated namespace.
	UnaggName = "default"
	// ColdWriteNsName is the name for cold write namespace.
	ColdWriteNsName = "coldWritesRepairAndNoIndex"
)

// DockerResources represents a set of dockerized test components.
type DockerResources interface {
	// Cleanup closes and removes all corresponding containers.
	Cleanup() error
	// Nodes returns all node resources.
	Nodes() Nodes
	// Coordinator returns the coordinator resource.
	Coordinator() Coordinator
}

type dockerResources struct {
	coordinator Coordinator
	nodes       Nodes

	pool *dockertest.Pool
}

// SetupSingleM3DBNode creates docker resources representing a setup with a
// single DB node.
func SetupSingleM3DBNode(opts ...SetupOptions) (DockerResources, error) { // nolint: gocyclo
	options := setupOptions{}
	for _, f := range opts {
		f(&options)
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	pool.MaxWait = timeout
	err = setupNetwork(pool)
	if err != nil {
		return nil, err
	}

	err = setupVolume(pool)
	if err != nil {
		return nil, err
	}

	iOpts := instrument.NewOptions()
	dbNode, err := newDockerHTTPNode(pool, dockerResourceOptions{
		image: options.dbNodeImage,
		iOpts: iOpts,
	})

	success := false
	dbNodes := Nodes{dbNode}
	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success {
			for _, dbNode := range dbNodes {
				if dbNode != nil {
					dbNode.Close()
				}
			}
		}
	}()

	if err != nil {
		return nil, err
	}

	coordinator, err := newDockerHTTPCoordinator(pool, dockerResourceOptions{
		image: options.coordinatorImage,
		iOpts: iOpts,
	})

	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success && coordinator != nil {
			coordinator.Close()
		}
	}()

	if err != nil {
		return nil, err
	}

	logger := iOpts.Logger().With(zap.String("source", "harness"))
	hosts := make([]*admin.Host, 0, len(dbNodes))
	ids := make([]string, 0, len(dbNodes))
	for _, n := range dbNodes {
		h, err := n.HostDetails(9000)
		if err != nil {
			logger.Error("could not get host details", zap.Error(err))
			return nil, err
		}

		hosts = append(hosts, h)
		ids = append(ids, h.GetId())
	}

	var (
		aggDatabase = admin.DatabaseCreateRequest{
			Type:              "cluster",
			NamespaceName:     AggName,
			RetentionTime:     retention,
			NumShards:         4,
			ReplicationFactor: 1,
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
		return nil, err
	}

	logger.Info("creating database", zap.Any("request", aggDatabase))
	if _, err := coordinator.CreateDatabase(aggDatabase); err != nil {
		return nil, err
	}

	logger.Info("waiting for placements", zap.Strings("placement ids", ids))
	if err := coordinator.WaitForInstances(ids); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", AggName))
	if err := coordinator.WaitForNamespace(AggName); err != nil {
		return nil, err
	}

	logger.Info("creating namespace", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", UnaggName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return nil, err
	}

	logger.Info("creating namespace", zap.Any("request", coldWriteNamespace))
	if _, err := coordinator.AddNamespace(coldWriteNamespace); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", ColdWriteNsName))
	if err := coordinator.WaitForNamespace(UnaggName); err != nil {
		return nil, err
	}

	logger.Info("waiting for healthy")
	if err := dbNodes.waitForHealthy(); err != nil {
		return nil, err
	}

	logger.Info("all healthy")
	success = true
	return &dockerResources{
		coordinator: coordinator,
		nodes:       dbNodes,

		pool: pool,
	}, err
}

func (r *dockerResources) Cleanup() error {
	if r == nil {
		return nil
	}

	var multiErr xerrors.MultiError
	if r.coordinator != nil {
		multiErr = multiErr.Add(r.coordinator.Close())
	}

	for _, dbNode := range r.nodes {
		if dbNode != nil {
			multiErr = multiErr.Add(dbNode.Close())
		}
	}

	return multiErr.FinalError()
}

func (r *dockerResources) Nodes() Nodes             { return r.nodes }
func (r *dockerResources) Coordinator() Coordinator { return r.coordinator }
