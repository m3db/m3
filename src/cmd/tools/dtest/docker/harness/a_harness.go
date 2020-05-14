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

package harness

import (
	"sync"
	"time"

	"github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	dockertest "github.com/ory/dockertest"
	"go.uber.org/zap"
)

const (
	aggName         = "aggregated"
	unaggName       = "unaggregated"
	coldWriteNsName = "coldWritesRepairAndNoIndex"
	retention       = "6h"
)

// Nodes is a slice of nodes.
type Nodes []Node

func (n Nodes) waitForHealthy() error {
	var (
		multiErr xerrors.MultiError
		mu       sync.Mutex
		wg       sync.WaitGroup
	)

	for _, node := range n {
		wg.Add(1)
		node := node
		go func() {
			defer wg.Done()
			err := node.WaitForBootstrap()
			if err != nil {
				mu.Lock()
				multiErr = multiErr.Add(err)
				mu.Unlock()
			}
		}()
	}

	wg.Wait()
	return multiErr.FinalError()
}

type cleanup func()

type dockerResources struct {
	coordinator Coordinator
	nodes       Nodes

	cleanup cleanup
}

func setupSingleM3DBNode() (*dockerResources, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	pool.MaxWait = time.Second * 60
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
		iOpts: iOpts,
	})

	if err != nil {
		return nil, err
	}

	success := false
	dbNodes := Nodes{dbNode}
	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success {
			for _, dbNode := range dbNodes {
				dbNode.Close()
			}
		}
	}()

	coordinator, err := newDockerHTTPCoordinator(pool, dockerResourceOptions{
		iOpts: iOpts,
	})

	if err != nil {
		return nil, err
	}

	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success {
			coordinator.Close()
		}
	}()

	logger := iOpts.Logger().With(zap.String("source", "harness"))
	hosts := make([]*admin.Host, 0, len(dbNodes))
	ids := make([]string, 0, len(dbNodes))
	for _, n := range dbNodes {
		h, err := n.HostDetails()
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
			NamespaceName:     aggName,
			RetentionTime:     retention,
			NumShards:         4,
			ReplicationFactor: 1,
			Hosts:             hosts,
		}

		unaggDatabase = admin.DatabaseCreateRequest{
			NamespaceName: unaggName,
			RetentionTime: retention,
		}

		coldWriteNamespace = admin.NamespaceAddRequest{
			Name: coldWriteNsName,
			Options: &namespace.NamespaceOptions{
				BootstrapEnabled:  true,
				FlushEnabled:      true,
				WritesToCommitLog: true,
				CleanupEnabled:    true,
				SnapshotEnabled:   true,
				RepairEnabled:     true,
				ColdWritesEnabled: true,
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
	if err := coordinator.WaitForPlacements(ids); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", aggName))
	if err := coordinator.WaitForNamespace(aggName); err != nil {
		return nil, err
	}

	logger.Info("creating namespace", zap.Any("request", unaggDatabase))
	if _, err := coordinator.CreateDatabase(unaggDatabase); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", unaggName))
	if err := coordinator.WaitForNamespace(unaggName); err != nil {
		return nil, err
	}

	logger.Info("creating namespace", zap.Any("request", coldWriteNamespace))
	if _, err := coordinator.AddNamespace(coldWriteNamespace); err != nil {
		return nil, err
	}

	logger.Info("waiting for namespace", zap.String("name", coldWriteNsName))
	if err := coordinator.WaitForNamespace(unaggName); err != nil {
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

		cleanup: func() {
			coordinator.Close()
			for _, dbNode := range dbNodes {
				dbNode.Close()
			}
		},
	}, err
}
