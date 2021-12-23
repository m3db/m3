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

// Package docker contains resources needed to setup docker containers for M3 tests.
package docker

import (
	"time"

	"github.com/ory/dockertest/v3"
	"go.uber.org/zap"

	"github.com/m3db/m3/src/integration/resources"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
)

const timeout = time.Second * 60

type dockerResources struct {
	coordinator resources.Coordinator
	nodes       resources.Nodes

	pool *dockertest.Pool
}

// SetupSingleM3DBNode creates docker resources representing a setup with a
// single DB node.
func SetupSingleM3DBNode(opts ...SetupOptions) (resources.M3Resources, error) { // nolint: gocyclo
	options := setupOptions{}
	for _, f := range opts {
		f(&options)
	}

	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}

	pool.MaxWait = timeout

	if !options.existingCluster {
		if err := SetupNetwork(pool); err != nil {
			return nil, err
		}

		if err := setupVolume(pool); err != nil {
			return nil, err
		}
	}

	iOpts := instrument.NewOptions()
	dbNode, err := newDockerHTTPNode(pool, ResourceOptions{
		Image:          options.dbNodeImage,
		ContainerName:  options.dbNodeContainerName,
		InstrumentOpts: iOpts,
	})

	success := false
	dbNodes := resources.Nodes{dbNode}
	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success {
			for _, dbNode := range dbNodes {
				if dbNode != nil {
					dbNode.Close() //nolint:errcheck
				}
			}
		}
	}()

	if err != nil {
		return nil, err
	}

	coordinator, err := newDockerHTTPCoordinator(pool, ResourceOptions{
		Image:          options.coordinatorImage,
		ContainerName:  options.coordinatorContainerName,
		InstrumentOpts: iOpts,
	})

	defer func() {
		// NB: only defer close in the failure case, otherwise calling function
		// is responsible for closing the resources.
		if !success && coordinator != nil {
			coordinator.Close() //nolint:errcheck
		}
	}()

	if err != nil {
		return nil, err
	}

	cluster := &dockerResources{
		coordinator: coordinator,
		nodes:       dbNodes,
		pool:        pool,
	}
	err = resources.SetupCluster(cluster, resources.ClusterOptions{})

	logger := iOpts.Logger().With(zap.String("source", "harness"))
	logger.Info("all healthy")
	success = true
	return cluster, err
}

// AttachToExistingContainers attaches docker API to an existing coordinator
// and one or more dbnode containers.
func AttachToExistingContainers(
	coordinatorContainerName string,
	dbNodesContainersNames []string,
) (resources.M3Resources, error) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		return nil, err
	}
	pool.MaxWait = timeout

	iOpts := instrument.NewOptions()
	dbNodes := resources.Nodes{}
	for _, containerName := range dbNodesContainersNames {
		dbNode, err := newDockerHTTPNode(pool, ResourceOptions{
			InstrumentOpts: iOpts,
			ContainerName:  containerName,
		})
		if err != nil {
			return nil, err
		}
		dbNodes = append(dbNodes, dbNode)
	}

	coordinator, err := newDockerHTTPCoordinator(
		pool,
		ResourceOptions{
			InstrumentOpts: iOpts,
			ContainerName:  coordinatorContainerName,
		},
	)
	if err != nil {
		return nil, err
	}

	return &dockerResources{
		coordinator: coordinator,
		nodes:       dbNodes,
		pool:        pool,
	}, err
}

func (r *dockerResources) Start() {
	// noop as docker containers are expected to be started before attaching.
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

func (r *dockerResources) Nodes() resources.Nodes             { return r.nodes }
func (r *dockerResources) Coordinator() resources.Coordinator { return r.coordinator }
func (r *dockerResources) Aggregators() resources.Aggregators { return nil }
