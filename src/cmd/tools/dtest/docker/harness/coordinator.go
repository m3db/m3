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
	"errors"
	"fmt"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/x/instrument"
	dockertest "github.com/ory/dockertest"
	dc "github.com/ory/dockertest/docker"
	"go.uber.org/zap"
)

const (
	coordinatorDockerfile = "./m3coordinator.Dockerfile"
)

// Coordinator is a wrapper for a coordinator. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Coordinator interface {
	GetNamespace() (admin.NamespaceGetResponse, error)
	AddNamespace() error
	Close() error
}

type coordinator struct {
	baseURL string

	logger *zap.Logger

	resource *dockertest.Resource
	pool     *dockertest.Pool
}

func newDockerHTTPCoordinator(
	pool *dockertest.Pool,
	containerName string,
	iOpts instrument.Options,
	portList ...int,
) (Coordinator, error) {
	logger := iOpts.Logger().With(
		zap.String("source", "coordinator"),
		zap.String("container name", containerName),
	)

	if err := pool.RemoveContainerByName(containerName); err != nil {
		logger.Error("could not remove container from pool", zap.Error(err))
		return nil, err
	}

	if len(portList) == 0 {
		portList = []int{7201, 7203, 7204}
	}

	opts := exposePorts(newOptions(containerName), portList...)
	opts.Mounts = []string{volumeName}

	logger.Info("building container with options", zap.Any("options", opts))
	coord, err := pool.BuildAndRunWithOptions(coordinatorDockerfile, opts,
		func(c *dc.HostConfig) {
			c.NetworkMode = networkName
		})

	if err != nil {
		logger.Error("could not build and run container", zap.Error(err))
		return nil, err
	}

	port := "7201/tcp"
	url := coord.GetHostPort(port)
	if len(url) == 0 {
		err := errors.New("could not get host port for coordinator")
		if purgeErr := pool.Purge(coord); purgeErr != nil {
			logger.Error("could not tear down failed coordinator",
				zap.String("port", port), zap.Error(purgeErr),
				zap.String("base error", err.Error()))
			return nil, purgeErr
		}

		logger.Error("no host port for port", zap.String("port", port))
		return nil, errors.New("could not get host port for coordinator")
	}

	return &coordinator{
		baseURL:  url,
		logger:   logger.With(zap.String("base", url)),
		resource: coord,
		pool:     pool,
	}, nil
}

func (c *coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	url := fmt.Sprintf("%s/api/v1/namespace")
	logger := c.logger.With(
		zap.String("method", "getNamespace"),
		zap.String("url", url),
	)

	resp, err := http.Get(c.baseURL)
	if err != nil {
		logger.Error("failed get", zap.String("url", url), zap.Error(err))
		return admin.NamespaceGetResponse{}, err
	}

	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx", zap.String("url", url),
			zap.Int("status code", resp.StatusCode), zap.String("status", resp.Status))
		return admin.NamespaceGetResponse{}, fmt.Errorf("status code %d", resp.StatusCode)
	}

	var response admin.NamespaceGetResponse
	err = jsonpb.Unmarshal(resp.Body, &response)
	if closeErr := resp.Body.Close(); closeErr != nil {
		// NB: just log here; no reason to fail.
		logger.Error("could not close body", zap.Error(closeErr))
	}

	if err != nil {
		logger.Error("unable to unmarshal response", zap.Error(err))
		return response, err
	}

	return response, nil
}

func (c *coordinator) AddNamespace() error {
	return nil
}

func (c *coordinator) Close() error {
	return nil
}
