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
	"bytes"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/golang/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	dockertest "github.com/ory/dockertest"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

const (
	coordinatorDockerfile = "./m3coordinator.Dockerfile"
)

const (
	defaultCoordinatorSource     = "coordinator"
	defaultCoordinatorName       = "coord01"
	defaultCoordinatorDockerfile = "./m3coordinator.Dockerfile"
	defaultCoordinatorPort       = "7201/tcp"
)

func zapMethod(s string) zapcore.Field { return zap.String("method", s) }

var (
	defaultCoordinatorList = []int{7201, 7203, 7204}

	defaultCoordinatorOptions = dockerResourceOptions{
		source:        defaultCoordinatorSource,
		containerName: defaultCoordinatorName,
		dockerFile:    defaultCoordinatorDockerfile,
		defaultPort:   defaultCoordinatorPort,
		portList:      defaultCoordinatorList,
	}
)

// Coordinator is a wrapper for a coordinator. It provides a wrapper on HTTP
// endpoints that expose cluster management APIs as well as read and write
// endpoints for series data.
// TODO: consider having this work on underlying structures.
type Coordinator interface {
	Admin
}

// Admin is a wrapper for admin functions.
type Admin interface {
	// GetNamespace gets namespaces.
	GetNamespace() (admin.NamespaceGetResponse, error)
	// WaitForNamespace blocks until the given namespace is enabled.
	// NB: if the name string is empty, this will instead
	// check for a successful response.
	WaitForNamespace(name string) error
	// CreateNamespace creates a namespace.
	// CreateNamespace(admin.DatabaseCreateRequest) (admin.DatabaseCreateResponse, error)
	// CreateDatabase creates a database.
	CreateDatabase(admin.DatabaseCreateRequest) (admin.DatabaseCreateResponse, error)
	// GetPlacement gets placements.
	GetPlacement() (admin.PlacementGetResponse, error)
	// WaitForPlacements blocks until the given placement IDs are present.
	WaitForPlacements(ids []string) error
	// GetPlacementWithID gets placements for a certain ID.
	// GetPlacementWithID(id string) (admin.PlacementGetResponse, error)
	// Close closes the wrapper and releases any held resources, including
	// deleting docker containers.
	Close() error
}

type coordinator struct {
	resource *dockerResource
}

func newDockerHTTPCoordinator(
	pool *dockertest.Pool,
	opts dockerResourceOptions,
) (Coordinator, error) {
	opts = opts.withDefaults(defaultCoordinatorOptions)
	opts.mounts = []string{setupMount("/etc/m3coordinator/")}

	resource, err := newDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	return &coordinator{
		resource: resource,
	}, nil
}

func toResponse(
	resp *http.Response,
	response proto.Message,
	logger *zap.Logger,
) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	err = jsonpb.Unmarshal(bytes.NewReader(b), response)
	defer resp.Body.Close()

	if err != nil {
		logger.Error("unable to unmarshal response",
			zap.Error(err),
			zap.Any("response", response))
		return err
	}

	return nil
}

func (c *coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	url := fmt.Sprintf("%s/api/v1/namespace", c.resource.baseURL)
	logger := c.resource.logger.With(
		zapMethod("getNamespace"), zap.String("url", url))

	resp, err := http.Get(url)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return admin.NamespaceGetResponse{}, err
	}

	var response admin.NamespaceGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.NamespaceGetResponse{}, err
	}

	return response, nil
}

func (c *coordinator) GetPlacement() (admin.PlacementGetResponse, error) {
	if c.resource.closed {
		return admin.PlacementGetResponse{}, errClosed
	}

	url := fmt.Sprintf("%s/api/v1/placement", c.resource.baseURL)
	logger := c.resource.logger.With(
		zapMethod("getPlacement"), zap.String("url", url))

	resp, err := http.Get(url)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return admin.PlacementGetResponse{}, err
	}

	var response admin.PlacementGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.PlacementGetResponse{}, err
	}

	return response, nil
}

func (c *coordinator) WaitForNamespace(name string) error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("waitForNamespace"))
	return c.resource.pool.Retry(func() error {
		ns, err := c.GetNamespace()
		if err != nil {
			return err
		}

		// If no name passed in, instad just check for success.
		if len(name) == 0 {
			return nil
		}

		nss := ns.GetRegistry().GetNamespaces()
		namespace, found := nss[name]
		if !found {
			err := fmt.Errorf("no namespace with name %s", name)
			logger.Error("could not get namespace", zap.Error(err))
			return err
		}

		enabled := namespace.GetIndexOptions().GetEnabled()
		if !enabled {
			err := fmt.Errorf("namespace %s not enabled", name)
			logger.Error("namespace not enabled", zap.Error(err))
			return err
		}

		logger.Info("namespace ready", zap.String("namespace", name))
		return nil
	})
}

func (c *coordinator) WaitForPlacements(
	ids []string,
) error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("waitForPlacement"))
	return c.resource.pool.Retry(func() error {
		placement, err := c.GetPlacement()
		if err != nil {
			logger.Error("retrying get placement", zap.Error(err))
			return err
		}

		logger.Info("got placement", zap.Any("placement", placement))
		instances := placement.GetPlacement().GetInstances()
		for _, id := range ids {
			placement, found := instances[id]
			if !found {
				err = fmt.Errorf("no instance with id %s", id)
				logger.Error("could not get instance", zap.Error(err))
				return err
			}

			if pID := placement.GetId(); pID != id {
				err = fmt.Errorf("id mismatch: instance(%s) != placement(%s)", id, pID)
				logger.Error("could not get instance", zap.Error(err))
				return err
			}
		}

		logger.Info("instances ready")
		return nil
	})
}

func (c *coordinator) CreateDatabase(
	addRequest admin.DatabaseCreateRequest,
) (admin.DatabaseCreateResponse, error) {
	if c.resource.closed {
		return admin.DatabaseCreateResponse{}, errClosed
	}

	url := fmt.Sprintf("%s/api/v1/database/create", c.resource.baseURL)
	logger := c.resource.logger.With(
		zapMethod("createDatabase"), zap.String("request", addRequest.String()))

	b, err := json.Marshal(addRequest)
	if err != nil {
		logger.Error("failed to marshal", zap.Error(err))
		return admin.DatabaseCreateResponse{}, err
	}

	resp, err := http.Post(url, "application/json", bytes.NewReader(b))
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return admin.DatabaseCreateResponse{}, err
	}

	var response admin.DatabaseCreateResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.DatabaseCreateResponse{}, err
	}

	return response, nil
}

func (c *coordinator) Close() error {
	return c.resource.close()
}
