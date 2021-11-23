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

package docker

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/prometheus/common/model"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
)

const (
	defaultCoordinatorSource = "coordinator"
	defaultCoordinatorName   = "coord01"
)

var (
	defaultCoordinatorList = []int{7201, 7203, 7204}

	defaultCoordinatorOptions = ResourceOptions{
		Source:        defaultCoordinatorSource,
		ContainerName: defaultCoordinatorName,
		PortList:      defaultCoordinatorList,
	}
)

type coordinator struct {
	resource *Resource
	client   resources.CoordinatorClient
}

func newDockerHTTPCoordinator(
	pool *dockertest.Pool,
	opts ResourceOptions,
) (resources.Coordinator, error) {
	opts = opts.withDefaults(defaultCoordinatorOptions)
	opts.TmpfsMounts = []string{"/etc/m3coordinator/"}

	resource, err := NewDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	return &coordinator{
		resource: resource,
		client: resources.NewCoordinatorClient(resources.CoordinatorClientOptions{
			Client:    http.DefaultClient,
			HTTPPort:  7201,
			Logger:    resource.logger,
			RetryFunc: resource.pool.Retry,
		}),
	}, nil
}

func (c *coordinator) HostDetails() (*resources.InstanceInfo, error) {
	// TODO: add implementation
	return nil, errors.New("not implemented")
}

func (c *coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	return c.client.GetNamespace()
}

func (c *coordinator) GetPlacement(
	opts resources.PlacementRequestOptions,
) (admin.PlacementGetResponse, error) {
	if c.resource.closed {
		return admin.PlacementGetResponse{}, errClosed
	}

	return c.client.GetPlacement(opts)
}

func (c *coordinator) InitPlacement(
	opts resources.PlacementRequestOptions,
	req admin.PlacementInitRequest,
) (admin.PlacementGetResponse, error) {
	if c.resource.closed {
		return admin.PlacementGetResponse{}, errClosed
	}

	return c.client.InitPlacement(opts, req)
}

func (c *coordinator) DeleteAllPlacements(
	opts resources.PlacementRequestOptions,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.DeleteAllPlacements(opts)
}

func (c *coordinator) WaitForNamespace(name string) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WaitForNamespace(name)
}

func (c *coordinator) WaitForInstances(
	ids []string,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WaitForInstances(ids)
}

func (c *coordinator) WaitForShardsReady() error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WaitForShardsReady()
}

func (c *coordinator) WaitForClusterReady() error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WaitForClusterReady()
}

func (c *coordinator) CreateDatabase(
	addRequest admin.DatabaseCreateRequest,
) (admin.DatabaseCreateResponse, error) {
	if c.resource.closed {
		return admin.DatabaseCreateResponse{}, errClosed
	}

	return c.client.CreateDatabase(addRequest)
}

func (c *coordinator) AddNamespace(
	addRequest admin.NamespaceAddRequest,
) (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	return c.client.AddNamespace(addRequest)
}

func (c *coordinator) UpdateNamespace(
	req admin.NamespaceUpdateRequest,
) (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	return c.client.UpdateNamespace(req)
}

func (c *coordinator) DeleteNamespace(namespaceID string) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.DeleteNamespace(namespaceID)
}

func (c *coordinator) WriteCarbon(
	port int, metric string, v float64, t time.Time,
) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.resource.GetHostPort(fmt.Sprintf("%d/tcp", port))

	return c.client.WriteCarbon(url, metric, v, t)
}

func (c *coordinator) WriteProm(
	name string,
	tags map[string]string,
	samples []prompb.Sample,
	headers resources.Headers,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WriteProm(name, tags, samples, headers)
}

func (c *coordinator) WritePromWithLabels(
	name string,
	labels []prompb.Label,
	samples []prompb.Sample,
	headers resources.Headers,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.WritePromWithLabels(name, labels, samples, headers)
}

func (c *coordinator) ApplyKVUpdate(update string) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.ApplyKVUpdate(update)
}

func (c *coordinator) RunQuery(
	verifier resources.ResponseVerifier,
	query string,
	headers resources.Headers,
) error {
	if c.resource.closed {
		return errClosed
	}

	return c.client.RunQuery(verifier, query, headers)
}

func (c *coordinator) InstantQuery(
	req resources.QueryRequest,
	headers resources.Headers,
) (model.Vector, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.InstantQuery(req, headers)
}

// InstantQueryWithEngine runs an instant query with provided headers and the specified
// query engine.
func (c *coordinator) InstantQueryWithEngine(
	req resources.QueryRequest,
	engine options.QueryEngine,
	headers resources.Headers,
) (model.Vector, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.InstantQueryWithEngine(req, engine, headers)
}

// RangeQuery runs a range query with provided headers
func (c *coordinator) RangeQuery(
	req resources.RangeQueryRequest,
	headers resources.Headers,
) (model.Matrix, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.RangeQuery(req, headers)
}

// GraphiteQuery retrieves graphite raw data.
func (c *coordinator) GraphiteQuery(req resources.GraphiteQueryRequest) ([]resources.Datapoint, error) {
	return c.client.GraphiteQuery(req)
}

// RangeQueryWithEngine runs a range query with provided headers and the specified
// query engine.
func (c *coordinator) RangeQueryWithEngine(
	req resources.RangeQueryRequest,
	engine options.QueryEngine,
	headers resources.Headers,
) (model.Matrix, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.RangeQueryWithEngine(req, engine, headers)
}

// LabelNames return matching label names based on the request.
func (c *coordinator) LabelNames(
	req resources.LabelNamesRequest,
	headers resources.Headers,
) (model.LabelNames, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.LabelNames(req, headers)
}

// LabelValues returns matching label values based on the request.
func (c *coordinator) LabelValues(
	req resources.LabelValuesRequest,
	headers resources.Headers,
) (model.LabelValues, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.LabelValues(req, headers)
}

// Series returns matching series based on the request.
func (c *coordinator) Series(
	req resources.SeriesRequest,
	headers resources.Headers,
) ([]model.Metric, error) {
	if c.resource.closed {
		return nil, errClosed
	}
	return c.client.Series(req, headers)
}

func (c *coordinator) Close() error {
	if c.resource.closed {
		return errClosed
	}

	return c.resource.Close()
}

func (c *coordinator) InitM3msgTopic(
	opts resources.M3msgTopicOptions,
	req admin.TopicInitRequest,
) (admin.TopicGetResponse, error) {
	return c.client.InitM3msgTopic(opts, req)
}

func (c *coordinator) GetM3msgTopic(
	opts resources.M3msgTopicOptions,
) (admin.TopicGetResponse, error) {
	return c.client.GetM3msgTopic(opts)
}

func (c *coordinator) AddM3msgTopicConsumer(
	opts resources.M3msgTopicOptions,
	req admin.TopicAddRequest,
) (admin.TopicGetResponse, error) {
	return c.client.AddM3msgTopicConsumer(opts, req)
}
