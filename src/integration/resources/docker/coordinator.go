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
	"bytes"
	"context"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"time"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/ory/dockertest/v3"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
)

const (
	defaultCoordinatorSource = "coordinator"
	defaultCoordinatorName   = "coord01"
)

var (
	defaultCoordinatorList = []int{7201, 7203, 7204}

	defaultCoordinatorOptions = dockerResourceOptions{
		source:        defaultCoordinatorSource,
		containerName: defaultCoordinatorName,
		portList:      defaultCoordinatorList,
	}
)

type coordinator struct {
	resource *dockerResource
}

func newDockerHTTPCoordinator(
	pool *dockertest.Pool,
	opts dockerResourceOptions,
) (resources.Coordinator, error) {
	opts = opts.withDefaults(defaultCoordinatorOptions)
	opts.mounts = []string{"/etc/m3coordinator/"}

	resource, err := newDockerResource(pool, opts)
	if err != nil {
		return nil, err
	}

	return &coordinator{
		resource: resource,
	}, nil
}

func (c *coordinator) GetNamespace() (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	url := c.resource.getURL(7201, "api/v1/services/m3db/namespace")
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

	url := c.resource.getURL(7201, "api/v1/services/m3db/placement")
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

func (c *coordinator) WaitForInstances(
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

func (c *coordinator) WaitForShardsReady() error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("waitForShards"))
	return c.resource.pool.Retry(func() error {
		placement, err := c.GetPlacement()
		if err != nil {
			logger.Error("retrying get placement", zap.Error(err))
			return err
		}

		for _, instance := range placement.Placement.Instances {
			for _, shard := range instance.Shards {
				if shard.State == placementpb.ShardState_INITIALIZING {
					err = fmt.Errorf("at least shard %d of dbnode %s still initializing", shard.Id, instance.Id)
					logger.Error("shards still are initializing", zap.Error(err))
					return err
				}
			}
		}
		return nil
	})
}

func (c *coordinator) CreateDatabase(
	addRequest admin.DatabaseCreateRequest,
) (admin.DatabaseCreateResponse, error) {
	if c.resource.closed {
		return admin.DatabaseCreateResponse{}, errClosed
	}

	url := c.resource.getURL(7201, "api/v1/database/create")
	logger := c.resource.logger.With(
		zapMethod("createDatabase"), zap.String("url", url),
		zap.String("request", addRequest.String()))

	resp, err := makeRequest(logger, url, http.MethodPost, &addRequest)
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return admin.DatabaseCreateResponse{}, err
	}

	var response admin.DatabaseCreateResponse
	if err := toResponse(resp, &response, logger); err != nil {
		logger.Error("failed response", zap.Error(err))
		return admin.DatabaseCreateResponse{}, err
	}

	if err = c.setNamespaceReady(addRequest.NamespaceName); err != nil {
		logger.Error("failed to set namespace to ready state",
			zap.Error(err),
			zap.String("namespace", addRequest.NamespaceName),
		)
		return response, err
	}

	logger.Info("created database")
	return response, nil
}

func (c *coordinator) AddNamespace(
	addRequest admin.NamespaceAddRequest,
) (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	url := c.resource.getURL(7201, "api/v1/services/m3db/namespace")
	logger := c.resource.logger.With(
		zapMethod("addNamespace"), zap.String("url", url),
		zap.String("request", addRequest.String()))

	resp, err := makeRequest(logger, url, http.MethodPost, &addRequest)
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return admin.NamespaceGetResponse{}, err
	}

	var response admin.NamespaceGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.NamespaceGetResponse{}, err
	}

	if err = c.setNamespaceReady(addRequest.Name); err != nil {
		logger.Error("failed to set namespace to ready state", zap.Error(err), zap.String("namespace", addRequest.Name))
		return response, err
	}

	return response, nil
}

func (c *coordinator) UpdateNamespace(
	req admin.NamespaceUpdateRequest,
) (admin.NamespaceGetResponse, error) {
	if c.resource.closed {
		return admin.NamespaceGetResponse{}, errClosed
	}

	url := c.resource.getURL(7201, "api/v1/services/m3db/namespace")
	logger := c.resource.logger.With(
		zapMethod("updateNamespace"), zap.String("url", url),
		zap.String("request", req.String()))

	resp, err := makeRequest(logger, url, http.MethodPut, &req)
	if err != nil {
		logger.Error("failed to update namespace", zap.Error(err))
		return admin.NamespaceGetResponse{}, err
	}

	var response admin.NamespaceGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.NamespaceGetResponse{}, err
	}

	return response, nil
}

func (c *coordinator) setNamespaceReady(name string) error {
	url := c.resource.getURL(7201, "api/v1/services/m3db/namespace/ready")
	logger := c.resource.logger.With(
		zapMethod("setNamespaceReady"), zap.String("url", url),
		zap.String("namespace", name))

	_, err := makeRequest(logger, url, http.MethodPost, // nolint: bodyclose
		&admin.NamespaceReadyRequest{
			Name:  name,
			Force: true,
		})
	return err
}

func (c *coordinator) DeleteNamespace(namespaceID string) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.getURL(7201, "api/v1/services/m3db/namespace/"+namespaceID)
	logger := c.resource.logger.With(zapMethod("deleteNamespace"), zap.String("url", url))

	if _, err := makeRequest(logger, url, http.MethodDelete, nil); err != nil { // nolint: bodyclose
		logger.Error("failed to delete namespace", zap.Error(err))
		return err
	}
	return nil
}

func (c *coordinator) WriteCarbon(
	port int, metric string, v float64, t time.Time,
) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.resource.GetHostPort(fmt.Sprintf("%d/tcp", port))
	logger := c.resource.logger.With(
		zapMethod("writeCarbon"), zap.String("url", url),
		zap.String("at time", time.Now().String()),
		zap.String("at ts", t.String()))

	con, err := net.Dial("tcp", url)
	if err != nil {
		logger.Error("could not dial", zap.Error(err))
		return err
	}

	write := fmt.Sprintf("%s %f %d", metric, v, t.Unix())
	logger.Info("writing", zap.String("metric", write))
	n, err := con.Write([]byte(write))
	if err != nil {
		logger.Error("could not write", zap.Error(err))
	}

	if n != len(write) {
		err := fmt.Errorf("wrote %d, wanted %d", n, len(write))
		logger.Error("write failure", zap.Error(err))
		return err
	}

	logger.Info("write success", zap.Int("bytes written", n))
	return con.Close()
	// return nil
}

func (c *coordinator) WriteProm(name string, tags map[string]string, samples []prompb.Sample) error {
	if c.resource.closed {
		return errClosed
	}

	var (
		url       = c.resource.getURL(7201, "api/v1/prom/remote/write")
		reqLabels = []prompb.Label{{Name: []byte(model.MetricNameLabel), Value: []byte(name)}}
	)

	for tag, value := range tags {
		reqLabels = append(reqLabels, prompb.Label{
			Name:  []byte(tag),
			Value: []byte(value),
		})
	}
	writeRequest := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  reqLabels,
				Samples: samples,
			},
		},
	}

	logger := c.resource.logger.With(
		zapMethod("createDatabase"), zap.String("url", url),
		zap.String("request", writeRequest.String()))

	body, err := proto.Marshal(&writeRequest)
	if err != nil {
		logger.Error("failed marshaling request message", zap.Error(err))
		return err
	}
	data := bytes.NewBuffer(snappy.Encode(nil, body))

	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, data)
	if err != nil {
		logger.Error("failed constructing request", zap.Error(err))
		return err
	}
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("failed making a request", zap.Error(err))
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	return nil
}

func makeRequest(logger *zap.Logger, url string, method string, body proto.Message) (*http.Response, error) {
	data := bytes.NewBuffer(nil)
	if body != nil {
		if err := (&jsonpb.Marshaler{}).Marshal(data, body); err != nil {
			logger.Error("failed to marshal", zap.Error(err))

			return nil, fmt.Errorf("failed to marshal: %w", err)
		}
	}

	req, err := http.NewRequestWithContext(context.Background(), method, url, data)
	if err != nil {
		logger.Error("failed to construct request", zap.Error(err))

		return nil, fmt.Errorf("failed to construct request: %w", err)
	}

	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	return http.DefaultClient.Do(req)
}

func (c *coordinator) ApplyKVUpdate(update string) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.getURL(7201, "api/v1/kvstore")

	logger := c.resource.logger.With(
		zapMethod("ApplyKVUpdate"), zap.String("url", url),
		zap.String("update", update))

	data := bytes.NewBuffer([]byte(update))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, data)
	if err != nil {
		logger.Error("failed to construct request", zap.Error(err))
		return fmt.Errorf("failed to construct request: %w", err)
	}

	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("failed to apply request", zap.Error(err))
		return fmt.Errorf("failed to apply request: %w", err)
	}

	bs, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("failed to read body", zap.Error(err))
		return fmt.Errorf("failed to read body: %w", err)
	}

	logger.Info("applied KV update", zap.ByteString("response", bs))
	_ = resp.Body.Close()
	return nil
}

func (c *coordinator) query(
	verifier resources.ResponseVerifier, query string, headers map[string][]string,
) error {
	if c.resource.closed {
		return errClosed
	}

	url := c.resource.getURL(7201, query)
	logger := c.resource.logger.With(
		zapMethod("query"), zap.String("url", url), zap.Any("headers", headers))
	logger.Info("running")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	if headers != nil {
		req.Header = headers
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)

	return verifier(resp.StatusCode, resp.Header, string(b), err)
}

func (c *coordinator) RunQuery(
	verifier resources.ResponseVerifier, query string, headers map[string][]string,
) error {
	if c.resource.closed {
		return errClosed
	}

	logger := c.resource.logger.With(zapMethod("runQuery"),
		zap.String("query", query))
	err := c.resource.pool.Retry(func() error {
		err := c.query(verifier, query, headers)
		if err != nil {
			logger.Info("retrying", zap.Error(err))
		}

		return err
	})
	if err != nil {
		logger.Error("failed run", zap.Error(err))
	}

	return err
}

func (c *coordinator) Close() error {
	if c.resource.closed {
		return errClosed
	}

	return c.resource.close()
}
