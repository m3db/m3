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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/prometheus/common/model"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placementhandler"
	"github.com/m3db/m3/src/query/api/v1/handler/graphite"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/topic"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/x/headers"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

var errUnknownServiceType = errors.New("unknown service type")

// RetryFunc is a function that retries the provided
// operation until successful.
type RetryFunc func(op func() error) error

// ZapMethod appends the method as a log field.
func ZapMethod(s string) zapcore.Field { return zap.String("method", s) }

// CoordinatorClient is a client use to invoke API calls
// on a coordinator
type CoordinatorClient struct {
	client    *http.Client
	httpPort  int
	logger    *zap.Logger
	retryFunc RetryFunc
}

// CoordinatorClientOptions are the options for the CoordinatorClient.
type CoordinatorClientOptions struct {
	Client    *http.Client
	HTTPPort  int
	Logger    *zap.Logger
	RetryFunc RetryFunc
}

// NewCoordinatorClient creates a new CoordinatorClient.
func NewCoordinatorClient(opts CoordinatorClientOptions) CoordinatorClient {
	return CoordinatorClient{
		client:    opts.Client,
		httpPort:  opts.HTTPPort,
		logger:    opts.Logger,
		retryFunc: opts.RetryFunc,
	}
}

func (c *CoordinatorClient) makeURL(resource string) string {
	return fmt.Sprintf("http://0.0.0.0:%d/%s", c.httpPort, strings.TrimPrefix(resource, "/"))
}

// GetNamespace gets namespaces.
func (c *CoordinatorClient) GetNamespace() (admin.NamespaceGetResponse, error) {
	url := c.makeURL("api/v1/services/m3db/namespace")
	logger := c.logger.With(
		ZapMethod("getNamespace"), zap.String("url", url))

	//nolint:noctx
	resp, err := c.client.Get(url)
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

// GetPlacement gets placements.
func (c *CoordinatorClient) GetPlacement(opts PlacementRequestOptions) (admin.PlacementGetResponse, error) {
	var handlerurl string
	switch opts.Service {
	case ServiceTypeM3DB:
		handlerurl = placementhandler.M3DBGetURL
	case ServiceTypeM3Aggregator:
		handlerurl = placementhandler.M3AggGetURL
	case ServiceTypeM3Coordinator:
		handlerurl = placementhandler.M3CoordinatorGetURL
	default:
		return admin.PlacementGetResponse{}, errUnknownServiceType
	}
	url := c.makeURL(handlerurl)
	logger := c.logger.With(
		ZapMethod("getPlacement"), zap.String("url", url))

	resp, err := c.makeRequest(logger, url, placementhandler.GetHTTPMethod, nil, placementOptsToMap(opts))
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

// InitPlacement initializes placements.
func (c *CoordinatorClient) InitPlacement(
	opts PlacementRequestOptions,
	initRequest admin.PlacementInitRequest,
) (admin.PlacementGetResponse, error) {
	var handlerurl string
	switch opts.Service {
	case ServiceTypeM3DB:
		handlerurl = placementhandler.M3DBInitURL
	case ServiceTypeM3Aggregator:
		handlerurl = placementhandler.M3AggInitURL
	case ServiceTypeM3Coordinator:
		handlerurl = placementhandler.M3CoordinatorInitURL
	default:
		return admin.PlacementGetResponse{}, errUnknownServiceType
	}
	url := c.makeURL(handlerurl)
	logger := c.logger.With(
		ZapMethod("initPlacement"), zap.String("url", url))

	resp, err := c.makeRequest(logger, url, placementhandler.InitHTTPMethod, &initRequest, placementOptsToMap(opts))
	if err != nil {
		logger.Error("failed init", zap.Error(err))
		return admin.PlacementGetResponse{}, err
	}

	var response admin.PlacementGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		return admin.PlacementGetResponse{}, err
	}

	return response, nil
}

// DeleteAllPlacements deletes all placements for the specified service.
func (c *CoordinatorClient) DeleteAllPlacements(opts PlacementRequestOptions) error {
	var handlerurl string
	switch opts.Service {
	case ServiceTypeM3DB:
		handlerurl = placementhandler.M3DBDeleteAllURL
	case ServiceTypeM3Aggregator:
		handlerurl = placementhandler.M3AggDeleteAllURL
	case ServiceTypeM3Coordinator:
		handlerurl = placementhandler.M3CoordinatorDeleteAllURL
	default:
		return errUnknownServiceType
	}
	url := c.makeURL(handlerurl)
	logger := c.logger.With(
		ZapMethod("deleteAllPlacements"), zap.String("url", url))

	resp, err := c.makeRequest(
		logger, url, placementhandler.DeleteAllHTTPMethod, nil, placementOptsToMap(opts),
	)
	if err != nil {
		logger.Error("failed to delete all placements", zap.Error(err))
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	logger.Info("placements deleted")

	return nil
}

// WaitForNamespace blocks until the given namespace is enabled.
// NB: if the name string is empty, this will instead
// check for a successful response.
func (c *CoordinatorClient) WaitForNamespace(name string) error {
	logger := c.logger.With(ZapMethod("waitForNamespace"))
	return c.retryFunc(func() error {
		ns, err := c.GetNamespace()
		if err != nil {
			return err
		}

		// If no name passed in, instad just check for success.
		if len(name) == 0 {
			return nil
		}

		nss := ns.GetRegistry().GetNamespaces()
		_, found := nss[name]
		if !found {
			err := fmt.Errorf("no namespace with name %s", name)
			logger.Error("could not get namespace", zap.Error(err))
			return err
		}

		logger.Info("namespace ready", zap.String("namespace", name))
		return nil
	})
}

// WaitForInstances blocks until the given instance is available.
func (c *CoordinatorClient) WaitForInstances(
	ids []string,
) error {
	logger := c.logger.With(ZapMethod("waitForPlacement"))
	return c.retryFunc(func() error {
		placement, err := c.GetPlacement(PlacementRequestOptions{Service: ServiceTypeM3DB})
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

// WaitForShardsReady waits until all shards gets ready.
func (c *CoordinatorClient) WaitForShardsReady() error {
	logger := c.logger.With(ZapMethod("waitForShards"))
	return c.retryFunc(func() error {
		placement, err := c.GetPlacement(PlacementRequestOptions{Service: ServiceTypeM3DB})
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

// WaitForClusterReady waits until the cluster is ready to receive reads and writes.
func (c *CoordinatorClient) WaitForClusterReady() error {
	var (
		url    = c.makeURL("ready")
		logger = c.logger.With(ZapMethod("waitForClusterReady"), zap.String("url", url))
	)
	return c.retryFunc(func() error {
		req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
		if err != nil {
			logger.Error("failed to create request", zap.Error(err))
			return err
		}

		resp, err := c.client.Do(req)
		if err != nil {
			logger.Error("failed checking cluster readiness", zap.Error(err))
			return err
		}
		defer resp.Body.Close()

		if resp.StatusCode != 200 {
			err = errors.New("non-200 status code received")

			body, rerr := ioutil.ReadAll(resp.Body)
			if rerr != nil {
				logger.Warn("failed parse response body", zap.Error(rerr))
				body = []byte("")
			}

			logger.Error("failed to check cluster readiness", zap.Error(err),
				zap.String("responseBody", string(body)),
			)
			return err
		}

		logger.Info("cluster ready to receive reads and writes")

		return nil
	})
}

// CreateDatabase creates a database.
func (c *CoordinatorClient) CreateDatabase(
	addRequest admin.DatabaseCreateRequest,
) (admin.DatabaseCreateResponse, error) {
	url := c.makeURL("api/v1/database/create")
	logger := c.logger.With(
		ZapMethod("createDatabase"), zap.String("url", url),
		zap.String("request", addRequest.String()))

	resp, err := c.makeRequest(logger, url, http.MethodPost, &addRequest, nil)
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

// AddNamespace adds a namespace.
func (c *CoordinatorClient) AddNamespace(
	addRequest admin.NamespaceAddRequest,
) (admin.NamespaceGetResponse, error) {
	url := c.makeURL("api/v1/services/m3db/namespace")
	logger := c.logger.With(
		ZapMethod("addNamespace"), zap.String("url", url),
		zap.String("request", addRequest.String()))

	resp, err := c.makeRequest(logger, url, http.MethodPost, &addRequest, nil)
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

// UpdateNamespace updates the namespace.
func (c *CoordinatorClient) UpdateNamespace(
	req admin.NamespaceUpdateRequest,
) (admin.NamespaceGetResponse, error) {
	url := c.makeURL("api/v1/services/m3db/namespace")
	logger := c.logger.With(
		ZapMethod("updateNamespace"), zap.String("url", url),
		zap.String("request", req.String()))

	resp, err := c.makeRequest(logger, url, http.MethodPut, &req, nil)
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

func (c *CoordinatorClient) setNamespaceReady(name string) error {
	url := c.makeURL("api/v1/services/m3db/namespace/ready")
	logger := c.logger.With(
		ZapMethod("setNamespaceReady"), zap.String("url", url),
		zap.String("namespace", name))

	_, err := c.makeRequest(logger, url, http.MethodPost, // nolint: bodyclose
		&admin.NamespaceReadyRequest{
			Name:  name,
			Force: true,
		}, nil)
	return err
}

// DeleteNamespace removes the namespace.
func (c *CoordinatorClient) DeleteNamespace(namespaceID string) error {
	url := c.makeURL("api/v1/services/m3db/namespace/" + namespaceID)
	logger := c.logger.With(ZapMethod("deleteNamespace"), zap.String("url", url))

	if _, err := c.makeRequest(logger, url, http.MethodDelete, nil, nil); err != nil { // nolint: bodyclose
		logger.Error("failed to delete namespace", zap.Error(err))
		return err
	}
	return nil
}

//nolint:dupl
// InitM3msgTopic initializes an m3msg topic
func (c *CoordinatorClient) InitM3msgTopic(
	topicOpts M3msgTopicOptions,
	initRequest admin.TopicInitRequest,
) (admin.TopicGetResponse, error) {
	url := c.makeURL(topic.InitURL)
	logger := c.logger.With(
		ZapMethod("initM3msgTopic"),
		zap.String("url", url),
		zap.String("request", initRequest.String()),
		zap.String("topic", fmt.Sprintf("%v", topicOpts)))

	resp, err := c.makeRequest(logger, url, topic.InitHTTPMethod, &initRequest, m3msgTopicOptionsToMap(topicOpts))
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	var response admin.TopicGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		logger.Error("failed response", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	logger.Info("topic initialized")
	return response, nil
}

// GetM3msgTopic fetches an m3msg topic
func (c *CoordinatorClient) GetM3msgTopic(
	topicOpts M3msgTopicOptions,
) (admin.TopicGetResponse, error) {
	url := c.makeURL(topic.GetURL)
	logger := c.logger.With(
		ZapMethod("getM3msgTopic"), zap.String("url", url),
		zap.String("topic", fmt.Sprintf("%v", topicOpts)))

	resp, err := c.makeRequest(logger, url, topic.GetHTTPMethod, nil, m3msgTopicOptionsToMap(topicOpts))
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	var response admin.TopicGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		logger.Error("failed response", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	logger.Info("topic get")
	return response, nil
}

//nolint:dupl
// AddM3msgTopicConsumer adds a consumer service to an m3msg topic
func (c *CoordinatorClient) AddM3msgTopicConsumer(
	topicOpts M3msgTopicOptions,
	addRequest admin.TopicAddRequest,
) (admin.TopicGetResponse, error) {
	url := c.makeURL(topic.AddURL)
	logger := c.logger.With(
		ZapMethod("addM3msgTopicConsumer"),
		zap.String("url", url),
		zap.String("request", addRequest.String()),
		zap.String("topic", fmt.Sprintf("%v", topicOpts)))

	resp, err := c.makeRequest(logger, url, topic.AddHTTPMethod, &addRequest, m3msgTopicOptionsToMap(topicOpts))
	if err != nil {
		logger.Error("failed post", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	var response admin.TopicGetResponse
	if err := toResponse(resp, &response, logger); err != nil {
		logger.Error("failed response", zap.Error(err))
		return admin.TopicGetResponse{}, err
	}

	logger.Info("topic consumer added")
	return response, nil
}

func placementOptsToMap(opts PlacementRequestOptions) map[string]string {
	return map[string]string{
		headers.HeaderClusterEnvironmentName: opts.Env,
		headers.HeaderClusterZoneName:        opts.Zone,
	}
}

func m3msgTopicOptionsToMap(opts M3msgTopicOptions) map[string]string {
	return map[string]string{
		headers.HeaderClusterEnvironmentName: opts.Env,
		headers.HeaderClusterZoneName:        opts.Zone,
		topic.HeaderTopicName:                opts.TopicName,
	}
}

// WriteCarbon writes a carbon metric datapoint at a given time.
func (c *CoordinatorClient) WriteCarbon(
	url string, metric string, v float64, t time.Time,
) error {
	logger := c.logger.With(
		ZapMethod("writeCarbon"), zap.String("url", url),
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
}

// WriteProm writes a prometheus metric. Takes tags/labels as a map for convenience.
func (c *CoordinatorClient) WriteProm(
	name string,
	tags map[string]string,
	samples []prompb.Sample,
	headers Headers,
) error {
	labels := make([]prompb.Label, 0, len(tags))

	for tag, value := range tags {
		labels = append(labels, prompb.Label{
			Name:  []byte(tag),
			Value: []byte(value),
		})
	}

	return c.WritePromWithLabels(name, labels, samples, headers)
}

// WritePromWithLabels writes a prometheus metric. Allows you to provide the labels for the write
// directly instead of conveniently converting them from a map.
func (c *CoordinatorClient) WritePromWithLabels(
	name string,
	labels []prompb.Label,
	samples []prompb.Sample,
	headers Headers,
) error {
	var (
		url       = c.makeURL("api/v1/prom/remote/write")
		reqLabels = []prompb.Label{{Name: []byte(model.MetricNameLabel), Value: []byte(name)}}
	)
	reqLabels = append(reqLabels, labels...)

	writeRequest := prompb.WriteRequest{
		Timeseries: []prompb.TimeSeries{
			{
				Labels:  reqLabels,
				Samples: samples,
			},
		},
	}

	logger := c.logger.With(
		ZapMethod("writeProm"), zap.String("url", url),
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
	for key, vals := range headers {
		for _, val := range vals {
			req.Header.Add(key, val)
		}
	}
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)

	resp, err := c.client.Do(req)
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

func (c *CoordinatorClient) makeRequest(
	logger *zap.Logger,
	url string,
	method string,
	body proto.Message,
	header map[string]string,
) (*http.Response, error) {
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
	for k, v := range header {
		req.Header.Add(k, v)
	}

	return c.client.Do(req)
}

// ApplyKVUpdate applies a KV update.
func (c *CoordinatorClient) ApplyKVUpdate(update string) error {
	url := c.makeURL("api/v1/kvstore")

	logger := c.logger.With(
		ZapMethod("ApplyKVUpdate"), zap.String("url", url),
		zap.String("update", update))

	data := bytes.NewBuffer([]byte(update))
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, data)
	if err != nil {
		logger.Error("failed to construct request", zap.Error(err))
		return fmt.Errorf("failed to construct request: %w", err)
	}

	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeJSON)

	resp, err := c.client.Do(req)
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

func (c *CoordinatorClient) query(
	verifier ResponseVerifier, query string, headers map[string][]string,
) error {
	url := c.makeURL(query)
	logger := c.logger.With(
		ZapMethod("query"), zap.String("url", url), zap.Any("headers", headers))
	logger.Info("running")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return err
	}

	if headers != nil {
		req.Header = headers
	}

	resp, err := c.client.Do(req)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)

	return verifier(resp.StatusCode, resp.Header, string(b), err)
}

// InstantQuery runs an instant query with provided headers
func (c *CoordinatorClient) InstantQuery(req QueryRequest, headers Headers) (model.Vector, error) {
	return c.instantQuery(req, route.QueryURL, headers)
}

// InstantQueryWithEngine runs an instant query with provided headers and the specified
// query engine.
func (c *CoordinatorClient) InstantQueryWithEngine(
	req QueryRequest,
	engine options.QueryEngine,
	headers Headers,
) (model.Vector, error) {
	if engine == options.M3QueryEngine {
		return c.instantQuery(req, native.M3QueryReadInstantURL, headers)
	} else if engine == options.PrometheusEngine {
		return c.instantQuery(req, native.PrometheusReadInstantURL, headers)
	}
	return nil, fmt.Errorf("unknown query engine: %s", engine)
}

func (c *CoordinatorClient) instantQuery(
	req QueryRequest,
	queryRoute string,
	headers Headers,
) (model.Vector, error) {
	queryStr := fmt.Sprintf("%s?query=%s", queryRoute, req.Query)
	if req.Time != nil {
		queryStr = fmt.Sprintf("%s&time=%d", queryStr, req.Time.Unix())
	}

	resp, err := c.runQuery(queryStr, headers)
	if err != nil {
		return nil, err
	}

	var parsedResp jsonInstantQueryResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return nil, err
	}

	return parsedResp.Data.Result, nil
}

type jsonInstantQueryResponse struct {
	Status string
	Data   vectorResult
}

type vectorResult struct {
	ResultType model.ValueType
	Result     model.Vector
}

// RangeQuery runs a range query with provided headers
func (c *CoordinatorClient) RangeQuery(
	req RangeQueryRequest,
	headers Headers,
) (model.Matrix, error) {
	return c.rangeQuery(req, route.QueryRangeURL, headers)
}

// RangeQueryWithEngine runs a range query with provided headers and the specified
// query engine.
func (c *CoordinatorClient) RangeQueryWithEngine(
	req RangeQueryRequest,
	engine options.QueryEngine,
	headers Headers,
) (model.Matrix, error) {
	if engine == options.M3QueryEngine {
		return c.rangeQuery(req, native.M3QueryReadURL, headers)
	} else if engine == options.PrometheusEngine {
		return c.rangeQuery(req, native.PrometheusReadURL, headers)
	}
	return nil, fmt.Errorf("unknown query engine: %s", engine)
}

func (c *CoordinatorClient) rangeQuery(
	req RangeQueryRequest,
	queryRoute string,
	headers Headers,
) (model.Matrix, error) {
	if req.Step == 0 {
		req.Step = 15 * time.Second // default step is 15 seconds.
	}
	queryStr := fmt.Sprintf(
		"%s?query=%s&start=%d&end=%d&step=%f",
		queryRoute, req.Query,
		req.Start.Unix(),
		req.End.Unix(),
		req.Step.Seconds(),
	)

	resp, err := c.runQuery(queryStr, headers)
	if err != nil {
		return nil, err
	}

	var parsedResp jsonRangeQueryResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return nil, err
	}

	return parsedResp.Data.Result, nil
}

// LabelNames return matching label names based on the request.
func (c *CoordinatorClient) LabelNames(
	req LabelNamesRequest,
	headers Headers,
) (model.LabelNames, error) {
	urlPathAndQuery := fmt.Sprintf("%s?%s", route.LabelNamesURL, req.String())
	resp, err := c.runQuery(urlPathAndQuery, headers)
	if err != nil {
		return nil, err
	}

	var parsedResp labelResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return nil, err
	}

	labelNames := make(model.LabelNames, 0, len(parsedResp.Data))
	for _, label := range parsedResp.Data {
		labelNames = append(labelNames, model.LabelName(label))
	}

	return labelNames, nil
}

// LabelValues return matching label values based on the request.
func (c *CoordinatorClient) LabelValues(
	req LabelValuesRequest,
	headers Headers,
) (model.LabelValues, error) {
	urlPathAndQuery := fmt.Sprintf("%s?%s",
		path.Join(route.Prefix, "label", req.LabelName, "values"),
		req.String())
	resp, err := c.runQuery(urlPathAndQuery, headers)
	if err != nil {
		return nil, err
	}

	var parsedResp labelResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return nil, err
	}

	labelValues := make(model.LabelValues, 0, len(parsedResp.Data))
	for _, label := range parsedResp.Data {
		labelValues = append(labelValues, model.LabelValue(label))
	}

	return labelValues, nil
}

// Series returns matching series based on the request.
func (c *CoordinatorClient) Series(
	req SeriesRequest,
	headers Headers,
) ([]model.Metric, error) {
	urlPathAndQuery := fmt.Sprintf("%s?%s", route.SeriesMatchURL, req.String())
	resp, err := c.runQuery(urlPathAndQuery, headers)
	if err != nil {
		return nil, err
	}

	var parsedResp seriesResponse
	if err := json.Unmarshal([]byte(resp), &parsedResp); err != nil {
		return nil, err
	}

	series := make([]model.Metric, 0, len(parsedResp.Data))
	for _, labels := range parsedResp.Data {
		labelSet := make(model.LabelSet)
		for name, val := range labels {
			labelSet[model.LabelName(name)] = model.LabelValue(val)
		}
		series = append(series, model.Metric(labelSet))
	}

	return series, nil
}

type jsonRangeQueryResponse struct {
	Status string
	Data   matrixResult
}

type matrixResult struct {
	ResultType model.ValueType
	Result     model.Matrix
}

type labelResponse struct {
	Status string
	Data   []string
}

type seriesResponse struct {
	Status string
	Data   []map[string]string
}

func (c *CoordinatorClient) runQuery(
	query string, headers map[string][]string,
) (string, error) {
	url := c.makeURL(query)
	logger := c.logger.With(
		ZapMethod("query"), zap.String("url", url), zap.Any("headers", headers))
	logger.Info("running")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return "", err
	}

	if headers != nil {
		req.Header = headers
	}

	resp, err := c.client.Do(req)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return "", err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)

	if status := resp.StatusCode; status != http.StatusOK {
		return "", fmt.Errorf("query response status not OK, received %v. error=%v",
			status, string(b))
	}

	if contentType, ok := resp.Header["Content-Type"]; !ok {
		return "", fmt.Errorf("missing Content-Type header")
	} else if len(contentType) != 1 || contentType[0] != "application/json" { //nolint:goconst
		return "", fmt.Errorf("expected json content type, got %v", contentType)
	}

	return string(b), err
}

// RunQuery runs the given query with a given verification function.
func (c *CoordinatorClient) RunQuery(
	verifier ResponseVerifier, query string, headers map[string][]string,
) error {
	logger := c.logger.With(ZapMethod("runQuery"),
		zap.String("query", query))
	err := c.retryFunc(func() error {
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

func toResponse(
	resp *http.Response,
	response proto.Message,
	logger *zap.Logger,
) error {
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		logger.Error("could not read body", zap.Error(err))
		return err
	}

	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		logger.Error("status code not 2xx",
			zap.Int("status code", resp.StatusCode),
			zap.String("status", resp.Status))
		return fmt.Errorf("status code %d", resp.StatusCode)
	}

	err = jsonpb.Unmarshal(bytes.NewReader(b), response)
	if err != nil {
		logger.Error("unable to unmarshal response",
			zap.Error(err),
			zap.Any("response", response))
		return err
	}

	return nil
}

// GraphiteQuery retrieves graphite raw data.
func (c *CoordinatorClient) GraphiteQuery(
	graphiteReq GraphiteQueryRequest,
) ([]Datapoint, error) {
	if graphiteReq.From.IsZero() {
		graphiteReq.From = time.Now().Add(-24 * time.Hour)
	}
	if graphiteReq.Until.IsZero() {
		graphiteReq.Until = time.Now()
	}

	queryStr := fmt.Sprintf(
		"%s?target=%s&from=%d&until=%d",
		graphite.ReadURL, graphiteReq.Target,
		graphiteReq.From.Unix(),
		graphiteReq.Until.Unix(),
	)

	url := c.makeURL(queryStr)
	logger := c.logger.With(
		ZapMethod("graphiteQuery"), zap.String("url", url))
	logger.Info("running")
	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.client.Do(req)
	if err != nil {
		logger.Error("failed get", zap.Error(err))
		return nil, err
	}

	defer resp.Body.Close()
	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	if status := resp.StatusCode; status != http.StatusOK {
		return nil, fmt.Errorf("query response status not OK, received %v %s", status, resp.Status)
	}

	var parsedResp jsonGraphiteQueryResponse
	if err := json.Unmarshal(b, &parsedResp); err != nil {
		return nil, err
	}

	if len(parsedResp) == 0 {
		return nil, nil
	}

	results := make([]Datapoint, 0, len(parsedResp[0].Datapoints))
	for _, dp := range parsedResp[0].Datapoints {
		if len(dp) != 2 {
			return nil, fmt.Errorf("failed to parse response: %s", string(b))
		}

		results = append(results, Datapoint{
			Value:     dp[0],
			Timestamp: int64(*dp[1]),
		})
	}

	return results, nil
}

type jsonGraphiteQueryResponse []series

type series struct {
	Target     string
	Datapoints []tuple
	StepSizeMs int
}

type tuple []*float64
