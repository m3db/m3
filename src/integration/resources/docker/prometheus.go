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

package docker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/prometheus/common/model"

	"github.com/m3db/m3/src/integration/resources"
	"github.com/m3db/m3/src/x/instrument"
)

// Prometheus is a docker-backed instantiation of Prometheus.
type Prometheus struct {
	pool      *dockertest.Pool
	pathToCfg string
	iOpts     instrument.Options

	resource *Resource
}

// PrometheusOptions contains the options for
// spinning up docker container running Prometheus
type PrometheusOptions struct {
	// Pool is the connection to the docker API
	Pool *dockertest.Pool
	// PathToCfg contains the path to the prometheus.yml configuration
	// file to be used on startup.
	PathToCfg string
	// InstrumentOptions are the instrument.Options to use when
	// creating the resource.
	InstrumentOptions instrument.Options
}

// NewPrometheus creates a new docker-backed Prometheus
// that implements the resources.ExternalResources interface.
func NewPrometheus(opts PrometheusOptions) resources.ExternalResources {
	if opts.InstrumentOptions == nil {
		opts.InstrumentOptions = instrument.NewOptions()
	}
	return &Prometheus{
		pool:      opts.Pool,
		pathToCfg: opts.PathToCfg,
		iOpts:     opts.InstrumentOptions,
	}
}

// Setup is a method that setups up the prometheus instance.
func (p *Prometheus) Setup() error {
	if p.resource != nil {
		return errors.New("prometheus already setup. must close resource " +
			"before attempting to setup again")
	}

	if err := SetupNetwork(p.pool); err != nil {
		return err
	}

	res, err := NewDockerResource(p.pool, ResourceOptions{
		ContainerName: "prometheus",
		Image: Image{
			Name: "prom/prometheus",
			Tag:  "latest",
		},
		PortList: []int{9090},
		Mounts: []string{
			fmt.Sprintf("%s:/etc/prometheus/prometheus.yml", p.pathToCfg),
		},
		InstrumentOpts: p.iOpts,
	})
	if err != nil {
		return err
	}

	p.resource = res

	return p.waitForHealthy()
}

func (p *Prometheus) waitForHealthy() error {
	return resources.Retry(func() error {
		req, err := http.NewRequestWithContext(
			context.Background(),
			http.MethodGet,
			"http://0.0.0.0:9090/-/ready",
			nil,
		)
		if err != nil {
			return err
		}

		client := http.Client{}
		res, _ := client.Do(req)
		if res != nil {
			_ = res.Body.Close()

			if res.StatusCode == http.StatusOK {
				return nil
			}
		}

		return errors.New("prometheus not ready")
	})
}

// PrometheusQueryRequest contains the parameters for making a query request.
type PrometheusQueryRequest struct {
	// Query is the prometheus query to execute
	Query string
	// Time is the time to execute the query at
	Time time.Time
}

// String converts the query request into a string suitable for use
// in the url params or request body
func (p *PrometheusQueryRequest) String() string {
	str := fmt.Sprintf("query=%v", p.Query)

	if !p.Time.IsZero() {
		str += fmt.Sprintf("&time=%v", p.Time.Unix())
	}

	return str
}

// Query executes a query request against the prometheus resource.
func (p *Prometheus) Query(req PrometheusQueryRequest) (model.Vector, error) {
	if p.resource.Closed() {
		return nil, errClosed
	}

	r, err := http.NewRequestWithContext(
		context.Background(),
		http.MethodGet,
		fmt.Sprintf("http://0.0.0.0:9090/api/v1/query?%s", req.String()),
		nil,
	)
	if err != nil {
		return nil, err
	}

	client := http.Client{}
	res, err := client.Do(r)
	if err != nil {
		return nil, err
	}
	defer res.Body.Close()

	body, err := ioutil.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	if res.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("non-200 status code received. "+
			"status=%v responseBody=%v", res.StatusCode, string(body))
	}

	var parsedResp jsonInstantQueryResponse
	if err := json.Unmarshal(body, &parsedResp); err != nil {
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

// Close cleans up the prometheus instance.
func (p *Prometheus) Close() error {
	if p.resource.Closed() {
		return errClosed
	}

	if err := p.resource.Close(); err != nil {
		return err
	}

	p.resource = nil

	return nil
}
