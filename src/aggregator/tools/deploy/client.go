// Copyright (c) 2017 Uber Technologies, Inc.
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

package deploy

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/aggregator/aggregator"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"
)

// AggregatorClient interacts with aggregator instances via aggregator endpoints.
type AggregatorClient interface {
	// IsHealthy determines whether an instance is healthy.
	IsHealthy(instance string) error

	// Status returns the instance status.
	Status(instance string) (aggregator.RuntimeStatus, error)

	// Resign asks an aggregator instance to give up its current leader role if applicable.
	// The instance however still participates in leader election as a follower after
	// resiganation succeeds.
	Resign(instance string) error
}

type doRequestFn func(*http.Request) (*http.Response, error)

type client struct {
	httpClient *http.Client

	doRequestFn doRequestFn
}

// NewAggregatorClient creates a new aggregator client.
func NewAggregatorClient(httpClient *http.Client) AggregatorClient {
	return &client{
		httpClient:  httpClient,
		doRequestFn: httpClient.Do,
	}
}

func (c *client) IsHealthy(instance string) error {
	response := httpserver.NewResponse()
	return c.doRequest(instance, http.MethodGet, httpserver.HealthPath, &response)
}

func (c *client) Status(instance string) (aggregator.RuntimeStatus, error) {
	var (
		status   aggregator.RuntimeStatus
		response = httpserver.NewStatusResponse()
	)
	err := c.doRequest(instance, http.MethodGet, httpserver.StatusPath, &response)
	if err != nil {
		return status, err
	}
	return response.Status, nil
}

func (c *client) Resign(instance string) error {
	response := httpserver.NewResponse()
	return c.doRequest(instance, http.MethodPost, httpserver.ResignPath, &response)
}

func (c *client) doRequest(
	hostPort, method, path string,
	response interface{},
) error {
	url := fmt.Sprintf("http://%s%s", hostPort, path)
	req, err := http.NewRequest(method, url, nil)
	if err != nil {
		return fmt.Errorf("unable to create request: %v", err)
	}
	req.Header.Add("Content-Type", "application/json")

	// NB(xichen): need to read and discard all the data in the response body
	// before closing the body or otherwise the underlying connections are not
	// eligible for reuse.
	resp, err := c.doRequestFn(req)
	if err != nil {
		return err
	}
	defer func() {
		io.Copy(ioutil.Discard, resp.Body)
		resp.Body.Close()
	}()

	statusCode := resp.StatusCode
	if statusCode != http.StatusOK {
		return fmt.Errorf("response code is %d instead of 200", statusCode)
	}

	b, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return fmt.Errorf("unable to read response body: %v", err)
	}

	if err := json.Unmarshal(b, response); err != nil {
		return fmt.Errorf("unable to unmarshal response body: %v", err)
	}

	var responseErr string
	switch t := response.(type) {
	case *httpserver.Response:
		responseErr = t.Error
	case *httpserver.StatusResponse:
		responseErr = t.Error
	}
	if responseErr != "" {
		return fmt.Errorf("received error response: %v", response)
	}

	return nil
}
