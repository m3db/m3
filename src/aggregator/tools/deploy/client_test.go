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
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"testing"

	"github.com/m3db/m3/src/aggregator/aggregator"
	httpserver "github.com/m3db/m3/src/aggregator/server/http"

	"github.com/stretchr/testify/require"
)

var (
	testInstanceID = "testInstanceID"
)

func TestIsHealthyRequestError(t *testing.T) {
	errRequest := errors.New("request error")
	c := NewAggregatorClient(nil).(*client)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return nil, errRequest }
	require.Equal(t, errRequest, c.IsHealthy(testInstanceID))
}

func TestIsHealthyResponseStatusNotOK(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusServiceUnavailable, nil)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.IsHealthy(testInstanceID))
}

func TestIsHealthyResponseUnmarshalError(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusOK, 0)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.IsHealthy(testInstanceID))
}

func TestIsHealthyResponseWithErrorMessage(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	payload := httpserver.NewResponse()
	payload.Error = "some error occurred"
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.IsHealthy(testInstanceID))
}

func TestIsHealthySuccess(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	payload := httpserver.NewResponse()
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.NoError(t, c.IsHealthy(testInstanceID))
}

func TestStatusRequestError(t *testing.T) {
	errRequest := errors.New("request error")
	c := NewAggregatorClient(nil).(*client)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return nil, errRequest }
	_, err := c.Status(testInstanceID)
	require.Equal(t, errRequest, err)
}

func TestStatusResponseStatusNotOK(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusServiceUnavailable, nil)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	_, err := c.Status(testInstanceID)
	require.Error(t, err)
}

func TestStatusResponseUnmarshalError(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusOK, 0)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	_, err := c.Status(testInstanceID)
	require.Error(t, err)
}

func TestStatusResponseWithErrorMessage(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	payload := httpserver.NewStatusResponse()
	payload.Error = "some error occurred"
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	_, err := c.Status(testInstanceID)
	require.Error(t, err)
}

func TestStatusSuccess(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	expected := aggregator.RuntimeStatus{
		FlushStatus: aggregator.FlushStatus{
			ElectionState: aggregator.LeaderState,
			CanLead:       true,
		},
	}
	payload := httpserver.NewStatusResponse()
	payload.Status = expected
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	status, err := c.Status(testInstanceID)
	require.NoError(t, err)
	require.Equal(t, expected, status)
}

func TestResignRequestError(t *testing.T) {
	errRequest := errors.New("request error")
	c := NewAggregatorClient(nil).(*client)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return nil, errRequest }
	require.Equal(t, errRequest, c.Resign(testInstanceID))
}

func TestResignResponseStatusNotOK(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusServiceUnavailable, nil)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.Resign(testInstanceID))
}

func TestResignResponseUnmarshalError(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	response := generateTestResponse(t, http.StatusOK, 0)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.Resign(testInstanceID))
}

func TestResignResponseWithErrorMessage(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	payload := httpserver.NewResponse()
	payload.Error = "some error occurred"
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.Error(t, c.Resign(testInstanceID))
}

func TestResignSuccess(t *testing.T) {
	c := NewAggregatorClient(nil).(*client)
	payload := httpserver.NewResponse()
	response := generateTestResponse(t, http.StatusOK, payload)
	c.doRequestFn = func(*http.Request) (*http.Response, error) { return response, nil }
	require.NoError(t, c.Resign(testInstanceID))
}

func generateTestResponse(t *testing.T, statusCode int, payload interface{}) *http.Response {
	response := &http.Response{}
	response.StatusCode = statusCode
	if payload == nil {
		response.Body = ioutil.NopCloser(bytes.NewBuffer(nil))
		return response
	}
	b, err := json.Marshal(payload)
	require.NoError(t, err)
	r := ioutil.NopCloser(bytes.NewBuffer(b))
	response.Body = r
	return response
}
