// Copyright (c) 2018 Uber Technologies, Inc.
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

package validator

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func newBodyWithMismatch() io.Reader {
	return strings.NewReader(
		`
		{
			"input": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								],
								[
									1543435003.200,
									"0.0122029"
								]
							]
						}
					]
				}
			},
			"results": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								],
								[
									1543435003.200,
									"0.0122039"
								]
							]
						}
					]
				}
			}
		}
		`,
	)
}

func newBodyWithNumM3dpMismatch() io.Reader {
	return strings.NewReader(
		`
		{
			"input": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								],
								[
									1543435003.200,
									"0.0122029"
								]
							]
						}
					]
				}
			},
			"results": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								]
							]
						}
					]
				}
			}
		}
		`,
	)
}

func newBodyWithNumPromdpMismatch() io.Reader {
	return strings.NewReader(
		`
		{
			"input": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								],
								[
									1543435003.200,
									"0.0122029"
								]
							]
						}
					]
				}
			},
			"results": {
				"status": "success",
				"data": {
					"resultType": "matrix",
					"result": [
						{
							"metric": {
								"__name__": "go_gc_duration_seconds",
								"instance": "localhost:9090",
								"job": "prometheus",
								"quantile": "1"
							},
							"values": [
								[
									1543434961.200,
									"0.0032431"
								],
								[
									1543434975.200,
									"0.0032431"
								],
								[
									1543434989.200,
									"0.0122029"
								],
								[
									1543435003.200,
									"0.0122029"
								],
								[
									1543435017.200,
									"0.05555"
								]
							]
						}
					]
				}
			}
		}
		`,
	)
}

type MismatchesJSON struct {
	Correct        bool `json:"correct"`
	MismatchesList []struct {
		Mismatches []struct {
			Name     string  `json:"name"`
			PromVal  float64 `json:"promVal"`
			PromTime string  `json:"promTime"`
			M3Val    float64 `json:"m3Val"`
			M3Time   string  `json:"m3Time"`
			Err      string  `json:"error"`
		} `json:"mismatches"`
	} `json:"mismatches_list"`
}

func newServer() (*httptest.Server, *PromDebugHandler) {
	logging.InitWithCores(nil)

	mockStorage := mock.NewMockStorage()
	scope := tally.NewTestScope("test", nil)
	debugHandler := NewPromDebugHandler(
		native.NewPromReadHandler(
			executor.NewEngine(mockStorage, tally.NewTestScope("test_engine", nil), cost.NoopChainedEnforcer()),
			models.NewTagOptions(),
			&config.LimitsConfiguration{},
			scope,
		), scope,
	)

	return httptest.NewServer(debugHandler), debugHandler
}

func TestValidateEndpoint(t *testing.T) {
	server, debugHandler := newServer()
	defer server.Close()

	req, _ := http.NewRequest("POST", PromDebugURL+"?start=1543431465&end=1543435005&step=14&query=go_gc_duration_seconds", newBodyWithMismatch())
	recorder := httptest.NewRecorder()
	debugHandler.ServeHTTP(recorder, req)

	var mismatches MismatchesJSON
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &mismatches))
	assert.False(t, mismatches.Correct)
	assert.Len(t, mismatches.MismatchesList, 1)

	mismatchesList := mismatches.MismatchesList[0]
	assert.Len(t, mismatchesList.Mismatches, 1)
	assert.Equal(t, "__name__=go_gc_duration_seconds,instance=localhost:9090,job=prometheus,quantile=1,", mismatchesList.Mismatches[0].Name)
	assert.Equal(t, 0.012203, mismatchesList.Mismatches[0].M3Val)
}

func TestValidateEndpointWithNumM3dpMismatch(t *testing.T) {
	server, debugHandler := newServer()
	defer server.Close()

	req, _ := http.NewRequest("POST", PromDebugURL+"?start=1543431465&end=1543435005&step=14&query=go_gc_duration_seconds", newBodyWithNumM3dpMismatch())
	recorder := httptest.NewRecorder()
	debugHandler.ServeHTTP(recorder, req)

	var mismatches MismatchesJSON
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &mismatches))
	assert.False(t, mismatches.Correct)
	assert.Len(t, mismatches.MismatchesList, 1)

	mismatchesList := mismatches.MismatchesList[0]
	assert.Len(t, mismatchesList.Mismatches, 1)
	assert.Equal(t, "series has extra m3 datapoints", mismatchesList.Mismatches[0].Err)
	assert.Equal(t, 0.012203, mismatchesList.Mismatches[0].M3Val)
}

func TestValidateEndpointWithNumPromdpMismatch(t *testing.T) {
	server, debugHandler := newServer()
	defer server.Close()

	req, _ := http.NewRequest("POST", PromDebugURL+"?start=1543431465&end=1543435005&step=14&query=go_gc_duration_seconds", newBodyWithNumPromdpMismatch())
	recorder := httptest.NewRecorder()
	debugHandler.ServeHTTP(recorder, req)

	var mismatches MismatchesJSON
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &mismatches))
	assert.False(t, mismatches.Correct)
	assert.Len(t, mismatches.MismatchesList, 1)

	mismatchesList := mismatches.MismatchesList[0]
	assert.Len(t, mismatchesList.Mismatches, 1)
	assert.Equal(t, "series has extra prom datapoints", mismatchesList.Mismatches[0].Err)
	assert.Equal(t, 0.05555, mismatchesList.Mismatches[0].PromVal)
}
