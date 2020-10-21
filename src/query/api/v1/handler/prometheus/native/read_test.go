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

package native

import (
	"context"
	"encoding/json"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/x/headers"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRequest(t *testing.T) {
	setup := newTestSetup(&prometheus.TimeoutOpts{
		FetchTimeout: 10 * time.Second,
	}, nil)
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	parsed, err := ParseRequest(req.Context(), req, false, setup.options)
	require.NoError(t, err)
	require.Equal(t, time.Second*10, parsed.Params.Timeout)
	require.Equal(t, time.Second*0, parsed.FetchOpts.Timeout)
	require.Equal(t, 0, parsed.FetchOpts.DocsLimit)
	require.Equal(t, 0, parsed.FetchOpts.SeriesLimit)
	require.Equal(t, false, parsed.FetchOpts.RequireExhaustive)
	require.Equal(t, 0, parsed.QueryOpts.QueryContextOptions.LimitMaxDocs)
	require.Equal(t, 0, parsed.QueryOpts.QueryContextOptions.LimitMaxTimeseries)
	require.Equal(t, false, parsed.QueryOpts.QueryContextOptions.RequireExhaustive)
	require.Nil(t, parsed.QueryOpts.QueryContextOptions.RestrictFetchType)
}

func TestPromReadHandlerRead(t *testing.T) {
	testPromReadHandlerRead(t, block.NewResultMetadata(), "")
	testPromReadHandlerRead(t, buildWarningMeta("foo", "bar"), "foo_bar")
	testPromReadHandlerRead(t, block.ResultMetadata{Exhaustive: false},
		headers.LimitHeaderSeriesLimitApplied)
}

func TestPromReadHandlerWithTimeout(t *testing.T) {
	ctrl := xtest.NewController(t)
	engine := executor.NewMockEngine(ctrl)
	engine.EXPECT().
		Options().
		Return(executor.NewEngineOptions())
	engine.EXPECT().
		ExecuteExpr(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		DoAndReturn(func(ctx context.Context,
			parser parser.Parser,
			opts *executor.QueryOptions,
			fetchOpts *storage.FetchOptions,
			params models.RequestParams,
		) (block.Block, error) {
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			return nil, nil
		})

	setup := newTestSetup(nil, engine)
	promRead := setup.Handlers.read

	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()
	ctx := req.Context()

	r, parseErr := testParseParams(req)
	require.Nil(t, parseErr)
	assert.Equal(t, models.FormatPromQL, r.FormatType)
	r.Timeout = 10 * time.Millisecond
	parsed := ParsedOptions{
		QueryOpts: setup.QueryOpts,
		FetchOpts: setup.FetchOpts,
		Params:    r,
		CancelWatcher: &cancelWatcher{
			delay: r.Timeout * 10,
		},
	}

	_, err := read(ctx, parsed, promRead.opts)
	require.Error(t, err)
	require.Equal(t,
		"context deadline exceeded",
		err.Error())
}

func testPromReadHandlerRead(
	t *testing.T,
	resultMeta block.ResultMetadata,
	ex string,
) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)

	setup := newTestSetup(timeoutOpts, nil)
	promRead := setup.Handlers.read

	seriesMeta := test.NewSeriesMeta("dummy", len(values))
	m := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(0, models.NewTagOptions()),
		ResultMetadata: resultMeta,
	}

	b := test.NewBlockFromValuesWithMetaAndSeriesMeta(m, seriesMeta, values)
	setup.Storage.SetFetchBlocksResult(block.Result{Blocks: []block.Block{b}}, nil)

	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()
	ctx := req.Context()

	r, parseErr := testParseParams(req)
	require.Nil(t, parseErr)
	assert.Equal(t, models.FormatPromQL, r.FormatType)
	parsed := ParsedOptions{
		QueryOpts: setup.QueryOpts,
		FetchOpts: setup.FetchOpts,
		Params:    r,
	}

	result, err := read(ctx, parsed, promRead.opts)
	require.NoError(t, err)
	seriesList := result.Series

	require.Len(t, seriesList, 2)
	s := seriesList[0]

	assert.Equal(t, 5, s.Values().Len())
	for i := 0; i < s.Values().Len(); i++ {
		assert.Equal(t, float64(i), s.Values().ValueAt(i))
	}
}

type M3QLResp []struct {
	Target     string            `json:"target"`
	Tags       map[string]string `json:"tags"`
	Datapoints [][]float64       `json:"datapoints"`
	StepSizeMs int               `json:"step_size_ms"`
}

func TestM3PromReadHandlerRead(t *testing.T) {
	testM3PromReadHandlerRead(t, block.NewResultMetadata(), "")
	testM3PromReadHandlerRead(t, buildWarningMeta("foo", "bar"), "foo_bar")
	testM3PromReadHandlerRead(t, block.ResultMetadata{Exhaustive: false},
		headers.LimitHeaderSeriesLimitApplied)
}

func testM3PromReadHandlerRead(
	t *testing.T,
	resultMeta block.ResultMetadata,
	ex string,
) {
	values, bounds := test.GenerateValuesAndBounds(nil, nil)

	setup := newTestSetup(timeoutOpts, nil)
	promRead := setup.Handlers.read

	seriesMeta := test.NewSeriesMeta("dummy", len(values))
	meta := block.Metadata{
		Bounds:         bounds,
		Tags:           models.NewTags(0, models.NewTagOptions()),
		ResultMetadata: resultMeta,
	}

	b := test.NewBlockFromValuesWithMetaAndSeriesMeta(meta, seriesMeta, values)
	setup.Storage.SetFetchBlocksResult(block.Result{Blocks: []block.Block{b}}, nil)

	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.Header.Add(headers.RenderFormat, "m3ql")
	req.URL.RawQuery = defaultParams().Encode()

	recorder := httptest.NewRecorder()
	promRead.ServeHTTP(recorder, req)

	header := recorder.Header().Get(headers.LimitHeader)
	assert.Equal(t, ex, header)

	var m3qlResp M3QLResp
	require.NoError(t, json.Unmarshal(recorder.Body.Bytes(), &m3qlResp))

	assert.Len(t, m3qlResp, 2)
	assert.Equal(t, "dummy0", m3qlResp[0].Target)
	assert.Equal(t, map[string]string{"__name__": "dummy0", "dummy0": "dummy0"},
		m3qlResp[0].Tags)
	assert.Equal(t, 10000, m3qlResp[0].StepSizeMs)
	assert.Equal(t, "dummy1", m3qlResp[1].Target)
	assert.Equal(t, map[string]string{"__name__": "dummy1", "dummy1": "dummy1"},
		m3qlResp[1].Tags)
	assert.Equal(t, 10000, m3qlResp[1].StepSizeMs)
}

func newReadRequest(t *testing.T, params url.Values) *http.Request {
	req, err := http.NewRequest("GET", PromReadURL, nil)
	require.NoError(t, err)
	req.URL.RawQuery = params.Encode()
	return req
}

type testSetup struct {
	Storage     mock.Storage
	Handlers    testSetupHandlers
	QueryOpts   *executor.QueryOptions
	FetchOpts   *storage.FetchOptions
	TimeoutOpts *prometheus.TimeoutOpts
	options     options.HandlerOptions
}

type testSetupHandlers struct {
	read        *promReadHandler
	instantRead *promReadHandler
}

func newTestSetup(
	timeout *prometheus.TimeoutOpts,
	mockEngine *executor.MockEngine,
) *testSetup {
	mockStorage := mock.NewMockStorage()

	instrumentOpts := instrument.NewOptions()
	engineOpts := executor.NewEngineOptions().
		SetStore(mockStorage).
		SetLookbackDuration(time.Minute).
		SetGlobalEnforcer(nil).
		SetInstrumentOptions(instrumentOpts)
	engine := executor.NewEngine(engineOpts)
	if mockEngine != nil {
		engine = mockEngine
	}
	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{}
	fetchOptsBuilder := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	tagOpts := models.NewTagOptions()
	limitsConfig := config.LimitsConfiguration{}
	keepNans := false

	opts := options.EmptyHandlerOptions().
		SetEngine(engine).
		SetFetchOptionsBuilder(fetchOptsBuilder).
		SetTagOptions(tagOpts).
		SetTimeoutOpts(timeout).
		SetInstrumentOpts(instrumentOpts).
		SetConfig(config.Configuration{
			Limits: limitsConfig,
			ResultOptions: config.ResultOptions{
				KeepNans: keepNans,
			},
		})

	read := NewPromReadHandler(opts).(*promReadHandler)
	instantRead := NewPromReadInstantHandler(opts).(*promReadHandler)

	return &testSetup{
		Storage: mockStorage,
		Handlers: testSetupHandlers{
			read:        read,
			instantRead: instantRead,
		},
		QueryOpts:   &executor.QueryOptions{},
		FetchOpts:   storage.NewFetchOptions(),
		TimeoutOpts: timeoutOpts,
		options:     opts,
	}
}

func TestPromReadHandlerServeHTTPMaxComputedDatapoints(t *testing.T) {
	setup := newTestSetup(timeoutOpts, nil)
	opts := setup.Handlers.read.opts
	setup.Handlers.read.opts = opts.SetConfig(config.Configuration{
		Limits: config.LimitsConfiguration{
			PerQuery: config.PerQueryLimitsConfiguration{
				PrivateMaxComputedDatapoints: 3599,
			},
		},
	})

	params := defaultParams()
	params.Set(startParam, time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).
		Format(time.RFC3339Nano))
	params.Set(endParam, time.Date(2018, 1, 1, 1, 0, 0, 0, time.UTC).
		Format(time.RFC3339Nano))
	params.Set(handleroptions.StepParam, (time.Second).String())
	req := newReadRequest(t, params)

	recorder := httptest.NewRecorder()
	setup.Handlers.read.ServeHTTP(recorder, req)
	resp := recorder.Result()

	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)

	d, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	// not a public struct in xhttp, but it's small.
	var errResp struct {
		Error string `json:"error"`
	}
	require.NoError(t, json.Unmarshal(d, &errResp))

	expected := "querying from 2018-01-01 00:00:00 +0000 UTC to 2018-01-01 01:00:00 +0000 UTC with step size 1s " +
		"would result in too many datapoints (end - start / step > 3599). Either decrease the query resolution " +
		"(?step=XX), decrease the time window, or increase the limit (`limits.maxComputedDatapoints`)"
	assert.Equal(t, expected, errResp.Error)
}

func TestPromReadHandler_validateRequest(t *testing.T) {
	dt := func(year int, month time.Month, day, hour int) time.Time {
		return time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
	}

	cases := []struct {
		name          string
		params        models.RequestParams
		max           int
		errorExpected bool
	}{{
		name: "under limit",
		params: models.RequestParams{
			Step:  time.Second,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           3601,
		errorExpected: false,
	}, {
		name: "at limit",
		params: models.RequestParams{
			Step:  time.Second,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           3600,
		errorExpected: false,
	}, {
		name: "over limit",
		params: models.RequestParams{
			Step:  time.Second,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           3599,
		errorExpected: true,
	}, {
		name: "large query, limit disabled (0)",
		params: models.RequestParams{
			Step:  time.Second,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           0,
		errorExpected: false,
	}, {
		name: "large query, limit disabled (negative)",
		params: models.RequestParams{
			Step:  time.Second,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           -50,
		errorExpected: false,
	}, {
		name: "uneven step over limit",
		params: models.RequestParams{
			Step:  34 * time.Minute,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 11),
		},
		max:           1,
		errorExpected: true,
	}, {
		name: "uneven step under limit",
		params: models.RequestParams{
			Step:  34 * time.Minute,
			Start: dt(2018, 1, 1, 0),
			End:   dt(2018, 1, 1, 1),
		},
		max:           2,
		errorExpected: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateRequest(tc.params, tc.max)
			if tc.errorExpected {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
			}
		})
	}
}

type cancelWatcher struct {
	delay time.Duration
}

var _ handler.CancelWatcher = (*cancelWatcher)(nil)

func (c *cancelWatcher) WatchForCancel(context.Context, context.CancelFunc) {
	// Simulate longer request to test timeout.
	time.Sleep(c.delay)
}
