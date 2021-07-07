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
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/mock"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/x/instrument"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseRequest(t *testing.T) {
	setup := newTestSetup(t, nil)
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	ctx, parsed, err := ParseRequest(req.Context(), req, false, setup.options)
	require.NoError(t, err)
	require.Equal(t, 15*time.Second, parsed.Params.Timeout)
	require.Equal(t, 15*time.Second, parsed.FetchOpts.Timeout)
	require.Equal(t, 0, parsed.FetchOpts.DocsLimit)
	require.Equal(t, 0, parsed.FetchOpts.SeriesLimit)
	require.Equal(t, false, parsed.FetchOpts.RequireExhaustive)
	require.Equal(t, 0, parsed.QueryOpts.QueryContextOptions.LimitMaxDocs)
	require.Equal(t, 0, parsed.QueryOpts.QueryContextOptions.LimitMaxTimeseries)
	require.Equal(t, false, parsed.QueryOpts.QueryContextOptions.RequireExhaustive)
	require.Nil(t, parsed.QueryOpts.QueryContextOptions.RestrictFetchType)
	// Make sure the context has the deadline and http header set.
	_, ok := ctx.Deadline()
	require.True(t, ok)
	header := ctx.Value(handleroptions.RequestHeaderKey)
	require.NotNil(t, header)
	_, ok = header.(http.Header)
	require.True(t, ok)
}

func TestPromReadHandlerRead(t *testing.T) {
	testPromReadHandlerRead(t, block.NewResultMetadata())
	testPromReadHandlerRead(t, buildWarningMeta("foo", "bar"))
	testPromReadHandlerRead(t, block.ResultMetadata{Exhaustive: false})
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

	setup := newTestSetup(t, engine)
	promRead := setup.Handlers.read

	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()
	ctx := req.Context()
	var cancel context.CancelFunc
	// Clients calling into read have the timeout set from the defined fetch params
	ctx, cancel = context.WithTimeout(ctx, 1*time.Nanosecond)
	defer cancel()

	r, parseErr := testParseParams(req)
	require.Nil(t, parseErr)
	assert.Equal(t, models.FormatPromQL, r.FormatType)
	parsed := ParsedOptions{
		QueryOpts: setup.QueryOpts,
		FetchOpts: setup.FetchOpts,
		Params:    r,
	}

	_, err := read(ctx, parsed, promRead.opts)
	require.Error(t, err)
	require.Equal(t,
		"context deadline exceeded",
		err.Error())
}

func testPromReadHandlerRead(t *testing.T, resultMeta block.ResultMetadata) {
	t.Helper()

	values, bounds := test.GenerateValuesAndBounds(nil, nil)

	setup := newTestSetup(t, nil)
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

type testSetup struct {
	Storage   mock.Storage
	Handlers  testSetupHandlers
	QueryOpts *executor.QueryOptions
	FetchOpts *storage.FetchOptions
	options   options.HandlerOptions
}

type testSetupHandlers struct {
	read        *promReadHandler
	instantRead *promReadHandler
}

func newTestSetup(
	t *testing.T,
	mockEngine *executor.MockEngine,
) *testSetup {
	mockStorage := mock.NewMockStorage()

	instrumentOpts := instrument.NewOptions()
	engineOpts := executor.NewEngineOptions().
		SetStore(mockStorage).
		SetLookbackDuration(time.Minute).
		SetInstrumentOptions(instrumentOpts)
	engine := executor.NewEngine(engineOpts)
	if mockEngine != nil {
		engine = mockEngine
	}
	fetchOptsBuilderCfg := handleroptions.FetchOptionsBuilderOptions{
		Timeout: 15 * time.Second,
	}
	fetchOptsBuilder, err := handleroptions.NewFetchOptionsBuilder(fetchOptsBuilderCfg)
	require.NoError(t, err)
	tagOpts := models.NewTagOptions()
	limitsConfig := config.LimitsConfiguration{}
	keepNaNs := false

	opts := options.EmptyHandlerOptions().
		SetEngine(engine).
		SetFetchOptionsBuilder(fetchOptsBuilder).
		SetTagOptions(tagOpts).
		SetInstrumentOpts(instrumentOpts).
		SetStorage(mockStorage).
		SetConfig(config.Configuration{
			Limits: limitsConfig,
			ResultOptions: config.ResultOptions{
				KeepNaNs: keepNaNs,
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
		QueryOpts: &executor.QueryOptions{},
		FetchOpts: storage.NewFetchOptions(),
		options:   opts,
	}
}
