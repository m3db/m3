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

package remote

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/block"
	qcost "github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/test/m3"
	xclock "github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

var (
	promReadTestMetrics     = newPromReadMetrics(tally.NewTestScope("", nil))
	defaultLookbackDuration = time.Minute

	timeoutOpts = &prometheus.TimeoutOpts{
		FetchTimeout: 15 * time.Second,
	}
)

func newEngine(
	s storage.Storage,
	lookbackDuration time.Duration,
	enforcer qcost.ChainedEnforcer,
	instrumentOpts instrument.Options,
) executor.Engine {
	engineOpts := executor.NewEngineOptions().
		SetStore(s).
		SetLookbackDuration(lookbackDuration).
		SetGlobalEnforcer(enforcer).
		SetInstrumentOptions(instrumentOpts)

	return executor.NewEngine(engineOpts)
}

func setupServer(t *testing.T) *httptest.Server {
	ctrl := gomock.NewController(t)
	// No calls expected on session object
	lstore, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, fmt.Errorf("not initialized"))
	storage := test.NewSlowStorage(lstore, 10*time.Millisecond)
	promRead := readHandler(storage, timeoutOpts)
	server := httptest.NewServer(test.NewSlowHandler(promRead, 10*time.Millisecond))
	return server
}

func readHandler(store storage.Storage, timeoutOpts *prometheus.TimeoutOpts) *PromReadHandler {
	opts := handleroptions.FetchOptionsBuilderOptions{Limit: 100}
	engine := newEngine(store, defaultLookbackDuration, nil,
		instrument.NewOptions())
	return &PromReadHandler{
		engine:              engine,
		promReadMetrics:     promReadTestMetrics,
		timeoutOpts:         timeoutOpts,
		fetchOptionsBuilder: handleroptions.NewFetchOptionsBuilder(opts),
		instrumentOpts:      instrument.NewOptions(),
	}
}

func TestPromReadParsing(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	opts := handleroptions.FetchOptionsBuilderOptions{Limit: 100}
	engine := newEngine(storage, defaultLookbackDuration, nil,
		instrument.NewOptions())
	promRead := &PromReadHandler{
		engine:              engine,
		promReadMetrics:     promReadTestMetrics,
		fetchOptionsBuilder: handleroptions.NewFetchOptionsBuilder(opts),
	}

	req := httptest.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	r, err := promRead.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Queries), 1)
}

func TestPromFetchTimeoutParsing(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	opts := handleroptions.FetchOptionsBuilderOptions{Limit: 100}
	engine := newEngine(storage, defaultLookbackDuration, nil,
		instrument.NewOptions())
	promRead := &PromReadHandler{
		engine:          engine,
		promReadMetrics: promReadTestMetrics,
		timeoutOpts: &prometheus.TimeoutOpts{
			FetchTimeout: 2 * time.Minute,
		},
		fetchOptionsBuilder: handleroptions.NewFetchOptionsBuilder(opts),
	}

	req := httptest.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	dur, err := prometheus.ParseRequestTimeout(req, promRead.timeoutOpts.FetchTimeout)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, dur)
}

func TestPromReadParsingBad(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	promRead := readHandler(storage, timeoutOpts)
	req := httptest.NewRequest("POST", PromReadURL, strings.NewReader("bad body"))
	_, err := promRead.parseRequest(req)
	require.NotNil(t, err, "unable to parse request")
}

func TestPromReadStorageWithFetchError(t *testing.T) {
	ctrl := gomock.NewController(t)
	store, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: true}, fmt.Errorf("unable to get data"))
	session.EXPECT().IteratorPools().
		Return(nil, nil)
	promRead := readHandler(store, timeoutOpts)
	req := test.GeneratePromReadRequest()
	recorder := httptest.NewRecorder()
	res, err := promRead.read(context.TODO(), recorder,
		req, time.Hour, storage.NewFetchOptions())
	require.Error(t, err, "unable to read from storage")
	header := recorder.Header().Get(handleroptions.LimitHeader)
	assert.Equal(t, 0, len(header))

	meta := res.meta
	assert.True(t, meta.Exhaustive)
	assert.True(t, meta.LocalOnly)
	assert.Equal(t, 0, len(meta.Warnings))
}

func TestQueryMatchMustBeEqual(t *testing.T) {
	req := test.GeneratePromReadRequest()
	matchers, err := storage.PromMatchersToM3(req.Queries[0].Matchers)
	require.NoError(t, err)

	_, err = matchers.ToTags(models.NewTagOptions())
	assert.NoError(t, err)
}

func TestQueryKillOnClientDisconnect(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	c := &http.Client{
		Timeout: 1 * time.Millisecond,
	}

	_, err := c.Post(server.URL, "application/x-protobuf", test.GeneratePromReadBody(t))
	assert.Error(t, err)
}

func TestQueryKillOnTimeout(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, test.GeneratePromReadBody(t))
	req.Header.Add("Content-Type", "application/x-protobuf")
	req.Header.Add("timeout", "1ms")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
	assert.Equal(t, resp.StatusCode, 500, "Status code not 500")
}

func TestReadErrorMetricsCount(t *testing.T) {
	ctrl := gomock.NewController(t)
	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: true}, fmt.Errorf("unable to get data"))
	session.EXPECT().IteratorPools().
		Return(nil, nil)

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	readMetrics := newPromReadMetrics(scope)
	opts := handleroptions.FetchOptionsBuilderOptions{Limit: 100}
	engine := newEngine(storage, defaultLookbackDuration, nil,
		instrument.NewOptions())
	promRead := &PromReadHandler{
		engine:              engine,
		promReadMetrics:     readMetrics,
		timeoutOpts:         timeoutOpts,
		fetchOptionsBuilder: handleroptions.NewFetchOptionsBuilder(opts),
		instrumentOpts:      instrument.NewOptions(),
	}

	req := httptest.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	promRead.ServeHTTP(httptest.NewRecorder(), req)
	foundMetric := xclock.WaitUntil(func() bool {
		found := reporter.Counters()["fetch.errors"]
		return found == 1
	}, 5*time.Second)
	require.True(t, foundMetric)
}

func TestMultipleRead(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := storage.TimeToPromTimestamp(now)

	r := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Samples: []prompb.Sample{{Value: 1, Timestamp: promNow}},
					Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				},
			},
		},
		Metadata: block.ResultMetadata{
			Exhaustive: true,
			LocalOnly:  true,
			Warnings:   []block.Warning{block.Warning{Name: "foo", Message: "bar"}},
		},
	}

	rTwo := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Samples: []prompb.Sample{{Value: 2, Timestamp: promNow}},
					Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				},
			},
		},
		Metadata: block.ResultMetadata{
			Exhaustive: false,
			LocalOnly:  true,
			Warnings:   []block.Warning{},
		},
	}

	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{StartTimestampMs: 10},
			{StartTimestampMs: 20},
		},
	}

	q, err := storage.PromReadQueryToM3(req.Queries[0])
	require.NoError(t, err)
	qTwo, err := storage.PromReadQueryToM3(req.Queries[1])
	require.NoError(t, err)

	engine := executor.NewMockEngine(ctrl)
	engine.EXPECT().
		ExecuteProm(gomock.Any(), q, gomock.Any(), gomock.Any()).
		Return(r, nil)
	engine.EXPECT().
		ExecuteProm(gomock.Any(), qTwo, gomock.Any(), gomock.Any()).
		Return(rTwo, nil)

	handlerOpts := options.EmptyHandlerOptions().SetEngine(engine).
		SetConfig(config.Configuration{
			ResultOptions: config.ResultOptions{
				KeepNans: true,
			},
		})

	h := NewPromReadHandler(handlerOpts).(*PromReadHandler)
	res, err := h.read(context.TODO(), nil, req, 0, storage.NewFetchOptions())
	require.NoError(t, err)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 2}},
			},
		},
	}

	result := res.result
	assert.Equal(t, expected.Timeseries[0], result[0].Timeseries[0])
	assert.Equal(t, expected.Timeseries[1], result[1].Timeseries[0])

	meta := res.meta
	assert.False(t, meta.Exhaustive)
	assert.True(t, meta.LocalOnly)
	require.Equal(t, 1, len(meta.Warnings))
	assert.Equal(t, "foo_bar", meta.Warnings[0].Header())
}

func TestReadWithOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := storage.TimeToPromTimestamp(now)

	r := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				&prompb.TimeSeries{
					Samples: []prompb.Sample{{Value: 1, Timestamp: promNow}},
					Labels: []prompb.Label{
						{Name: []byte("a"), Value: []byte("b")},
						{Name: []byte("remove"), Value: []byte("c")},
					},
				},
			},
		},
		Metadata: block.NewResultMetadata(),
	}

	req := &prompb.ReadRequest{
		Queries: []*prompb.Query{{StartTimestampMs: 10}},
	}

	q, err := storage.PromReadQueryToM3(req.Queries[0])
	require.NoError(t, err)

	engine := executor.NewMockEngine(ctrl)
	engine.EXPECT().
		ExecuteProm(gomock.Any(), q, gomock.Any(), gomock.Any()).
		Return(r, nil)

	opts := storage.NewFetchOptions()
	opts.RestrictQueryOptions = &storage.RestrictQueryOptions{
		RestrictByTag: &storage.RestrictByTag{
			Strip: [][]byte{[]byte("remove")},
		},
	}

	handlerOpts := options.EmptyHandlerOptions().SetEngine(engine).
		SetConfig(config.Configuration{
			ResultOptions: config.ResultOptions{
				KeepNans: true,
			},
		})

	h := NewPromReadHandler(handlerOpts).(*PromReadHandler)
	res, err := h.read(context.TODO(), nil, req, 0, opts)
	require.NoError(t, err)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			&prompb.TimeSeries{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	result := res.result
	assert.Equal(t, expected.Timeseries[0], result[0].Timeseries[0])
}
