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
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	xmetrics "github.com/m3db/m3/src/dbnode/x/metrics"
	"github.com/m3db/m3/src/query/api/v1/handler"
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
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtest "github.com/m3db/m3/src/x/test"

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

type testVals struct {
	start time.Time
	query string
}

func buildBody(query string, start time.Time) io.Reader {
	vals := url.Values{}
	vals.Add("query", query)
	vals.Add("start", start.Format(time.RFC3339))
	vals.Add("end", start.Add(time.Hour).Format(time.RFC3339))
	qs := vals.Encode()
	return bytes.NewBuffer([]byte(qs))
}

func TestParseExpr(t *testing.T) {
	query := "" +
		`up{a="b"} + 7 - sum(rate(down{c!="d"}[2m])) + ` +
		`left{e=~"f"} offset 30m and right{g!~"h"} + ` + `
		max_over_time(foo[1m] offset 1h)`

	start := time.Now().Truncate(time.Hour)
	req := httptest.NewRequest(http.MethodPost, "/", buildBody(query, start))
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeFormURLEncoded)
	readReq, err := ParseExpr(req)
	require.NoError(t, err)

	q := func(start, end time.Time, matchers []*prompb.LabelMatcher) *prompb.Query {
		return &prompb.Query{
			StartTimestampMs: start.Unix() * 1000,
			EndTimestampMs:   end.Unix() * 1000,
			Matchers:         matchers,
		}
	}

	b := func(s string) []byte { return []byte(s) }
	expected := []*prompb.Query{
		q(start, start.Add(time.Hour),
			[]*prompb.LabelMatcher{
				{Name: b("a"), Value: b("b"), Type: prompb.LabelMatcher_EQ},
				{Name: b("__name__"), Value: b("up"), Type: prompb.LabelMatcher_EQ}}),
		q(start.Add(time.Minute*-2), start.Add(time.Hour),
			[]*prompb.LabelMatcher{
				{Name: b("c"), Value: b("d"), Type: prompb.LabelMatcher_NEQ},
				{Name: b("__name__"), Value: b("down"), Type: prompb.LabelMatcher_EQ}}),
		q(start.Add(time.Minute*-30), start.Add(time.Minute*30),
			[]*prompb.LabelMatcher{
				{Name: b("e"), Value: b("f"), Type: prompb.LabelMatcher_RE},
				{Name: b("__name__"), Value: b("left"), Type: prompb.LabelMatcher_EQ}}),
		q(start, start.Add(time.Hour),
			[]*prompb.LabelMatcher{
				{Name: b("g"), Value: b("h"), Type: prompb.LabelMatcher_NRE},
				{Name: b("__name__"), Value: b("right"), Type: prompb.LabelMatcher_EQ}}),
		q(start.Add(time.Minute*-61), start,
			[]*prompb.LabelMatcher{
				{Name: b("__name__"), Value: b("foo"), Type: prompb.LabelMatcher_EQ}}),
	}

	assert.Equal(t, expected, readReq.Queries)
}

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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	lstore, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false},
			fmt.Errorf("not initialized")).MaxTimes(1)
	storage := test.NewSlowStorage(lstore, 10*time.Millisecond)
	promRead := readHandler(storage, timeoutOpts)
	server := httptest.NewServer(test.NewSlowHandler(promRead, 10*time.Millisecond))
	return server
}

func readHandler(store storage.Storage,
	timeoutOpts *prometheus.TimeoutOpts) http.Handler {
	fetchOpts := handleroptions.FetchOptionsBuilderOptions{
		Limits: handleroptions.FetchOptionsBuilderLimitsOptions{
			SeriesLimit: 100,
		},
	}
	iOpts := instrument.NewOptions()
	engine := newEngine(store, defaultLookbackDuration, nil, iOpts)
	opts := options.EmptyHandlerOptions().
		SetEngine(engine).
		SetInstrumentOpts(iOpts).
		SetFetchOptionsBuilder(handleroptions.NewFetchOptionsBuilder(fetchOpts)).
		SetTimeoutOpts(timeoutOpts)

	return NewPromReadHandler(opts)
}

func TestPromReadParsing(t *testing.T) {
	ctrl := xtest.NewController(t)
	storage, _ := m3.NewStorageAndSession(t, ctrl)
	builderOpts := handleroptions.FetchOptionsBuilderOptions{
		Limits: handleroptions.FetchOptionsBuilderLimitsOptions{
			SeriesLimit: 100,
		},
	}
	engine := newEngine(storage, defaultLookbackDuration, nil,
		instrument.NewOptions())

	opts := options.EmptyHandlerOptions().
		SetEngine(engine).
		SetFetchOptionsBuilder(handleroptions.NewFetchOptionsBuilder(builderOpts)).
		SetTimeoutOpts(timeoutOpts)

	req := httptest.NewRequest("POST", PromReadURL, test.GeneratePromReadBody(t))
	r, fetchOpts, err := ParseRequest(context.TODO(), req, opts)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, len(r.Queries), 1)
	fmt.Println(fetchOpts)
}

func TestPromFetchTimeoutParsing(t *testing.T) {
	url := fmt.Sprintf("%s?timeout=2m", PromReadURL)
	req := httptest.NewRequest("POST", url, test.GeneratePromReadBody(t))
	dur, err := prometheus.ParseRequestTimeout(req, time.Second)
	require.NoError(t, err)
	assert.Equal(t, 2*time.Minute, dur)
}

func TestPromReadParsingBad(t *testing.T) {
	req := httptest.NewRequest("POST", PromReadURL, strings.NewReader("bad body"))
	_, _, err := ParseRequest(context.TODO(), req, options.EmptyHandlerOptions())
	require.NotNil(t, err, "unable to parse request")
}

func TestPromReadStorageWithFetchError(t *testing.T) {
	ctrl := xtest.NewController(t)
	watcher := &cancelWatcher{}
	readRequest := &prompb.ReadRequest{
		Queries: []*prompb.Query{
			{},
		},
	}

	fetchOpts := &storage.FetchOptions{}
	result := storage.PromResult{Metadata: block.ResultMetadata{
		Exhaustive: true, LocalOnly: true}}
	engine := executor.NewMockEngine(ctrl)
	engine.EXPECT().
		ExecuteProm(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(result, fmt.Errorf("expr err"))

	opts := options.EmptyHandlerOptions().SetEngine(engine)
	res, err := Read(context.TODO(), watcher, readRequest, fetchOpts, opts)
	require.Error(t, err, "unable to read from storage")

	meta := res.Meta
	assert.True(t, meta.Exhaustive)
	assert.True(t, meta.LocalOnly)
	assert.Equal(t, 0, len(meta.Warnings))

	assert.Equal(t, 1, watcher.count)
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

	_, err := c.Post(server.URL, xhttp.ContentTypeProtobuf, test.GeneratePromReadBody(t))
	assert.Error(t, err)
}

func TestQueryKillOnTimeout(t *testing.T) {
	server := setupServer(t)
	defer server.Close()

	req, _ := http.NewRequest("POST", server.URL, test.GeneratePromReadBody(t))
	req.Header.Add(xhttp.HeaderContentType, xhttp.ContentTypeProtobuf)
	req.Header.Add("timeout", "1ms")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	require.NotNil(t, resp)
	assert.Equal(t, resp.StatusCode, 500, "Status code not 500")
}

func TestReadErrorMetricsCount(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().FetchTagged(gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: true}, fmt.Errorf("unable to get data"))
	session.EXPECT().IteratorPools().
		Return(nil, nil)

	reporter := xmetrics.NewTestStatsReporter(xmetrics.NewTestStatsReporterOptions())
	scope, closer := tally.NewRootScope(tally.ScopeOptions{Reporter: reporter}, time.Millisecond)
	defer closer.Close()
	readMetrics := newPromReadMetrics(scope)
	buildOpts := handleroptions.FetchOptionsBuilderOptions{
		Limits: handleroptions.FetchOptionsBuilderLimitsOptions{
			SeriesLimit: 100,
		},
	}
	engine := newEngine(storage, defaultLookbackDuration, nil,
		instrument.NewOptions())
	opts := options.EmptyHandlerOptions().
		SetEngine(engine).
		SetTimeoutOpts(&prometheus.TimeoutOpts{FetchTimeout: time.Minute}).
		SetFetchOptionsBuilder(handleroptions.NewFetchOptionsBuilder(buildOpts))
	promRead := &promReadHandler{
		promReadMetrics: readMetrics,
		opts:            opts,
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
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := storage.TimeToPromTimestamp(now)

	r := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
					Samples: []prompb.Sample{{Value: 1, Timestamp: promNow}},
					Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				},
			},
		},
		Metadata: block.ResultMetadata{
			Exhaustive: true,
			LocalOnly:  true,
			Warnings:   []block.Warning{{Name: "foo", Message: "bar"}},
		},
	}

	rTwo := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
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

	fetchOpts := &storage.FetchOptions{}
	watcher := &cancelWatcher{}
	res, err := Read(context.TODO(), watcher, req, fetchOpts, handlerOpts)
	require.NoError(t, err)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
			{
				Labels:  []prompb.Label{{Name: []byte("c"), Value: []byte("d")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 2}},
			},
		},
	}

	result := res.Result
	assert.Equal(t, expected.Timeseries[0], result[0].Timeseries[0])
	assert.Equal(t, expected.Timeseries[1], result[1].Timeseries[0])

	meta := res.Meta
	assert.False(t, meta.Exhaustive)
	assert.True(t, meta.LocalOnly)
	require.Equal(t, 1, len(meta.Warnings))
	assert.Equal(t, "foo_bar", meta.Warnings[0].Header())

	assert.Equal(t, 2, watcher.count)
}

func TestReadWithOptions(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	now := time.Now()
	promNow := storage.TimeToPromTimestamp(now)

	r := storage.PromResult{
		PromResult: &prompb.QueryResult{
			Timeseries: []*prompb.TimeSeries{
				{
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

	fetchOpts := storage.NewFetchOptions()
	fetchOpts.RestrictQueryOptions = &storage.RestrictQueryOptions{
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

	res, err := Read(context.TODO(), nil, req, fetchOpts, handlerOpts)
	require.NoError(t, err)
	expected := &prompb.QueryResult{
		Timeseries: []*prompb.TimeSeries{
			{
				Labels:  []prompb.Label{{Name: []byte("a"), Value: []byte("b")}},
				Samples: []prompb.Sample{{Timestamp: promNow, Value: 1}},
			},
		},
	}

	result := res.Result
	assert.Equal(t, expected.Timeseries[0], result[0].Timeseries[0])
}

type cancelWatcher struct {
	sync.Mutex
	count int
}

var _ handler.CancelWatcher = (*cancelWatcher)(nil)

func (c *cancelWatcher) WatchForCancel(context.Context, context.CancelFunc) {
	c.Lock()
	c.count++
	c.Unlock()
}
