// Copyright (c) 2019 Uber Technologies, Inc.
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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"runtime"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/require"
)

func main() {
	var ts int
	flag.IntVar(&ts, "t", -1, "metric name to search")
	flag.Parse()

	require.True(t, ts > 0, "no timestamp supplied")
	name = fmt.Sprintf("foo_%d", ts)
	instant := fmt.Sprintf("http://0.0.0.0:7201/api/v1/query?query=%s", name)
	rnge := fmt.Sprintf("http://0.0.0.0:7201/api/v1/query_range?query=%s"+
		"&start=%d&end=%d&step=100", name, ts/100*100, (ts/100+1)*100)

	for _, url := range []string{instant, rnge} {
		singleClusterDefaultStrip(url)
		bothClusterCustomStrip(url)
		bothClusterDefaultStrip(url)
		bothClusterNoStrip(url)
		bothClusterMultiStrip(url)
	}
}

func queryWithHeader(url string, h string) (prometheus.Response, error) {
	var result prometheus.Response
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return result, err
	}

	req.Header.Add(handler.RestrictByTagsJSONHeader, h)
	client := http.DefaultClient
	resp, err := client.Do(req)
	if err != nil {
		return result, err
	}

	if resp.StatusCode != http.StatusOK {
		return result, fmt.Errorf("response failed with code %s", resp.Status)
	}

	defer resp.Body.Close()
	data, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return result, err
	}

	json.Unmarshal(data, &result)
	return result, err
}

func mustMatcher(t models.MatchType, n string, v string) models.Matcher {
	m, err := models.NewMatcher(models.MatchEqual, []byte("val"), []byte("1"))
	if err != nil {
		panic(err)
	}

	return m
}

type tester struct{}

// Ensure tester is a TestingT and set a global `t`.
var t require.TestingT = &tester{}

// name is global and set on startup.
var name string

func (t *tester) Errorf(format string, args ...interface{}) {
	_, fn, line, _ := runtime.Caller(4)
	args[2] = fmt.Sprintf(" at %s:%d:\n%v", fn, line, args[2])
	fmt.Printf(format, args...)
}

func (t *tester) FailNow() {
	os.Exit(1)
}

func mustParseOpts(o handler.StringTagOptions) string {
	m, err := json.Marshal(o)
	require.NoError(t, err, "cannot marshal to json")
	return string(m)
}

func bothClusterDefaultStrip(url string) {
	m := mustParseOpts(handler.StringTagOptions{
		Restrict: []handler.StringMatch{
			handler.StringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
	})

	resp, err := queryWithHeader(url, string(m))
	require.NoError(t, err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	require.Equal(t, len(data), 2)
	clusters := []string{"coordinator-cluster-a", "coordinator-cluster-b"}
	for i, d := range data {
		require.Equal(t, 2, len(d.Metric))
		require.Equal(t, name, d.Metric["__name__"])
		require.Equal(t, clusters[i], d.Metric["cluster"])
	}
}

func bothClusterCustomStrip(url string) {
	m := mustParseOpts(handler.StringTagOptions{
		Restrict: []handler.StringMatch{
			handler.StringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{"__name__"},
	})

	resp, err := queryWithHeader(url, string(m))
	require.NoError(t, err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	require.Equal(t, len(data), 2)
	clusters := []string{"coordinator-cluster-a", "coordinator-cluster-b"}
	for i, d := range data {
		require.Equal(t, 2, len(d.Metric))
		require.Equal(t, clusters[i], d.Metric["cluster"])
		require.Equal(t, "1", d.Metric["val"])
	}
}

func bothClusterNoStrip(url string) {
	m := mustParseOpts(handler.StringTagOptions{
		Restrict: []handler.StringMatch{
			handler.StringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{},
	})

	resp, err := queryWithHeader(url, string(m))
	require.NoError(t, err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	require.Equal(t, len(data), 2)
	clusters := []string{"coordinator-cluster-a", "coordinator-cluster-b"}
	for i, d := range data {
		require.Equal(t, 3, len(d.Metric))
		require.Equal(t, name, d.Metric["__name__"])
		require.Equal(t, clusters[i], d.Metric["cluster"])
		require.Equal(t, "1", d.Metric["val"])
	}
}

func bothClusterMultiStrip(url string) {
	m := mustParseOpts(handler.StringTagOptions{
		Restrict: []handler.StringMatch{
			handler.StringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{"val", "__name__"},
	})

	resp, err := queryWithHeader(url, string(m))
	require.NoError(t, err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	require.Equal(t, len(data), 2)
	clusters := []string{"coordinator-cluster-a", "coordinator-cluster-b"}
	for i, d := range data {
		require.Equal(t, 1, len(d.Metric))
		require.Equal(t, clusters[i], d.Metric["cluster"])
	}
}

// NB: cluster 1 is expected to have metrics with vals in range: [1,5]
// and cluster 2 is expected to have metrics with vals in range: [1,10]
// so setting the value to be in (5..10] should hit only a single metric.
func singleClusterDefaultStrip(url string) {
	m := mustParseOpts(handler.StringTagOptions{
		Restrict: []handler.StringMatch{
			handler.StringMatch{Name: "val", Type: "EQUAL", Value: "9"},
		},
	})

	resp, err := queryWithHeader(url, string(m))
	require.NoError(t, err, "failed to query")

	data := resp.Data.Result
	require.Equal(t, len(data), 1, url)
	require.Equal(t, 2, len(data[0].Metric))
	require.Equal(t, name, data[0].Metric["__name__"], "single")
	require.Equal(t, "coordinator-cluster-b", data[0].Metric["cluster"])
}
