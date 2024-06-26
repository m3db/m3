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
	"reflect"
	"runtime"
	"sort"
	"strings"
)

var (
	// name is global and set on startup.
	name string
	// clusters are constant, set by the test harness.
	clusters = []string{"coordinator-cluster-a", "coordinator-cluster-b"}
)

func main() {
	var ts int
	flag.IntVar(&ts, "t", -1, "metric name to search")
	flag.Parse()

	requireTrue(ts > 0, "no timestamp supplied")
	name = fmt.Sprintf("foo_%d", ts)
	instant := fmt.Sprintf("http://0.0.0.0:7201/m3query/api/v1/query?query=%s", name)
	rnge := fmt.Sprintf("http://0.0.0.0:7201/m3query/api/v1/query_range?query=%s"+
		"&start=%d&end=%d&step=100", name, ts/100*100, (ts/100+1)*100)

	for _, url := range []string{instant, rnge} {
		singleClusterDefaultStrip(url)
		bothClusterCustomStrip(url)
		bothClusterDefaultStrip(url)
		bothClusterNoStrip(url)
		bothClusterMultiStrip(url)
	}
}

func queryWithHeader(url string, h string) (response, error) {
	var result response
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return result, err
	}

	req.Header.Add(restrictByTagsJSONHeader, h)
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

func mustParseOpts(o stringTagOptions) string {
	m, err := json.Marshal(o)
	requireNoError(err, "cannot marshal to json")
	return string(m)
}

func bothClusterDefaultStrip(url string) {
	m := mustParseOpts(stringTagOptions{
		Restrict: []stringMatch{
			stringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
	})

	resp, err := queryWithHeader(url, m)
	requireNoError(err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	requireEqual(len(data), 2)
	for i, d := range data {
		requireEqual(2, len(d.Metric))
		requireEqual(name, d.Metric["__name__"])
		requireEqual(clusters[i], d.Metric["cluster"])
	}
}

func bothClusterCustomStrip(url string) {
	m := mustParseOpts(stringTagOptions{
		Restrict: []stringMatch{
			stringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{"__name__"},
	})

	resp, err := queryWithHeader(url, string(m))
	requireNoError(err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	requireEqual(len(data), 2)
	for i, d := range data {
		requireEqual(2, len(d.Metric))
		requireEqual(clusters[i], d.Metric["cluster"])
		requireEqual("1", d.Metric["val"])
	}
}

func bothClusterNoStrip(url string) {
	m := mustParseOpts(stringTagOptions{
		Restrict: []stringMatch{
			stringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{},
	})

	resp, err := queryWithHeader(url, string(m))
	requireNoError(err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	requireEqual(len(data), 2)
	for i, d := range data {
		requireEqual(3, len(d.Metric))
		requireEqual(name, d.Metric["__name__"])
		requireEqual(clusters[i], d.Metric["cluster"])
		requireEqual("1", d.Metric["val"])
	}
}

func bothClusterMultiStrip(url string) {
	m := mustParseOpts(stringTagOptions{
		Restrict: []stringMatch{
			stringMatch{Name: "val", Type: "EQUAL", Value: "1"},
		},
		Strip: []string{"val", "__name__"},
	})

	resp, err := queryWithHeader(url, string(m))
	requireNoError(err, "failed to query")

	data := resp.Data.Result
	data.Sort()
	requireEqual(len(data), 2)
	for i, d := range data {
		requireEqual(1, len(d.Metric))
		requireEqual(clusters[i], d.Metric["cluster"])
	}
}

// NB: cluster 1 is expected to have metrics with vals in range: [1,5]
// and cluster 2 is expected to have metrics with vals in range: [1,10]
// so setting the value to be in (5..10] should hit only a single metric.
func singleClusterDefaultStrip(url string) {
	m := mustParseOpts(stringTagOptions{
		Restrict: []stringMatch{
			stringMatch{Name: "val", Type: "EQUAL", Value: "9"},
		},
	})

	resp, err := queryWithHeader(url, string(m))
	requireNoError(err, "failed to query")

	data := resp.Data.Result
	requireEqual(len(data), 1, url)
	requireEqual(2, len(data[0].Metric))
	requireEqual(name, data[0].Metric["__name__"], "single")
	requireEqual("coordinator-cluster-b", data[0].Metric["cluster"])
}

/*

Helper functions below. This allows the test file to avoid importing any non
standard libraries.

*/

const restrictByTagsJSONHeader = "M3-Restrict-By-Tags-JSON"

// StringMatch is an easy to use JSON representation of models.Matcher that
// allows plaintext fields rather than forcing base64 encoded values.
type stringMatch struct {
	Name  string `json:"name"`
	Type  string `json:"type"`
	Value string `json:"value"`
}

// stringTagOptions is an easy to use JSON representation of
// storage.RestrictByTag that allows plaintext string fields rather than
// forcing base64 encoded values.
type stringTagOptions struct {
	Restrict []stringMatch `json:"match"`
	Strip    []string      `json:"strip"`
}

func printMessage(msg ...interface{}) {
	fmt.Println(msg...)

	_, fn, line, _ := runtime.Caller(4)
	fmt.Printf("\tin func: %v, line: %v\n", fn, line)

	os.Exit(1)
}

func testEqual(ex interface{}, ac interface{}) bool {
	if ex == nil || ac == nil {
		return ex == ac
	}

	return reflect.DeepEqual(ex, ac)
}

func requireEqual(ex interface{}, ac interface{}, msg ...interface{}) {
	if testEqual(ex, ac) {
		return
	}

	fmt.Printf(""+
		"Not equal: %#v (expected)\n"+
		"           %#v (actual)\n", ex, ac)
	printMessage(msg...)
}

func requireNoError(err error, msg ...interface{}) {
	if err == nil {
		return
	}

	fmt.Printf("Received unexpected error %q\n", err)
	printMessage(msg...)
}

func requireTrue(b bool, msg ...interface{}) {
	if b {
		return
	}

	fmt.Println("Expected true, got false")
	printMessage(msg...)
}

// response represents Prometheus's query response.
type response struct {
	// Status is the response status.
	Status string `json:"status"`
	// Data is the response data.
	Data data `json:"data"`
}

type data struct {
	// ResultType is the result type for the response.
	ResultType string `json:"resultType"`
	// Result is the list of results for the response.
	Result results `json:"result"`
}

type results []result

// Len is the number of elements in the collection.
func (r results) Len() int { return len(r) }

// Less reports whether the element with
// index i should sort before the element with index j.
func (r results) Less(i, j int) bool {
	return r[i].id < r[j].id
}

// Swap swaps the elements with indexes i and j.
func (r results) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

// Sort sorts the results.
func (r results) Sort() {
	for i, result := range r {
		r[i] = result.genID()
	}

	sort.Sort(r)
}

// result is the result itself.
type result struct {
	// Metric is the tags for the result.
	Metric tags `json:"metric"`
	// Values is the set of values for the result.
	Values values `json:"values"`
	id     string
}

// tags is a simple representation of Prometheus tags.
type tags map[string]string

// Values is a list of values for the Prometheus result.
type values []value

// Value is a single value for Prometheus result.
type value []interface{}

func (r *result) genID() result {
	tags := make(sort.StringSlice, 0, len(r.Metric))
	for k, v := range r.Metric {
		tags = append(tags, fmt.Sprintf("%s:%s,", k, v))
	}

	sort.Sort(tags)
	var sb strings.Builder
	// NB: this may clash but exact tag values are also checked, and this is a
	// validation endpoint so there's less concern over correctness.
	for _, t := range tags {
		sb.WriteString(t)
	}

	r.id = sb.String()
	return *r
}
