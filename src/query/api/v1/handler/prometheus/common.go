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

package prometheus

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	xpromql "github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/json"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/golang/snappy"
	"github.com/prometheus/prometheus/promql"
)

const (
	queryParam          = "query"
	filterNameTagsParam = "tag"
	errFormatStr        = "error parsing param: %s, error: %v"
	maxTimeout          = 5 * time.Minute
	tolerance           = 0.0000001
)

var (
	roleName = []byte("role")
)

// TimeoutOpts stores options related to various timeout configurations.
type TimeoutOpts struct {
	FetchTimeout time.Duration
}

// ParsePromCompressedRequestResult is the result of a
// ParsePromCompressedRequest call.
type ParsePromCompressedRequestResult struct {
	CompressedBody   []byte
	UncompressedBody []byte
}

// ParsePromCompressedRequest parses a snappy compressed request from Prometheus.
func ParsePromCompressedRequest(
	r *http.Request,
) (ParsePromCompressedRequestResult, *xhttp.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return ParsePromCompressedRequestResult{},
			xhttp.NewParseError(err, http.StatusBadRequest)
	}
	defer body.Close()
	compressed, err := ioutil.ReadAll(body)

	if err != nil {
		return ParsePromCompressedRequestResult{},
			xhttp.NewParseError(err, http.StatusInternalServerError)
	}

	if len(compressed) == 0 {
		return ParsePromCompressedRequestResult{},
			xhttp.NewParseError(fmt.Errorf("empty request body"),
				http.StatusBadRequest)
	}

	reqBuf, err := snappy.Decode(nil, compressed)
	if err != nil {
		return ParsePromCompressedRequestResult{},
			xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return ParsePromCompressedRequestResult{
		CompressedBody:   compressed,
		UncompressedBody: reqBuf,
	}, nil
}

// ParseRequestTimeout parses the input request timeout with a default.
func ParseRequestTimeout(
	r *http.Request,
	configFetchTimeout time.Duration,
) (time.Duration, error) {
	timeout := r.Header.Get("timeout")
	if timeout == "" {
		return configFetchTimeout, nil
	}

	duration, err := time.ParseDuration(timeout)
	if err != nil {
		return 0, fmt.Errorf("%s: invalid 'timeout': %v",
			xhttp.ErrInvalidParams, err)
	}

	if duration > maxTimeout {
		return 0, fmt.Errorf("%s: invalid 'timeout': greater than %v",
			xhttp.ErrInvalidParams, maxTimeout)
	}

	return duration, nil
}

// ParseTagCompletionParamsToQueries parses all params from the GET request.
// Returns queries, a boolean indicating if the query completes names only, and
// any errors.
func ParseTagCompletionParamsToQueries(
	r *http.Request,
) ([]*storage.CompleteTagsQuery, bool, *xhttp.ParseError) {
	start, err := parseTimeWithDefault(r, "start", time.Time{})
	if err != nil {
		return nil, false, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	end, err := parseTimeWithDefault(r, "end", time.Now())
	if err != nil {
		return nil, false, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	// If there is a result type field present, parse it and set
	// complete name only parameter appropriately. Otherwise, default
	// to returning both completed tag names and values
	nameOnly := false
	if result := r.FormValue("result"); result != "" {
		switch result {
		case "default":
			// no-op
		case "tagNamesOnly":
			nameOnly = true
		default:
			return nil, false, xhttp.NewParseError(
				errors.ErrInvalidResultParamError, http.StatusBadRequest)
		}
	}

	queries, err := parseTagCompletionQueries(r)
	if err != nil {
		return nil, nameOnly, xhttp.NewParseError(
			fmt.Errorf(errFormatStr, queryParam, err), http.StatusBadRequest)
	}

	tagQueries := make([]*storage.CompleteTagsQuery, 0, len(queries))
	for _, query := range queries {
		tagQuery := &storage.CompleteTagsQuery{
			Start:            start,
			End:              end,
			CompleteNameOnly: nameOnly,
		}

		matchers, err := models.MatchersFromString(query)
		if err != nil {
			return nil, nameOnly, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		tagQuery.TagMatchers = matchers
		filterNameTags := r.Form[filterNameTagsParam]
		tagQuery.FilterNameTags = make([][]byte, len(filterNameTags))
		for i, f := range filterNameTags {
			tagQuery.FilterNameTags[i] = []byte(f)
		}

		tagQueries = append(tagQueries, tagQuery)
	}

	return tagQueries, nameOnly, nil
}

func parseTagCompletionQueries(r *http.Request) ([]string, error) {
	queries, ok := r.URL.Query()[queryParam]
	if !ok || len(queries) == 0 || queries[0] == "" {
		return nil, errors.ErrNoQueryFound
	}

	return queries, nil
}

func parseTimeWithDefault(
	r *http.Request,
	key string,
	defaultTime time.Time,
) (time.Time, error) {
	if t := r.FormValue(key); t != "" {
		return util.ParseTimeString(t)
	}

	return defaultTime, nil
}

// ParseSeriesMatchQuery parses all params from the GET request.
func ParseSeriesMatchQuery(
	r *http.Request,
	tagOptions models.TagOptions,
) ([]*storage.FetchQuery, *xhttp.ParseError) {
	r.ParseForm()
	matcherValues := r.Form["match[]"]
	if len(matcherValues) == 0 {
		return nil, xhttp.NewParseError(errors.ErrInvalidMatchers, http.StatusBadRequest)
	}

	start, err := parseTimeWithDefault(r, "start", time.Time{})
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	end, err := parseTimeWithDefault(r, "end", time.Now())
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	queries := make([]*storage.FetchQuery, len(matcherValues))
	for i, s := range matcherValues {
		promMatchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		matchers, err := xpromql.LabelMatchersToModelMatcher(promMatchers, tagOptions)
		if err != nil {
			return nil, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		queries[i] = &storage.FetchQuery{
			Raw:         fmt.Sprintf("match[]=%s", s),
			TagMatchers: matchers,
			Start:       start,
			End:         end,
		}
	}

	return queries, nil
}

func renderNameOnlyTagCompletionResultsJSON(
	w io.Writer,
	results []storage.CompletedTag,
) error {
	jw := json.NewWriter(w)
	jw.BeginArray()

	for _, tag := range results {
		jw.WriteString(string(tag.Name))
	}

	jw.EndArray()

	return jw.Close()
}

func renderDefaultTagCompletionResultsJSON(
	w io.Writer,
	results []storage.CompletedTag,
) error {
	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("hits")
	jw.WriteInt(len(results))

	jw.BeginObjectField("tags")
	jw.BeginArray()

	for _, tag := range results {
		jw.BeginObject()

		jw.BeginObjectField("key")
		jw.WriteString(string(tag.Name))

		jw.BeginObjectField("values")
		jw.BeginArray()
		for _, value := range tag.Values {
			jw.WriteString(string(value))
		}
		jw.EndArray()

		jw.EndObject()
	}
	jw.EndArray()

	jw.EndObject()

	return jw.Close()
}

// RenderListTagResultsJSON renders list tag results to json format.
func RenderListTagResultsJSON(
	w io.Writer,
	result *storage.CompleteTagsResult,
) error {
	if !result.CompleteNameOnly {
		return errors.ErrWithNames
	}

	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	jw.BeginObjectField("data")
	jw.BeginArray()

	for _, t := range result.CompletedTags {
		jw.WriteString(string(t.Name))
	}

	jw.EndArray()

	jw.EndObject()

	return jw.Close()
}

// RenderTagCompletionResultsJSON renders tag completion results to json format.
func RenderTagCompletionResultsJSON(
	w io.Writer, result storage.CompleteTagsResult) error {
	results := result.CompletedTags
	if result.CompleteNameOnly {
		return renderNameOnlyTagCompletionResultsJSON(w, results)
	}

	return renderDefaultTagCompletionResultsJSON(w, results)
}

// RenderTagValuesResultsJSON renders tag values results to json format.
func RenderTagValuesResultsJSON(
	w io.Writer,
	result *storage.CompleteTagsResult,
) error {
	if result.CompleteNameOnly {
		return errors.ErrNamesOnly
	}

	tagCount := len(result.CompletedTags)

	if tagCount > 1 {
		return errors.ErrMultipleResults
	}

	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	jw.BeginObjectField("data")
	jw.BeginArray()

	// if no tags found, return empty array
	if tagCount == 0 {
		jw.EndArray()

		jw.EndObject()

		return jw.Close()
	}

	values := result.CompletedTags[0].Values
	for _, value := range values {
		jw.WriteString(string(value))
	}

	jw.EndArray()

	jw.EndObject()

	return jw.Close()
}

// RenderSeriesMatchResultsJSON renders series match results to json format.
func RenderSeriesMatchResultsJSON(
	w io.Writer,
	results []models.Metrics,
	dropRole bool,
) error {
	jw := json.NewWriter(w)
	jw.BeginObject()

	jw.BeginObjectField("status")
	jw.WriteString("success")

	jw.BeginObjectField("data")
	jw.BeginArray()

	for _, result := range results {
		for _, tags := range result {
			jw.BeginObject()
			for _, tag := range tags.Tags.Tags {
				if bytes.Equal(tag.Name, roleName) && dropRole {
					// NB: When data is written from Prometheus remote write, additional
					// `"role":"remote"` tag is added, which should not be included in the
					// results.
					continue
				}
				jw.BeginObjectField(string(tag.Name))
				jw.WriteString(string(tag.Value))
			}

			jw.EndObject()
		}
	}

	jw.EndArray()
	jw.EndObject()

	return jw.Close()
}

// Response represents Prometheus's query response.
type Response struct {
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

type results []Result

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

// Result is the result itself.
type Result struct {
	// Metric is the tags for the result.
	Metric Tags `json:"metric"`
	// Values is the set of values for the result.
	Values Values `json:"values"`
	id     string
}

// Tags is a simple representation of Prometheus tags.
type Tags map[string]string

// Values is a list of values for the Prometheus result.
type Values []Value

// Value is a single value for Prometheus result.
type Value []interface{}

func (r *Result) genID() Result {
	tags := make(sort.StringSlice, len(r.Metric))
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

// MatchInformation describes how well two responses match.
type MatchInformation struct {
	// FullMatch indicates a full match.
	FullMatch bool
	// NoMatch indicates that the responses do not match sufficiently.
	NoMatch bool
}

// Matches compares two responses and determines how closely they match.
func (p Response) Matches(other Response) (MatchInformation, error) {
	if p.Status != other.Status {
		err := fmt.Errorf("status %s does not match other status %s",
			p.Status, other.Status)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return p.Data.matches(other.Data)
}

func (d data) matches(other data) (MatchInformation, error) {
	if d.ResultType != other.ResultType {
		err := fmt.Errorf("result type %s does not match other result type %s",
			d.ResultType, other.ResultType)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return d.Result.matches(other.Result)
}

func (r results) matches(other results) (MatchInformation, error) {
	if len(r) != len(other) {
		err := fmt.Errorf("result length %d does not match other result length %d",
			len(r), len(other))
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	r.Sort()
	other.Sort()
	for i, result := range r {
		if err := result.matches(other[i]); err != nil {
			return MatchInformation{
				NoMatch: true,
			}, err
		}
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r Result) matches(other Result) error {
	// NB: tags should match by here so this is more of a sanity check.
	if err := r.Metric.matches(other.Metric); err != nil {
		return err
	}

	return r.Values.matches(other.Values)
}

func (t Tags) matches(other Tags) error {
	if len(t) != len(other) {
		return fmt.Errorf("tag length %d does not match other tag length %d",
			len(t), len(other))
	}

	for k, v := range t {
		if vv, ok := other[k]; ok {
			if v != vv {
				return fmt.Errorf("tag %s does not match other tag length %s", v, vv)
			}
		} else {
			return fmt.Errorf("tag %s not found in other tagset", v)
		}
	}

	return nil
}

func (v Values) matches(other Values) error {
	if len(v) != len(other) {
		return fmt.Errorf("values length %d does not match other values length %d",
			len(v), len(other))
	}

	for i, val := range v {
		if err := val.matches(other[i]); err != nil {
			return err
		}
	}

	return nil
}

func (v Value) matches(other Value) error {
	if len(v) != 2 {
		return fmt.Errorf("value length %d must be 2", len(v))
	}

	if len(other) != 2 {
		return fmt.Errorf("other value length %d must be 2", len(other))
	}

	tsV := fmt.Sprint(v[0])
	tsOther := fmt.Sprint(v[0])
	if tsV != tsOther {
		return fmt.Errorf("ts %s does not match other ts %s", tsV, tsOther)
	}

	valV, err := strconv.ParseFloat(fmt.Sprint(v[1]), 64)
	if err != nil {
		return err
	}

	valOther, err := strconv.ParseFloat(fmt.Sprint(other[1]), 64)
	if err != nil {
		return err
	}

	if math.Abs(valV-valOther) > tolerance {
		return fmt.Errorf("point %f does not match other point %f", valV, valOther)
	}

	for i, val := range v {
		otherVal := other[i]
		if val != otherVal {
		}
	}

	return nil
}

// PromDebug represents the input and output that are used in the debug endpoint.
type PromDebug struct {
	Input   Response `json:"input"`
	Results Response `json:"results"`
}

// FilterSeriesByOptions removes series tags based on options.
func FilterSeriesByOptions(
	series []*ts.Series,
	opts *storage.FetchOptions,
) []*ts.Series {
	if opts == nil {
		return series
	}

	keys := opts.RestrictQueryOptions.GetRestrictByTag().GetFilterByNames()
	if len(keys) > 0 {
		for i, s := range series {
			series[i].Tags = s.Tags.TagsWithoutKeys(keys)
		}
	}

	return series
}
