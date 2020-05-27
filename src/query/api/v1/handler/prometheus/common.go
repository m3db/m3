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
	"encoding/json"
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
	jsonutil "github.com/m3db/m3/src/query/util/json"
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
	var timeout string
	if v := r.FormValue("timeout"); v != "" {
		timeout = v
	}
	// Note: Header should take precedence.
	if v := r.Header.Get("timeout"); v != "" {
		timeout = v
	}

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

// TagCompletionQueries are tag completion queries.
type TagCompletionQueries struct {
	// Queries are the tag completion queries.
	Queries []*storage.CompleteTagsQuery
	// NameOnly indicates name only
	NameOnly bool
}

// ParseTagCompletionParamsToQueries parses all params from the GET request.
// Returns queries, a boolean indicating if the query completes names only, and
// any errors.
func ParseTagCompletionParamsToQueries(
	r *http.Request,
) (TagCompletionQueries, *xhttp.ParseError) {
	tagCompletionQueries := TagCompletionQueries{}
	start, err := parseTimeWithDefault(r, "start", time.Time{})
	if err != nil {
		return tagCompletionQueries, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	end, err := parseTimeWithDefault(r, "end", time.Now())
	if err != nil {
		return tagCompletionQueries, xhttp.NewParseError(err, http.StatusBadRequest)
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
			return tagCompletionQueries, xhttp.NewParseError(
				errors.ErrInvalidResultParamError, http.StatusBadRequest)
		}
	}

	tagCompletionQueries.NameOnly = nameOnly
	queries, err := parseTagCompletionQueries(r)
	if err != nil {
		return tagCompletionQueries, xhttp.NewParseError(
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
			return tagCompletionQueries, xhttp.NewParseError(err, http.StatusBadRequest)
		}

		tagQuery.TagMatchers = matchers
		filterNameTags := r.Form[filterNameTagsParam]
		tagQuery.FilterNameTags = make([][]byte, len(filterNameTags))
		for i, f := range filterNameTags {
			tagQuery.FilterNameTags[i] = []byte(f)
		}

		tagQueries = append(tagQueries, tagQuery)
	}

	tagCompletionQueries.Queries = tagQueries
	return tagCompletionQueries, nil
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
	jw := jsonutil.NewWriter(w)
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
	jw := jsonutil.NewWriter(w)
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

	jw := jsonutil.NewWriter(w)
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

	jw := jsonutil.NewWriter(w)
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
	jw := jsonutil.NewWriter(w)
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
	ResultType string `json:"resultType"`
	Result     result
}

type result interface {
	matches(other result) (MatchInformation, error)
}

type matrixResult struct {
	// Result is the list of matrixRow for the response.
	Result []matrixRow `json:"result"`
}

type vectorResult struct {
	// Result is the list of vectorItem for the response.
	Result []vectorItem `json:"result"`
}

type scalarResult struct {
	// Result is the scalar Value for the response.
	Result Value `json:"result"`
}

type stringResult struct {
	// Result is the string Value for the response.
	Result Value `json:"result"`
}

func (d *data) UnmarshalJSON(bytes []byte) error {
	var metaData struct {
		ResultType string `json:"resultType"`
	}
	if err := json.Unmarshal(bytes, &metaData); err != nil {
		return err
	}
	*d = data{ResultType: metaData.ResultType}

	switch metaData.ResultType {

	case "matrix":
		r := &matrixResult{}
		(*d).Result = r
		return json.Unmarshal(bytes, r)

	case "vector":
		r := &vectorResult{}
		(*d).Result = r
		return json.Unmarshal(bytes, r)

	case "scalar":
		r := &scalarResult{}
		(*d).Result = r
		return json.Unmarshal(bytes, r)

	case "string":
		r := &stringResult{}
		(*d).Result = r
		return json.Unmarshal(bytes, r)

	default:
		return fmt.Errorf("unknown resultType: %s, %s", metaData.ResultType, string(bytes))
	}
}

// Len is the number of elements in the collection.
func (r matrixResult) Len() int { return len(r.Result) }

// Less reports whether the element with
// index i should sort before the element with index j.
func (r matrixResult) Less(i, j int) bool {
	return r.Result[i].id < r.Result[j].id
}

// Swap swaps the elements with indexes i and j.
func (r matrixResult) Swap(i, j int) { r.Result[i], r.Result[j] = r.Result[j], r.Result[i] }

// Sort sorts the matrixResult.
func (r matrixResult) Sort() {
	for i, result := range r.Result {
		r.Result[i].id = result.Metric.genID()
	}

	sort.Sort(r)
}

// Len is the number of elements in the vector.
func (r vectorResult) Len() int { return len(r.Result) }

// Less reports whether the element with
// index i should sort before the element with index j.
func (r vectorResult) Less(i, j int) bool {
	return r.Result[i].id < r.Result[j].id
}

// Swap swaps the elements with indexes i and j.
func (r vectorResult) Swap(i, j int) { r.Result[i], r.Result[j] = r.Result[j], r.Result[i] }

// Sort sorts the vectorResult.
func (r vectorResult) Sort() {
	for i, result := range r.Result {
		r.Result[i].id = result.Metric.genID()
	}

	sort.Sort(r)
}

// matrixRow is a single row of "matrix" Result.
type matrixRow struct {
	// Metric is the tags for the matrixRow.
	Metric Tags `json:"metric"`
	// Values is the set of values for the matrixRow.
	Values Values `json:"values"`
	id     string
}

// vectorItem is a single item of "vector" Result.
type vectorItem struct {
	// Metric is the tags for the vectorItem.
	Metric Tags `json:"metric"`
	// Value is the value for the vectorItem.
	Value Value `json:"value"`
	id    string
}

// Tags is a simple representation of Prometheus tags.
type Tags map[string]string

// Values is a list of values for the Prometheus Result.
type Values []Value

// Value is a single value for Prometheus Result.
type Value []interface{}

func (t *Tags) genID() string {
	tags := make(sort.StringSlice, len(*t))
	for k, v := range *t {
		tags = append(tags, fmt.Sprintf("%s:%s,", k, v))
	}

	sort.Sort(tags)
	var sb strings.Builder
	// NB: this may clash but exact tag values are also checked, and this is a
	// validation endpoint so there's less concern over correctness.
	for _, t := range tags {
		sb.WriteString(t)
	}

	return sb.String()
}

// MatchInformation describes how well two responses match.
type MatchInformation struct {
	// FullMatch indicates a full match.
	FullMatch bool
	// NoMatch indicates that the responses do not match sufficiently.
	NoMatch bool
}

// Matches compares two responses and determines how closely they match.
func (r Response) Matches(other Response) (MatchInformation, error) {
	if r.Status != other.Status {
		err := fmt.Errorf("status %s does not match other status %s",
			r.Status, other.Status)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return r.Data.matches(other.Data)
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

func (r matrixResult) matches(other result) (MatchInformation, error) {
	otherMatrix, ok := other.(*matrixResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected matrixResult, %v", other)
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if len(r.Result) != len(otherMatrix.Result) {
		err := fmt.Errorf("result length %d does not match other result length %d",
			len(r.Result), len(otherMatrix.Result))
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	r.Sort()
	otherMatrix.Sort()
	for i, result := range r.Result {
		if err := result.matches(otherMatrix.Result[i]); err != nil {
			return MatchInformation{
				NoMatch: true,
			}, err
		}
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r vectorResult) matches(other result) (MatchInformation, error) {
	otherVector, ok := other.(*vectorResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected vectorResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if len(r.Result) != len(otherVector.Result) {
		err := fmt.Errorf("result length %d does not match other result length %d",
			len(r.Result), len(otherVector.Result))
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	r.Sort()
	otherVector.Sort()
	for i, result := range r.Result {
		if err := result.matches(otherVector.Result[i]); err != nil {
			return MatchInformation{
				NoMatch: true,
			}, err
		}
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r scalarResult) matches(other result) (MatchInformation, error) {
	otherScalar, ok := other.(*scalarResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected scalarResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if err := r.Result.matches(otherScalar.Result); err != nil {
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r stringResult) matches(other result) (MatchInformation, error) {
	otherString, ok := other.(*stringResult)
	if !ok {
		err := fmt.Errorf("incorrect type for matching, expected stringResult")
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	if err := r.Result.matches(otherString.Result); err != nil {
		return MatchInformation{
			NoMatch: true,
		}, err
	}

	return MatchInformation{FullMatch: true}, nil
}

func (r matrixRow) matches(other matrixRow) error {
	// NB: tags should match by here so this is more of a sanity check.
	if err := r.Metric.matches(other.Metric); err != nil {
		return err
	}

	return r.Values.matches(other.Values)
}

func (r vectorItem) matches(other vectorItem) error {
	// NB: tags should match by here so this is more of a sanity check.
	if err := r.Metric.matches(other.Metric); err != nil {
		return err
	}

	return r.Value.matches(other.Value)
}

func (t Tags) matches(other Tags) error {
	if len(t) != len(other) {
		return fmt.Errorf("tag length %d does not match other tag length %d",
			len(t), len(other))
	}

	for k, v := range t {
		if vv, ok := other[k]; ok {
			if v != vv {
				return fmt.Errorf("tag %s value %s does not match other tag value %s", k, v, vv)
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
