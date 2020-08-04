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
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	xpromql "github.com/m3db/m3/src/query/parser/promql"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/consolidators"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/json"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/golang/snappy"
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
	start, err := util.ParseTimeStringWithDefault(r.FormValue("start"),
		time.Unix(0, 0))
	if err != nil {
		return tagCompletionQueries, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	end, err := util.ParseTimeStringWithDefault(r.FormValue("end"),
		time.Now())
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

// ParseSeriesMatchQuery parses all params from the GET request.
func ParseSeriesMatchQuery(
	r *http.Request,
	parseOpts xpromql.ParseOptions,
	tagOptions models.TagOptions,
) ([]*storage.FetchQuery, *xhttp.ParseError) {
	r.ParseForm()
	matcherValues := r.Form["match[]"]
	if len(matcherValues) == 0 {
		return nil, xhttp.NewParseError(errors.ErrInvalidMatchers, http.StatusBadRequest)
	}

	start, err := util.ParseTimeStringWithDefault(r.FormValue("start"),
		time.Unix(0, 0))
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	end, err := util.ParseTimeStringWithDefault(r.FormValue("end"),
		time.Now())
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	queries := make([]*storage.FetchQuery, len(matcherValues))
	fn := parseOpts.MetricSelectorFn()
	for i, s := range matcherValues {
		promMatchers, err := fn(s)
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
	results []consolidators.CompletedTag,
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
	results []consolidators.CompletedTag,
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
	result *consolidators.CompleteTagsResult,
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
	w io.Writer, result consolidators.CompleteTagsResult) error {
	results := result.CompletedTags
	if result.CompleteNameOnly {
		return renderNameOnlyTagCompletionResultsJSON(w, results)
	}

	return renderDefaultTagCompletionResultsJSON(w, results)
}

// RenderTagValuesResultsJSON renders tag values results to json format.
func RenderTagValuesResultsJSON(
	w io.Writer,
	result *consolidators.CompleteTagsResult,
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
