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

package tags

import (
	"fmt"
	"io"
	"net/http"

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/json"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/prometheus/common/route"
)

const (
	nameReplace         = "name"
	queryParam          = "query"
	filterNameTagsParam = "tag"
	formatErrStr        = "error parsing param: %s, error: %v"
)

// parseParamsToQuery parses all params from the GET request
func parseParamsToQuery(
	r *http.Request,
) (*storage.CompleteTagsQuery, *xhttp.ParseError) {
	tagQuery := storage.CompleteTagsQuery{}

	query, err := parseQuery(r)
	if err != nil {
		return nil, xhttp.NewParseError(fmt.Errorf(formatErrStr, queryParam, err), http.StatusBadRequest)
	}

	matchers, err := models.MatchersFromString(query)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	tagQuery.TagMatchers = matchers
	// If there is a result type field present, parse it and set
	// complete name only parameter appropriately. Otherwise, default
	// to returning both completed tag names and values
	if result := r.FormValue("result"); result != "" {
		switch result {
		case "default":
			tagQuery.CompleteNameOnly = false
		case "tagNamesOnly":
			tagQuery.CompleteNameOnly = true
		default:
			return nil, xhttp.NewParseError(errors.ErrInvalidResultParamError, http.StatusBadRequest)
		}
	}

	filterNameTags := r.Form[filterNameTagsParam]
	tagQuery.FilterNameTags = make([][]byte, len(filterNameTags))
	for i, f := range filterNameTags {
		tagQuery.FilterNameTags[i] = []byte(f)
	}

	return &tagQuery, nil
}

func parseTagValuesToQuery(
	r *http.Request,
) *storage.CompleteTagsQuery {
	ctx := r.Context()
	name := route.Param(ctx, nameReplace)
	return &storage.CompleteTagsQuery{
		CompleteNameOnly: true,
		FilterNameTags:   [][]byte{},
		TagMatchers: models.Matchers{
			models.Matcher{
				Type:  models.MatchEqual,
				Name:  []byte(name),
				Value: []byte{},
			},
		},
	}
}

func parseQuery(r *http.Request) (string, error) {
	queries, ok := r.URL.Query()[queryParam]
	if !ok || len(queries) == 0 || queries[0] == "" {
		return "", errors.ErrNoQueryFound
	}

	// TODO: currently, we only support one target at a time
	if len(queries) > 1 {
		return "", errors.ErrBatchQuery
	}

	return queries[0], nil
}

func renderNameOnlyResultsJSON(
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

func renderDefaultResultsJSON(
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

func renderResultsJSON(
	w io.Writer,
	result *storage.CompleteTagsResult,
) error {
	results := result.CompletedTags
	if result.CompleteNameOnly {
		return renderNameOnlyResultsJSON(w, results)
	}

	return renderDefaultResultsJSON(w, results)
}
