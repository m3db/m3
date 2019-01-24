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

package native

import (
	"net/http"

	"github.com/m3db/m3/src/query/errors"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/x/net/http"
)

func parseSearchParamsToQuery(
	r *http.Request,
) (*storage.CompleteTagsQuery, *xhttp.ParseError) {
	values := r.URL.Query()
	query := values.Get("query")
	if query == "" {
		return nil, xhttp.NewParseError(errors.ErrNoQueryFound, http.StatusBadRequest)
	}

	filter := graphite.GetQueryTerminatorTagName(query)
	matchers := graphite.TranslateQueryToMatchers(query)
	return &storage.CompleteTagsQuery{
		TagMatchers:      matchers,
		FilterNameTags:   [][]byte{filter},
		CompleteNameOnly: false,
	}, nil
}
