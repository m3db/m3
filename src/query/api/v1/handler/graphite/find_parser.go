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

package graphite

import (
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/graphite/graphite"
	graphitestorage "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/json"
	xerrors "github.com/m3db/m3/src/x/errors"
)

// parseFindParamsToQueries parses an incoming request to two find queries,
// which are then combined to give the final result.
// It returns, in order:
//  the given query; this will return all values for exactly that tag which have
// _terminatedQuery, which adds an explicit terminator after the last term in
//  no child nodes
// _childQuery, which adds an explicit match all after the last term in the
// given query; this will return all values for exactly that tag which have at
// least one child node.
// _rawQueryString, which is the initial query request (bar final
// matcher), which  is used to reconstruct the return values.
// _err, any error encountered during parsing.
//
// As an example, given the query `a.b*`, and metrics `a.bar.c` and `a.biz`,
// terminatedQuery will return only [biz], and childQuery will return only
// [bar].
func parseFindParamsToQueries(r *http.Request) (
	_terminatedQuery *storage.CompleteTagsQuery,
	_childQuery *storage.CompleteTagsQuery,
	_rawQueryString string,
	_err error,
) {
	query := r.FormValue("query")
	if query == "" {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(errors.ErrNoQueryFound)
	}

	now := time.Now()
	fromString, untilString := r.FormValue("from"), r.FormValue("until")
	if len(fromString) == 0 {
		fromString = "0"
	}

	if len(untilString) == 0 {
		untilString = "now"
	}

	from, err := graphite.ParseTime(
		fromString,
		now,
		tzOffsetForAbsoluteTime,
	)

	if err != nil {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(fmt.Errorf("invalid 'from': %s", fromString))
	}

	until, err := graphite.ParseTime(
		untilString,
		now,
		tzOffsetForAbsoluteTime,
	)

	if err != nil {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(fmt.Errorf("invalid 'until': %s", untilString))
	}

	matchers, err := graphitestorage.TranslateQueryToMatchersWithTerminator(query)
	if err != nil {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(fmt.Errorf("invalid 'query': %s", query))
	}

	// NB: Filter will always be the second last term in the matchers, and the
	// matchers should always have a length of at least 2 (term + terminator)
	// so this is a sanity check and unexpected in actual execution.
	if len(matchers) < 2 {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(fmt.Errorf("unable to parse 'query': %s", query))
	}

	filter := [][]byte{matchers[len(matchers)-2].Name}
	terminatedQuery := &storage.CompleteTagsQuery{
		CompleteNameOnly: false,
		FilterNameTags:   filter,
		TagMatchers:      matchers,
		Start:            from,
		End:              until,
	}

	clonedMatchers := make([]models.Matcher, len(matchers))
	copy(clonedMatchers, matchers)
	// NB: change terminator from `MatchNotField` to `MatchField` to ensure
	// segments with children are matched.
	clonedMatchers[len(clonedMatchers)-1].Type = models.MatchField
	childQuery := &storage.CompleteTagsQuery{
		CompleteNameOnly: false,
		FilterNameTags:   filter,
		TagMatchers:      clonedMatchers,
		Start:            from,
		End:              until,
	}

	return terminatedQuery, childQuery, query, nil
}

func findResultsJSON(
	w io.Writer,
	prefix string,
	tags map[string]nodeDescriptor,
) error {
	jw := json.NewWriter(w)
	jw.BeginArray()

	for value, descriptor := range tags {
		writeFindNodeResultJSON(jw, prefix, value, descriptor)
	}

	jw.EndArray()
	return jw.Close()
}

func writeFindNodeResultJSON(
	jw json.Writer,
	prefix string,
	value string,
	descriptor nodeDescriptor,
) {
	id := fmt.Sprintf("%s%s", prefix, value)
	if descriptor.isLeaf {
		writeFindResultJSON(jw, id, value, false)
	}

	if descriptor.hasChildren {
		writeFindResultJSON(jw, id, value, true)
	}
}

func writeFindResultJSON(
	jw json.Writer,
	id string,
	value string,
	hasChildren bool,
) {
	var leaf = 1
	if hasChildren {
		leaf = 0
	}
	jw.BeginObject()

	jw.BeginObjectField("id")
	jw.WriteString(id)

	jw.BeginObjectField("text")
	jw.WriteString(value)

	jw.BeginObjectField("leaf")
	jw.WriteInt(leaf)

	jw.BeginObjectField("expandable")
	jw.WriteInt(1 - leaf)

	jw.BeginObjectField("allowChildren")
	jw.WriteInt(1 - leaf)

	jw.EndObject()
}
