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
	"strings"
	"time"

	"github.com/m3db/m3/src/query/errors"
	"github.com/m3db/m3/src/query/graphite/graphite"
	graphitestorage "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/util/json"
	xerrors "github.com/m3db/m3/src/x/errors"
	xtime "github.com/m3db/m3/src/x/time"
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

	matchers, queryType, err := graphitestorage.TranslateQueryToMatchersWithTerminator(query)
	if err != nil {
		return nil, nil, "",
			xerrors.NewInvalidParamsError(fmt.Errorf("invalid 'query': %s", query))
	}

	switch queryType {
	case graphitestorage.StarStarUnterminatedTranslatedQuery:
		// Translated query for "**" has unterminated search for children
		// terms, we are only going to do a single search and assume all
		// results that come back have also children in the graphite path
		// tree (since it's very expensive to check if each result that comes
		// back is a child or leaf node and "**" in a find query is typically
		// only used for template variables rather than searching for metric
		// results, which is the only use case isLeaf/hasChildren is useful).
		// Note: Filter to all graphite tags that appears at the last node
		// or greater than that (we use 100 as an arbitrary upper bound).
		maxPathIndexes := 100
		filter := make([][]byte, 0, maxPathIndexes)
		parts := 1 + strings.Count(query, ".")
		firstPathIndex := parts - 1
		for i := firstPathIndex; i < firstPathIndex+maxPathIndexes; i++ {
			filter = append(filter, graphite.TagName(i))
		}
		childQuery := &storage.CompleteTagsQuery{
			CompleteNameOnly: false,
			FilterNameTags:   filter,
			TagMatchers:      matchers,
			Start:            xtime.ToUnixNano(from),
			End:              xtime.ToUnixNano(until),
		}
		return nil, childQuery, query, nil
	case graphitestorage.TerminatedTranslatedQuery:
		// Default type of translated query, explicitly craft queries for
		// a terminated part of the query and a child part of the query.
		break
	default:
		return nil, nil, "", fmt.Errorf("unknown query type: %v", queryType)
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
		Start:            xtime.ToUnixNano(from),
		End:              xtime.ToUnixNano(until),
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
		Start:            xtime.ToUnixNano(from),
		End:              xtime.ToUnixNano(until),
	}

	return terminatedQuery, childQuery, query, nil
}

type findResultsJSONOptions struct {
	includeBothExpandableAndLeaf bool
}

func findResultsJSON(
	w io.Writer,
	prefix string,
	tags map[string]nodeDescriptor,
	opts findResultsJSONOptions,
) error {
	jw := json.NewWriter(w)
	jw.BeginArray()

	for value, descriptor := range tags {
		writeFindNodeResultJSON(jw, prefix, value, descriptor, opts)
	}

	jw.EndArray()
	return jw.Close()
}

func writeFindNodeResultJSON(
	jw json.Writer,
	prefix string,
	value string,
	descriptor nodeDescriptor,
	opts findResultsJSONOptions,
) {
	id := fmt.Sprintf("%s%s", prefix, value)

	// Include the leaf node only if no leaf was specified or
	// if config optionally sets that both should come back.
	// The default behavior matches graphite web.
	includeLeafNode := (descriptor.isLeaf && !descriptor.hasChildren) ||
		(descriptor.isLeaf &&
			descriptor.hasChildren &&
			opts.includeBothExpandableAndLeaf)
	if includeLeafNode {
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
	leaf := 1
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
