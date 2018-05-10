// Copyright (c) 2016 Uber Technologies, Inc.
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

package convert

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3db/digest"
	"github.com/m3db/m3db/generated/thrift/rpc"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/x/xio"
	"github.com/m3db/m3db/x/xpool"
	"github.com/m3db/m3ninx/idx"
	"github.com/m3db/m3ninx/search"
	"github.com/m3db/m3ninx/search/query"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

var (
	errUnknownTimeType               = errors.New("unknown time type")
	errUnknownUnit                   = errors.New("unknown unit")
	errNilTaggedRequest              = errors.New("nil write tagged request")
	errDisjunctionQueriesUnsupported = errors.New("disjunction queries are not-supported")
	errInvalidNegationQuery          = errors.New("negation queries are not supported for composite queries")
	errUnsupportedQueryType          = errors.New("unsupported query type")

	timeZero time.Time
)

const (
	fetchTaggedTimeType = rpc.TimeType_UNIX_NANOSECONDS
)

// ToTime converts a value to a time
func ToTime(value int64, timeType rpc.TimeType) (time.Time, error) {
	unit, err := ToDuration(timeType)
	if err != nil {
		return timeZero, err
	}
	// NB(r): Doesn't matter what unit is if we have zero of them.
	if value == 0 {
		return timeZero, nil
	}
	return xtime.FromNormalizedTime(value, unit), nil
}

// ToValue converts a time to a value
func ToValue(t time.Time, timeType rpc.TimeType) (int64, error) {
	unit, err := ToDuration(timeType)
	if err != nil {
		return 0, err
	}
	return xtime.ToNormalizedTime(t, unit), nil
}

// ToDuration converts a time type to a duration
func ToDuration(timeType rpc.TimeType) (time.Duration, error) {
	unit, err := ToUnit(timeType)
	if err != nil {
		return 0, err
	}
	return unit.Value()
}

// ToUnit converts a time type to a unit
func ToUnit(timeType rpc.TimeType) (xtime.Unit, error) {
	switch timeType {
	case rpc.TimeType_UNIX_SECONDS:
		return xtime.Second, nil
	case rpc.TimeType_UNIX_MILLISECONDS:
		return xtime.Millisecond, nil
	case rpc.TimeType_UNIX_MICROSECONDS:
		return xtime.Microsecond, nil
	case rpc.TimeType_UNIX_NANOSECONDS:
		return xtime.Nanosecond, nil
	}
	return 0, errUnknownTimeType
}

// ToTimeType converts a unit to a time type
func ToTimeType(unit xtime.Unit) (rpc.TimeType, error) {
	switch unit {
	case xtime.Second:
		return rpc.TimeType_UNIX_SECONDS, nil
	case xtime.Millisecond:
		return rpc.TimeType_UNIX_MILLISECONDS, nil
	case xtime.Microsecond:
		return rpc.TimeType_UNIX_MICROSECONDS, nil
	case xtime.Nanosecond:
		return rpc.TimeType_UNIX_NANOSECONDS, nil
	}
	return 0, errUnknownUnit
}

// ToSegmentsResult is the result of a convert to segments call,
// if the segments were merged then checksum is ptr to the checksum
// otherwise it is nil.
type ToSegmentsResult struct {
	Segments *rpc.Segments
	Checksum *int64
}

// ToSegments converts a list of blocks to segments.
func ToSegments(blocks []xio.BlockReader) (ToSegmentsResult, error) {
	if len(blocks) == 0 {
		return ToSegmentsResult{}, nil
	}

	s := &rpc.Segments{}

	if len(blocks) == 1 {
		seg, err := blocks[0].Segment()
		if err != nil {
			return ToSegmentsResult{}, err
		}
		if seg.Len() == 0 {
			return ToSegmentsResult{}, nil
		}
		startTime := xtime.ToNormalizedTime(blocks[0].Start, time.Nanosecond)
		blockSize := xtime.ToNormalizedDuration(blocks[0].BlockSize, time.Nanosecond)
		s.Merged = &rpc.Segment{
			Head:      bytesRef(seg.Head),
			Tail:      bytesRef(seg.Tail),
			StartTime: &startTime,
			BlockSize: &blockSize,
		}
		checksum := int64(digest.SegmentChecksum(seg))
		return ToSegmentsResult{
			Segments: s,
			Checksum: &checksum,
		}, nil
	}

	for _, block := range blocks {
		seg, err := block.Segment()
		if err != nil {
			return ToSegmentsResult{}, err
		}
		if seg.Len() == 0 {
			continue
		}
		startTime := xtime.ToNormalizedTime(block.Start, time.Nanosecond)
		blockSize := xtime.ToNormalizedDuration(block.BlockSize, time.Nanosecond)
		s.Unmerged = append(s.Unmerged, &rpc.Segment{
			Head:      bytesRef(seg.Head),
			Tail:      bytesRef(seg.Tail),
			StartTime: &startTime,
			BlockSize: &blockSize,
		})
	}
	if len(s.Unmerged) == 0 {
		return ToSegmentsResult{}, nil
	}

	return ToSegmentsResult{Segments: s}, nil
}

func bytesRef(data checked.Bytes) []byte {
	if data != nil {
		return data.Bytes()
	}
	return nil
}

// ToRPCError converts a server error to a RPC error.
func ToRPCError(err error) *rpc.Error {
	if err == nil {
		return nil
	}
	if xerrors.IsInvalidParams(err) {
		return tterrors.NewBadRequestError(err)
	}
	return tterrors.NewInternalError(err)
}

// FetchTaggedConversionPools allows users to pass a pool for conversions.
type FetchTaggedConversionPools interface {
	// ID returns an ident.Pool
	ID() ident.Pool

	// CheckedBytesWrapperPool returns a CheckedBytesWrapperPool.
	CheckedBytesWrapper() xpool.CheckedBytesWrapperPool
}

// FromRPCFetchTaggedRequest converts the rpc request type for FetchTaggedRequest into corresponding Go API types.
func FromRPCFetchTaggedRequest(
	req *rpc.FetchTaggedRequest, pools FetchTaggedConversionPools,
) (ident.ID, index.Query, index.QueryOptions, bool, error) {
	start, rangeStartErr := ToTime(req.RangeStart, fetchTaggedTimeType)
	if rangeStartErr != nil {
		return nil, index.Query{}, index.QueryOptions{}, false, rangeStartErr
	}

	end, rangeEndErr := ToTime(req.RangeEnd, fetchTaggedTimeType)
	if rangeEndErr != nil {
		return nil, index.Query{}, index.QueryOptions{}, false, rangeEndErr
	}

	opts := index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
	}
	if l := req.Limit; l != nil {
		opts.Limit = int(*l)
	}

	q, err := fromRPCQuery(req.Query)
	if err != nil {
		return nil, index.Query{}, index.QueryOptions{}, false, err
	}

	var ns ident.ID
	if pools != nil {
		nsBytes := pools.CheckedBytesWrapper().Get(req.NameSpace)
		ns = pools.ID().BinaryID(nsBytes)
	} else {
		ns = ident.StringID(string(req.NameSpace))
	}
	return ns, index.Query{Query: q}, opts, req.FetchData, nil
}

// ToRPCFetchTaggedRequest converts the Go `client/` types into rpc request type for FetchTaggedRequest.
func ToRPCFetchTaggedRequest(
	ns ident.ID,
	q index.Query,
	opts index.QueryOptions,
	fetchData bool,
) (rpc.FetchTaggedRequest, error) {
	rangeStart, tsErr := ToValue(opts.StartInclusive, fetchTaggedTimeType)
	if tsErr != nil {
		return rpc.FetchTaggedRequest{}, tsErr
	}

	rangeEnd, tsErr := ToValue(opts.EndExclusive, fetchTaggedTimeType)
	if tsErr != nil {
		return rpc.FetchTaggedRequest{}, tsErr
	}

	query, queryErr := toRPCQuery(q.SearchQuery())
	if queryErr != nil {
		return rpc.FetchTaggedRequest{}, queryErr
	}

	request := rpc.FetchTaggedRequest{
		NameSpace:  ns.Bytes(),
		RangeStart: rangeStart,
		RangeEnd:   rangeEnd,
		FetchData:  fetchData,
		Query:      query,
	}

	if opts.Limit > 0 {
		l := int64(opts.Limit)
		request.Limit = &l
	}

	return request, nil
}

func fromRPCQuery(rpcQuery *rpc.IdxQuery) (idx.Query, error) {
	if rpcQuery == nil {
		return idx.Query{}, fmt.Errorf("nil query provided")
	}

	if rpcQuery.Operator != rpc.BooleanOperator_AND_OPERATOR {
		return idx.Query{}, fmt.Errorf("only AND queries are supported, received: %s", rpcQuery.Operator.String())
	}

	if len(rpcQuery.Filters) != 0 && len(rpcQuery.SubQueries) != 0 {
		return idx.Query{}, fmt.Errorf("illegal query composed of filters and sub-queries: %+v", *rpcQuery)
	}

	if l := len(rpcQuery.Filters); l != 0 {
		queries := make([]idx.Query, 0, l)
		for _, f := range rpcQuery.Filters {
			var (
				query idx.Query
				err   error
			)

			if f.Regexp {
				query, err = idx.NewRegexpQuery(f.TagName, f.TagValueFilter)
				if err != nil {
					return idx.Query{}, err
				}
			} else {
				query = idx.NewTermQuery(f.TagName, f.TagValueFilter)
			}

			if f.Negate {
				query = idx.NewNegationQuery(query)
			}

			queries = append(queries, query)
		}
		return idx.NewConjunctionQuery(queries...), nil
	}

	if l := len(rpcQuery.SubQueries); l != 0 {
		queries := make([]idx.Query, 0, l)
		for _, q := range rpcQuery.SubQueries {
			cq, err := fromRPCQuery(q)
			if err != nil {
				return idx.Query{}, err
			}
			queries = append(queries, cq)
		}
		return idx.NewConjunctionQuery(queries...), nil
	}

	return idx.Query{}, fmt.Errorf("at least one Filter/Sub-Query must be defined")
}

func toRPCQuery(searchQuery search.Query) (*rpc.IdxQuery, error) {
	switch q := searchQuery.(type) {
	case *query.TermQuery:
		return &rpc.IdxQuery{
			Operator: rpc.BooleanOperator_AND_OPERATOR,
			Filters: []*rpc.IdxTagFilter{
				&rpc.IdxTagFilter{
					TagName:        q.Field,
					TagValueFilter: q.Term,
				},
			},
		}, nil
	case *query.RegexpQuery:
		return &rpc.IdxQuery{
			Operator: rpc.BooleanOperator_AND_OPERATOR,
			Filters: []*rpc.IdxTagFilter{
				&rpc.IdxTagFilter{
					TagName:        q.Field,
					TagValueFilter: q.Regexp,
					Regexp:         true,
				},
			},
		}, nil
	case *query.NegationQuery:
		switch inner := q.Query.(type) {
		case *query.TermQuery, *query.RegexpQuery:
			iq, err := toRPCQuery(inner)
			if err != nil {
				return nil, err
			}
			iq.Filters[0].Negate = true
			return iq, nil
		}
		return nil, errInvalidNegationQuery
	case *query.ConjuctionQuery:
		ret := &rpc.IdxQuery{
			Operator: rpc.BooleanOperator_AND_OPERATOR,
		}
		for _, qq := range q.Queries {
			convertedQuery, err := toRPCQuery(qq)
			if err != nil {
				return nil, err
			}
			ret.SubQueries = append(ret.SubQueries, convertedQuery)
		}
		return ret, nil
	case *query.DisjuctionQuery:
		return nil, errDisjunctionQueriesUnsupported
	}
	// should never happen
	return nil, errUnsupportedQueryType
}

// ToTagsIter returns a tag iterator over the given request.
func ToTagsIter(r *rpc.WriteTaggedRequest) (ident.TagIterator, error) {
	if r == nil {
		return nil, errNilTaggedRequest
	}

	return &writeTaggedIter{
		rawRequest: r,
		currentIdx: -1,
	}, nil
}

// NB(prateek): writeTaggedIter is in-efficient in how it handles internal
// allocations. Only use it for non-performance critical RPC endpoints.
type writeTaggedIter struct {
	rawRequest *rpc.WriteTaggedRequest
	currentIdx int
	currentTag ident.Tag
}

func (w *writeTaggedIter) Next() bool {
	w.release()
	w.currentIdx++
	if w.currentIdx < len(w.rawRequest.Tags) {
		w.currentTag.Name = ident.StringID(w.rawRequest.Tags[w.currentIdx].Name)
		w.currentTag.Value = ident.StringID(w.rawRequest.Tags[w.currentIdx].Value)
		return true
	}
	return false
}

func (w *writeTaggedIter) release() {
	if i := w.currentTag.Name; i != nil {
		w.currentTag.Name.Finalize()
		w.currentTag.Name = nil
	}
	if i := w.currentTag.Value; i != nil {
		w.currentTag.Value.Finalize()
		w.currentTag.Value = nil
	}
}

func (w *writeTaggedIter) Current() ident.Tag {
	return w.currentTag
}

func (w *writeTaggedIter) Err() error {
	return nil
}

func (w *writeTaggedIter) Close() {
	w.release()
	w.currentIdx = -1
}

func (w *writeTaggedIter) Remaining() int {
	if r := len(w.rawRequest.Tags) - 1 - w.currentIdx; r >= 0 {
		return r
	}
	return 0
}

func (w *writeTaggedIter) Duplicate() ident.TagIterator {
	return &writeTaggedIter{
		rawRequest: w.rawRequest,
		currentIdx: -1,
	}
}
