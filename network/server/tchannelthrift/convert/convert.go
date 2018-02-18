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
	"github.com/m3db/m3ninx/index/segment"
	"github.com/m3db/m3x/checked"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"
)

var (
	errUnknownTimeType     = errors.New("unknown time type")
	errUnknownUnit         = errors.New("unknown unit")
	errIllegalLimit        = errors.New("illegal limit specified in query")
	errNilQuery            = errors.New("illegal query: nil")
	errUnequalTagNameValue = errors.New("un-equal length of tag names and tag values")
	timeZero               time.Time
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

// ToSegments converts a list of segment readers to segments.
func ToSegments(readers []xio.SegmentReader) (ToSegmentsResult, error) {
	if len(readers) == 0 {
		return ToSegmentsResult{}, nil
	}

	s := &rpc.Segments{}

	if len(readers) == 1 {
		seg, err := readers[0].Segment()
		if err != nil {
			return ToSegmentsResult{}, err
		}
		if seg.Len() == 0 {
			return ToSegmentsResult{}, nil
		}
		s.Merged = &rpc.Segment{
			Head: bytesRef(seg.Head),
			Tail: bytesRef(seg.Tail),
		}
		checksum := int64(digest.SegmentChecksum(seg))
		return ToSegmentsResult{
			Segments: s,
			Checksum: &checksum,
		}, nil
	}

	for _, reader := range readers {
		seg, err := reader.Segment()
		if err != nil {
			return ToSegmentsResult{}, err
		}
		if seg.Len() == 0 {
			continue
		}
		s.Unmerged = append(s.Unmerged, &rpc.Segment{
			Head: bytesRef(seg.Head),
			Tail: bytesRef(seg.Tail),
		})
	}
	if len(s.Unmerged) == 0 {
		return ToSegmentsResult{}, nil
	}

	return ToSegmentsResult{Segments: s}, nil
}

func bytesRef(data checked.Bytes) []byte {
	if data != nil {
		return data.Get()
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

// ToRPCTaggedResult converts an index.QueryResults -> appropriate RPC types.
func ToRPCTaggedResult(queryResult index.QueryResults) (*rpc.FetchTaggedResult_, error) {
	result := rpc.NewFetchTaggedResult_()
	result.Exhaustive = queryResult.Exhaustive
	// Make Elements an initialized empty array for JSON serialization as empty array than null
	result.Elements = make([]*rpc.FetchTaggedIDResult_, 0)
	iter := queryResult.Iterator
	for iter.Next() {
		ns, id, tags := iter.Current()
		elem := &rpc.FetchTaggedIDResult_{
			NameSpace: ns.String(),
			ID:        id.String(),
		}
		for _, t := range tags {
			// TODO(prateek): pool TagString and ...
			elem.Tags = append(elem.Tags, &rpc.TagString{
				Name:  t.Name.String(),
				Value: t.Value.String(),
			})
		}
	}

	if err := iter.Err(); err != nil {
		return nil, ToRPCError(err)
	}

	return result, nil
}

// ToIndexQueryOpts converts a FetchTaggedRequest to an `index.QueryOptions`.
func ToIndexQueryOpts(r *rpc.FetchTaggedRequest) (index.QueryOptions, error) {
	start, err := ToTime(r.RangeStart, r.RangeTimeType)
	if err != nil {
		return index.QueryOptions{}, err
	}
	end, err := ToTime(r.RangeEnd, r.RangeTimeType)
	if err != nil {
		return index.QueryOptions{}, err
	}
	limit := 0
	if r.Limit != nil {
		limit = int(*r.Limit)
		if limit < 0 {
			return index.QueryOptions{}, errIllegalLimit
		}
	}
	return index.QueryOptions{
		StartInclusive: start,
		EndExclusive:   end,
		Limit:          limit,
	}, nil
}

// ToIndexQuery converts a IdxQuery to an `index.Query`.
func ToIndexQuery(q *rpc.IdxQuery) (index.Query, error) {
	if q == nil {
		return index.Query{}, errNilQuery
	}

	if q.Operator != rpc.BooleanOperator_AND_OPERATOR {
		return index.Query{}, fmt.Errorf("illegal query operator: %v", q.Operator.String())
	}

	query := index.Query{
		segment.Query{
			Conjunction: segment.AndConjunction,
		},
	}

	for _, f := range q.Filters {
		filter, err := toIndexFilter(f)
		if err != nil {
			return index.Query{}, err
		}
		query.Filters = append(query.Filters, filter)
	}

	for _, s := range q.SubQueries {
		subQuery, err := ToIndexQuery(s)
		if err != nil {
			return index.Query{}, err
		}
		query.SubQueries = append(query.SubQueries, subQuery.Query)
	}

	return query, nil
}

func toIndexFilter(f *rpc.IdxTagFilter) (segment.Filter, error) {
	if f == nil {
		return segment.Filter{}, fmt.Errorf("illegal query filter: %v", f)
	}

	return segment.Filter{
		FieldName:        []byte(f.TagName),
		FieldValueFilter: []byte(f.TagValueFilter),
		Regexp:           f.Regexp,
		Negate:           f.Negate,
	}, nil
}

// ToTagsIter returns a tag iterator over the given request.
func ToTagsIter(r *rpc.WriteTaggedRequest) (ident.TagIterator, error) {
	if r == nil {
		return nil, errNilQuery
	}

	return &writeTaggedIter{
		rawRequest: r,
		currentIdx: -1,
	}, nil
}

// TODO(prateek): add tests for writeTaggedIter
type writeTaggedIter struct {
	rawRequest *rpc.WriteTaggedRequest
	currentIdx int
	currentTag ident.Tag
}

func (w *writeTaggedIter) Next() bool {
	w.release()
	w.currentIdx++
	if w.currentIdx < len(w.rawRequest.Tags) {
		// TODO(prateek): use an ident.Pool for writeTaggedIter allocations
		w.currentTag.Name = ident.StringID(w.rawRequest.Tags[w.currentIdx].Name)
		w.currentTag.Value = ident.StringID(w.rawRequest.Tags[w.currentIdx].Value)
		return true
	}
	return false
}

func (w *writeTaggedIter) release() {
	if i := w.currentTag.Name; i != nil {
		w.currentTag.Name = nil
		// w.identPool.Put(i)
	}
	if i := w.currentTag.Value; i != nil {
		w.currentTag.Value = nil
		// w.identPool.Put(i)
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

func (w *writeTaggedIter) Clone() ident.TagIterator {
	return &writeTaggedIter{
		rawRequest: w.rawRequest,
		currentIdx: -1,

		// identPool: w.identPool,
	}
}
