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
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	tterrors "github.com/m3db/m3db/network/server/tchannelthrift/errors"
	"github.com/m3db/m3db/x/io"
	"github.com/m3db/m3x/checked"
	"github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/time"
)

var (
	errUnknownTimeType = errors.New("unknown time type")
	errUnknownUnit     = errors.New("unknown unit")
	timeZero           time.Time
)

// ToTime converts a value to a time
func ToTime(value int64, timeType rpc.TimeType) (time.Time, error) {
	unit, err := ToDuration(timeType)
	if err != nil {
		return timeZero, err
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

// ToSegments converts a list of segment readers to segments.
func ToSegments(readers []xio.SegmentReader) (*rpc.Segments, error) {
	if len(readers) == 0 {
		return nil, nil
	}

	s := &rpc.Segments{}

	if len(readers) == 1 {
		seg, err := readers[0].Segment()
		if err != nil {
			return nil, err
		}
		if seg.Len() == 0 {
			return nil, nil
		}
		s.Merged = &rpc.Segment{
			Head: bytesRef(seg.Head),
			Tail: bytesRef(seg.Tail),
		}
		return s, nil
	}

	for _, reader := range readers {
		seg, err := reader.Segment()
		if err != nil {
			return nil, err
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
		return nil, nil
	}

	return s, nil
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
