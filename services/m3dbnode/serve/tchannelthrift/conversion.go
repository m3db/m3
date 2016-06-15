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

package tchannelthrift

import (
	"errors"
	"time"

	"github.com/m3db/m3db/services/m3dbnode/serve/tchannelthrift/thrift/gen-go/rpc"
	xtime "github.com/m3db/m3db/x/time"
)

var (
	errUnknownTimeType = errors.New("unknown time type")
	timeZero           time.Time
)

func valueToTime(value int64, timeType rpc.TimeType) (time.Time, error) {
	unit, err := timeTypeToDuration(timeType)
	if err != nil {
		return timeZero, err
	}
	return xtime.FromNormalizedTime(value, unit), nil
}

func timeToValue(t time.Time, timeType rpc.TimeType) (int64, error) {
	unit, err := timeTypeToDuration(timeType)
	if err != nil {
		return 0, err
	}
	return xtime.ToNormalizedTime(t, unit), nil
}

func timeTypeToDuration(timeType rpc.TimeType) (time.Duration, error) {
	unit, err := timeTypeToUnit(timeType)
	if err != nil {
		return 0, err
	}
	return unit.Value()
}

func timeTypeToUnit(timeType rpc.TimeType) (xtime.Unit, error) {
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
