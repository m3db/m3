// Copyright (c) 2017 Uber Technologies, Inc.
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
	"testing"
	"time"

	"github.com/m3db/m3db/generated/thrift/rpc"
	"github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
)

func TestToTime(t *testing.T) {
	at := time.Unix(time.Now().Unix(), 0)

	v, err := ToTime(at.Unix(), rpc.TimeType_UNIX_SECONDS)
	assert.NoError(t, err)
	assert.True(t, v.Equal(at))

	_, err = ToTime(123, rpc.TimeType(999))
	assert.Error(t, err)
}

func TestToValue(t *testing.T) {
	at := time.Unix(time.Now().Unix(), 0)

	v, err := ToValue(at, rpc.TimeType_UNIX_SECONDS)
	assert.NoError(t, err)
	assert.Equal(t, at.Unix(), v)

	_, err = ToValue(at, rpc.TimeType(999))
	assert.Error(t, err)
}

func TestToDuration(t *testing.T) {
	expected := []struct {
		in     int64
		inType rpc.DurationType
		out    time.Duration
		err    error
	}{
		{3, rpc.DurationType_SECONDS, 3 * time.Second, nil},
		{4, rpc.DurationType_MILLISECONDS, 4 * time.Millisecond, nil},
		{5, rpc.DurationType_MICROSECONDS, 5 * time.Microsecond, nil},
		{6, rpc.DurationType_NANOSECONDS, 6 * time.Nanosecond, nil},
		{7, rpc.DurationType(999), 0, errUnknownDurationType},
	}

	for _, test := range expected {
		v, err := ToDuration(test.in, test.inType)
		if test.err != nil {
			assert.Error(t, err)
			assert.Equal(t, test.err, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, test.out, v)
	}
}

func TestToUnit(t *testing.T) {
	expected := []struct {
		in  rpc.TimeType
		out xtime.Unit
		err error
	}{
		{rpc.TimeType_UNIX_SECONDS, xtime.Second, nil},
		{rpc.TimeType_UNIX_MILLISECONDS, xtime.Millisecond, nil},
		{rpc.TimeType_UNIX_MICROSECONDS, xtime.Microsecond, nil},
		{rpc.TimeType_UNIX_NANOSECONDS, xtime.Nanosecond, nil},
		{rpc.TimeType(999), 0, errUnknownTimeType},
	}

	for _, test := range expected {
		v, err := ToUnit(test.in)
		if test.err != nil {
			assert.Error(t, err)
			assert.Equal(t, test.err, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, test.out, v)
	}
}

func TestToTimeType(t *testing.T) {
	expected := []struct {
		in  xtime.Unit
		out rpc.TimeType
		err error
	}{
		{xtime.Second, rpc.TimeType_UNIX_SECONDS, nil},
		{xtime.Millisecond, rpc.TimeType_UNIX_MILLISECONDS, nil},
		{xtime.Microsecond, rpc.TimeType_UNIX_MICROSECONDS, nil},
		{xtime.Nanosecond, rpc.TimeType_UNIX_NANOSECONDS, nil},
		{xtime.Unit(255), 0, errUnknownUnit},
	}

	for _, test := range expected {
		v, err := ToTimeType(test.in)
		if test.err != nil {
			assert.Error(t, err)
			assert.Equal(t, test.err, err)
		} else {
			assert.NoError(t, err)
		}
		assert.Equal(t, test.out, v)
	}
}
