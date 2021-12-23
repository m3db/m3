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

package convert

import xtime "github.com/m3db/m3/src/x/time"

// UnitForM3DB converts a time unit into one that is suitable for m3db.
// The is done in an explicit way for better performance and the logic is
// ensured though unit test.
func UnitForM3DB(unit xtime.Unit) xtime.Unit {
	switch unit {
	case xtime.Second, xtime.Minute, xtime.Hour, xtime.Day, xtime.Year:
		return xtime.Second
	case xtime.Nanosecond, xtime.Millisecond, xtime.Microsecond:
		return unit
	default:
		return xtime.Nanosecond
	}
}
