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

package linear

import (
	"fmt"
	"math"
	"time"

	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/functions/lazy"
	"github.com/m3db/m3/src/query/parser"
)

const (
	// DayOfMonthType returns the day of the month for each of the given times
	// in UTC.
	// Returned values are from 1 to 31.
	DayOfMonthType = "day_of_month"

	// DayOfWeekType returns the day of the week for each of the given times
	// in UTC.
	// Returned values are from 0 to 6, where 0 means Sunday etc.
	DayOfWeekType = "day_of_week"

	// DaysInMonthType returns number of days in the month for each of the given
	// times in UTC.
	// Returned values are from 28 to 31.
	DaysInMonthType = "days_in_month"

	// HourType returns the hour of the day for each of the given times in UTC.
	// Returned values are from 0 to 23.
	HourType = "hour"

	// MinuteType returns the minute of the hour for each of the given times
	// in UTC.
	// Returned values are from 0 to 59.
	MinuteType = "minute"

	// MonthType returns the month of the year for each of the given times in UTC.
	// Returned values are from 1 to 12, where 1 means January etc.
	MonthType = "month"

	// YearType returns the year for each of the given times in UTC.
	YearType = "year"
)

type timeFn func(time.Time) float64

var (
	datetimeFuncs = map[string]timeFn{
		DayOfMonthType: func(t time.Time) float64 { return float64(t.Day()) },
		DayOfWeekType:  func(t time.Time) float64 { return float64(t.Weekday()) },
		DaysInMonthType: func(t time.Time) float64 {
			return float64(32 - time.Date(t.Year(), t.Month(),
				32, 0, 0, 0, 0, time.UTC).Day())
		},
		HourType:   func(t time.Time) float64 { return float64(t.Hour()) },
		MinuteType: func(t time.Time) float64 { return float64(t.Minute()) },
		MonthType:  func(t time.Time) float64 { return float64(t.Month()) },
		YearType:   func(t time.Time) float64 { return float64(t.Year()) },
	}
)

func buildTransform(fn timeFn, usingSeries bool) block.ValueTransform {
	if !usingSeries {
		return func(v float64) float64 {
			return fn(time.Now())
		}
	}

	return func(v float64) float64 {
		if math.IsNaN(v) {
			return v
		}

		t := time.Unix(int64(v), 0).UTC()
		return fn(t)
	}
}

// NewDateOp creates a new date op based on the type.
func NewDateOp(opType string, usingSeries bool) (parser.Params, error) {
	if dateFn, ok := datetimeFuncs[opType]; ok {
		fn := buildTransform(dateFn, usingSeries)
		lazyOpts := block.NewLazyOptions().SetValueTransform(fn)
		return lazy.NewLazyOp(opType, lazyOpts)
	}

	return nil, fmt.Errorf("unknown date type: %s", opType)
}
