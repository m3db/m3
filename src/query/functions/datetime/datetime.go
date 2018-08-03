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

package datetime

import (
	"fmt"
	"time"

	"github.com/m3db/m3db/src/query/block"
	"github.com/m3db/m3db/src/query/executor/transform"
)

const (
	// DayOfMonthType returns the day of the month for each of the given times in UTC.
	// Returned values are from 1 to 31.
	DayOfMonthType = "day_of_month"

	// DayOfWeekType returns the day of the week for each of the given times in UTC.
	// Returned values are from 0 to 6, where 0 means Sunday etc.
	DayOfWeekType = "day_of_week"

	// DaysInMonthType returns number of days in the month for each of the given times in UTC.
	// Returned values are from 28 to 31.
	DaysInMonthType = "days_in_month"

	// HourType returns the hour of the day for each of the given times in UTC.
	// Returned values are from 0 to 23.
	HourType = "hour"

	// MinuteType returns the minute of the hour for each of the given times in UTC.
	// Returned values are from 0 to 59.
	MinuteType = "minute"

	// MonthType returns the month of the year for each of the given times in UTC.
	// Returned values are from 1 to 12, where 1 means January etc.
	MonthType = "month"

	// YearType returns the year for each of the given times in UTC.
	YearType = "year"
)

var (
	datetimeFuncs = map[string]func(time.Time) float64{
		DayOfMonthType: func(t time.Time) float64 { return float64(t.Day()) },
		DayOfWeekType:  func(t time.Time) float64 { return float64(t.Weekday()) },
		DaysInMonthType: func(t time.Time) float64 {
			return float64(32 - time.Date(t.Year(), t.Month(), 32, 0, 0, 0, 0, time.UTC).Day())
		},
		HourType:   func(t time.Time) float64 { return float64(t.Hour()) },
		MinuteType: func(t time.Time) float64 { return float64(t.Minute()) },
		MonthType:  func(t time.Time) float64 { return float64(t.Month()) },
		YearType:   func(t time.Time) float64 { return float64(t.Year()) },
	}
)

// NewDateOp creates a new date op based on the type
func NewDateOp(optype string) (transform.Params, error) {
	if _, ok := datetimeFuncs[optype]; !ok {
		return emptyOp, fmt.Errorf("unknown date type: %s", optype)
	}

	return baseOp{
		operatorType: optype,
		processorFn:  newDateNode,
	}, nil
}

func newDateNode(op baseOp, controller *transform.Controller) Processor {
	return &dateNode{
		op:         op,
		controller: controller,
		dateFn:     datetimeFuncs[op.operatorType],
	}
}

type dateNode struct {
	op         baseOp
	controller *transform.Controller
	dateFn     func(time.Time) float64
}

func (c *dateNode) ProcessStep(values []float64, t time.Time) []float64 {
	for i := range values {
		values[i] = c.dateFn(t)
	}

	return values
}

func (c *dateNode) ProcessSeries(values []float64, bounds block.Bounds) []float64 {
	var t time.Time
	for i := range values {
		t, _ = bounds.TimeForIndex(i)
		values[i] = c.dateFn(t)
	}

	return values
}
