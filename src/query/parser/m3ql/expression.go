/*
 * Copyright (c) 2019 Uber Technologies, Inc.
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */

package m3ql

import (
	"context"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/ts"
)

// Expression is the basic building block of the Pipeline.
type Expression interface {
	// Executes the Expression against the given context and input, and returns the resulting time series data.
	Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error)
	Clone() Expression
}

// PreparedExpression must bootstrap itself concurrently with other PreparedExpressions
// before query execution.
type PreparedExpression interface {
	Expression
	Prepare(ctx context.Context) error
}

// ShiftingContextExpression can shift the time context for upstream
// pipeline expressions, the returned times will analyzed to see if they
// shifted the start time or the end time backwards and then cumulatively
// applied to the whole query with any other shifting expressions that appear.
// That is, if newStartTime that appears before startTime the start time will
// appear shifted into the past and similarly if it appears after startTime the
// start time will appear shifted into the future, the same rules apply for
// newEndTime.
type ShiftingContextExpression interface {
	Expression
	Shift(startTime, endTime time.Time) (newStartTime, newEndTime time.Time)
}

// ExpandableContextExpression can expand the time context for upstream
// pipeline expressions and child pipeline expressions, the returned
// durations will extend into the past and into the future if positive.
// That is a positive expandPast will move the start time back and a
// positive expandFuture will move the end time forward.
// The expansions for a query are collected and then the maximum of all
// requested expansions in either direction is applied to avoid multiple
// expansions stacking on top of each other.
type ExpandableContextExpression interface {
	Expression
	Expand(startTime, endTime time.Time) (expandPast, expandFuture time.Duration)
}

type constantExpression struct {
	name   string
	values []float64
	step   int
	tags   map[string]string
}

func newConstantExpression(name string, values []float64, step int) Expression {
	return &constantExpression{name, values, step, nil}
}

func newConstantExpressionWithTags(
	name string, values []float64, step int, tags map[string]string) Expression {

	m := make(map[string]string, len(tags))

	for k, v := range tags {
		m[k] = v
	}

	return &constantExpression{name, values, step, m}
}

func (e *constantExpression) Clone() Expression {
	return e // No mutable state.
}

func (e *constantExpression) Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error) {
	return nil, fmt.Errorf("not implemented")
}

func (e *constantExpression) String() string {
	return e.name
}

type boundFunction func(context.Context, ts.SeriesList) (ts.SeriesList, error)

// A functionExpression is an expression that evaluates a function returning a timeseries.
type functionExpression struct {
	name string
	fn   boundFunction
}

// newFunctionExpression creates a new expressioon based on the given function call.
func newFunctionExpression(name string, fn boundFunction) Expression {
	return &functionExpression{name, fn}
}

func (e *functionExpression) Clone() Expression {
	return e // No mutable state.
}

// Execute evaluates the function and returns the result as a timeseries.
func (e *functionExpression) Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error) {
	return e.fn(ctx, in)
}

func (e *functionExpression) String() string {
	return e.name
}

type shiftingContextFunc func(startTime, endTime time.Time) (newStartTime, newEndTime time.Time)

type shiftingContextExpression struct {
	*functionExpression
	shift shiftingContextFunc
}

func newShiftingContextExpression(name string, fn boundFunction, shift shiftingContextFunc) ShiftingContextExpression {
	return &shiftingContextExpression{
		functionExpression: &functionExpression{name: name, fn: fn},
		shift:              shift}
}

func (e *shiftingContextExpression) Clone() Expression {
	return e // No mutable state.
}

func (e *shiftingContextExpression) Shift(startTime, endTime time.Time) (time.Time, time.Time) {
	return e.shift(startTime, endTime)
}

type expandingContextFunc func(startTime, endTime time.Time) (startShift, endShift time.Duration)

type expandingContextExpression struct {
	*functionExpression
	expansion expandingContextFunc
}

func newExpandingContextExpression(name string, fn boundFunction, expansion expandingContextFunc) ExpandableContextExpression {
	return &expandingContextExpression{
		functionExpression: &functionExpression{name: name, fn: fn},
		expansion:          expansion}
}

func (e *expandingContextExpression) Clone() Expression {
	return e // No mutable state.
}

func (e *expandingContextExpression) Expand(startTime, endTime time.Time) (expandPast, expandFuture time.Duration) {
	return e.expansion(startTime, endTime)
}

// A noopExpression is an empty expression that returns nothing.
type noopExpression struct{}

func (e *noopExpression) Clone() Expression {
	return e // No mutable state.
}

// Execute returns nothing.
func (e *noopExpression) Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error) {
	return in, nil
}

func (e *noopExpression) String() string {
	return ""
}
