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

package native

import (
	"fmt"
	"reflect"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/errors"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
)

var (
	errTopLevelFunctionMustReturnTimeSeries = errors.NewInvalidParamsError(errors.New("top-level functions must return timeseries data"))
)

// An Expression is a metric query expression
type Expression interface {
	CallASTNode
	// Executes the expression against the given context, and returns the resulting time series data
	Execute(ctx *common.Context) (ts.SeriesList, error)
}

// CallASTNode is an interface to help with printing the AST.
type CallASTNode interface {
	// Name returns the name of the call.
	Name() string
	// Arguments describe each argument that the call has, some
	// arguments can be casted to an Call themselves.
	Arguments() []ArgumentASTNode
}

// ArgumentASTNode is an interface to help with printing the AST.
type ArgumentASTNode interface {
	String() string
}

// A fetchExpression is an expression that fetches a bunch of data from storage based on a path expression
type fetchExpression struct {
	// The path expression to fetch
	pathArg fetchExpressionPathArg
}

type fetchExpressionPathArg struct {
	path string
}

func (a fetchExpressionPathArg) String() string {
	return a.path
}

// newFetchExpression creates a new fetch expression for a single path
func newFetchExpression(path string) *fetchExpression {
	return &fetchExpression{pathArg: fetchExpressionPathArg{path: path}}
}

func (f *fetchExpression) Name() string {
	return "fetch"
}

func (f *fetchExpression) Arguments() []ArgumentASTNode {
	return []ArgumentASTNode{f.pathArg}
}

// Execute fetches results from storage
func (f *fetchExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	begin := time.Now()

	opts := storage.FetchOptions{
		StartTime: ctx.StartTime,
		EndTime:   ctx.EndTime,
		DataOptions: storage.DataOptions{
			Timeout: ctx.Timeout,
			Limit:   ctx.Limit,
		},
	}

	result, err := ctx.Engine.FetchByQuery(ctx, f.pathArg.path, opts)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	if ctx.TracingEnabled() {
		ctx.Trace(common.Trace{
			ActivityName: fmt.Sprintf("fetch %s", f.pathArg.path),
			Duration:     time.Since(begin),
			Outputs:      common.TraceStats{NumSeries: len(result.SeriesList)},
		})
	}

	for _, r := range result.SeriesList {
		r.Specification = f.pathArg.path
	}

	return ts.SeriesList{
		Values:   result.SeriesList,
		Metadata: result.Metadata,
	}, nil
}

// Evaluate evaluates the fetch and returns its results as a reflection value, allowing it to be used
// as an input argument to a function that takes a time series
func (f *fetchExpression) Evaluate(ctx *common.Context) (reflect.Value, error) {
	timeseries, err := f.Execute(ctx)
	if err != nil {
		return reflect.Value{}, err
	}

	return reflect.ValueOf(timeseries), nil
}

// CompatibleWith returns true if the reflected type is a time series or a generic interface.
func (f *fetchExpression) CompatibleWith(reflectType reflect.Type) bool {
	return reflectType == singlePathSpecType || reflectType == multiplePathSpecsType || reflectType == interfaceType
}

func (f *fetchExpression) String() string {
	return fmt.Sprintf("fetch(%s)", f.pathArg.path)
}

// A funcExpression is an expression that evaluates a function returning a timeseries
type funcExpression struct {
	call *functionCall
}

// newFuncExpression creates a new expressioon based on the given function call
func newFuncExpression(call *functionCall) (Expression, error) {
	if !(call.f.out == seriesListType || call.f.out == unaryContextShifterPtrType || call.f.out == binaryContextShifterPtrType) {
		return nil, errTopLevelFunctionMustReturnTimeSeries
	}

	return &funcExpression{call: call}, nil
}

func (f *funcExpression) Name() string {
	return f.call.Name()
}

func (f *funcExpression) Arguments() []ArgumentASTNode {
	return f.call.Arguments()
}

// Execute evaluates the function and returns the result as a timeseries
func (f *funcExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	out, err := f.call.Evaluate(ctx)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	return out.Interface().(ts.SeriesList), nil
}

func (f *funcExpression) String() string { return f.call.String() }

// A noopExpression is an empty expression that returns nothing
type noopExpression struct{}

// Execute returns nothing
func (noop noopExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	return ts.NewSeriesList(), nil
}

func (noop noopExpression) Name() string {
	return "noop"
}

func (noop noopExpression) Arguments() []ArgumentASTNode {
	return nil
}

func (noop noopExpression) String() string {
	return noop.Name()
}
