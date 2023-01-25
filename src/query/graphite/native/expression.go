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
	"errors"
	"fmt"
	"os"
	"reflect"
	"strings"
	"time"

	"github.com/m3db/m3/src/query/graphite/common"
	"github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/graphite/ts"
	xerrors "github.com/m3db/m3/src/x/errors"

	"github.com/m3db/m3/src/x/instrument"
	"github.com/uber-go/tally"
)

var errTopLevelFunctionMustReturnTimeSeries = xerrors.NewInvalidParamsError(
	errors.New("top-level functions must return timeseries data"))

// An Expression is a metric query expression
type Expression interface {
	CallASTNode
	ASTNode
	// Executes the expression against the given context, and returns the resulting time series data
	Execute(ctx *common.Context) (ts.SeriesList, error)
}

// CallASTNode is an interface to help with printing the AST.
type CallASTNode interface {
	// Name returns the name of the call.
	Name() string
	// Arguments describe each argument that the call has, some
	// arguments that can either be a call or path expression.
	Arguments() []ASTNode
	// ReplaceArguments replaces the call's arguments with the given arguments.
	ReplaceArguments(args []ASTNode) error
	// FunctionInfo returns function info and metadata that the call has.
	FunctionInfo() FunctionInfo
	// String is the pretty printed format.
	String() string
}

// FunctionInfo is a struct that contains function info and metadata.
type FunctionInfo struct {
	// MultiFetchOptimizationDisabled describes whether the function can be
	// optimized when multiple path specs and fetches are used.
	// Some functions that rely on order must disable this optimization
	// since they require the series to be fetched in a specific order
	// to be used from within the function.
	MultiFetchOptimizationDisabled bool
}

// ASTNode is an interface to help with printing the AST.
type ASTNode interface {
	// PathExpression returns the path expression and true if argument
	// is a path.
	PathExpression() (string, bool)
	// CallExpression returns the call expression and true if argument
	// is a call.
	CallExpression() (CallASTNode, bool)
	// String is the pretty printed format.
	String() string
}

// A fetchExpression is an expression that fetches a bunch of data from storage based on a path expression
type fetchExpression struct {
	// The path expression to fetch
	pathArg        fetchExpressionPathArg
	instrumentOpts instrument.Options
}

type fetchExpressionPathArg struct {
	path string
}

func (a fetchExpressionPathArg) PathExpression() (string, bool) {
	return a.path, true
}

func (a fetchExpressionPathArg) CallExpression() (CallASTNode, bool) {
	return nil, false
}

func (a fetchExpressionPathArg) String() string {
	return a.path
}

// newFetchExpression creates a new fetch expression for a single path
func newFetchExpression(path string) *fetchExpression {
	return &fetchExpression{pathArg: fetchExpressionPathArg{path: path}}
}

func (f *fetchExpression) withInstrumentOpts(
	opts instrument.Options,
) *fetchExpression {
	f.instrumentOpts = opts
	return f
}

func (f *fetchExpression) metricsScope() tally.Scope {
	if f.instrumentOpts == nil {
		return tally.NoopScope
	}
	return f.instrumentOpts.MetricsScope().SubScope("fetch-expression")
}

func (f *fetchExpression) Name() string {
	return "fetch"
}

func (f *fetchExpression) Arguments() []ASTNode {
	return []ASTNode{f.pathArg}
}

func (f *fetchExpression) ReplaceArguments(args []ASTNode) error {
	return fmt.Errorf("cannot replace arguments on fetch expression")
}

func (f *fetchExpression) PathExpression() (string, bool) {
	return "", false
}

func (f *fetchExpression) CallExpression() (CallASTNode, bool) {
	return f, true
}

func (f *fetchExpression) FunctionInfo() FunctionInfo {
	return FunctionInfo{}
}

// Execute fetches results from storage
func (f *fetchExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	begin := time.Now()

	opts := storage.FetchOptions{
		StartTime: ctx.StartTime,
		EndTime:   ctx.EndTime,
		DataOptions: storage.DataOptions{
			Timeout: ctx.Timeout,
		},
		QueryFetchOpts: ctx.FetchOpts,
	}

	scope := f.metricsScope()
	scope.Counter("execute-fetch").Inc(1)

	if os.Getenv("M3_GRAPHITE_METRICS_ENABLE_DETAILED_FETCH_BREAKDOWN") == "true" {
		if ctx.QueryRaw == "" || ctx.QueryExpression == nil {
			scope.Counter("query-missing").Inc(1)
		} else if strings.Contains(ctx.QueryRaw, "sum") && strings.Contains(ctx.QueryRaw, "divide") {
			// Use pretty printed canonical version of query since sum and divide have aliased names
			// so make sure is definitely a sumseries and a divideseries query.
			queryExpr := ctx.QueryExpression.String()
			if strings.Contains(ctx.QueryRaw, "sumSeries") && strings.Contains(ctx.QueryRaw, "divideSeries") {
				scope.Tagged(map[string]string{
					// Use pretty printend canonical version of query in metric tag
					// since will be really high cardinality if same versions of the query is sent
					// but with slightly different formatting.
					"query_expression": queryExpr,
				}).Counter("query-with-sumseries-divideseries").Inc(1)
			} else {
				scope.Counter("query-without-sumseries-divideseries").Inc(1)
			}
		} else {
			scope.Counter("query-without-sumseries-divideseries").Inc(1)
		}
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

func (f *fetchExpression) Type() reflect.Type {
	return reflect.ValueOf(f).Type()
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
	if !(call.f.out == seriesListType || call.f.out == unaryContextShifterPtrType) {
		return nil, errTopLevelFunctionMustReturnTimeSeries
	}

	return &funcExpression{call: call}, nil
}

func (f *funcExpression) Name() string {
	return f.call.Name()
}

func (f *funcExpression) Arguments() []ASTNode {
	return f.call.Arguments()
}

func (f *funcExpression) ReplaceArguments(args []ASTNode) error {
	return f.call.ReplaceArguments(args)
}

// Execute evaluates the function and returns the result as a timeseries
func (f *funcExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	out, err := f.call.Evaluate(ctx)
	if err != nil {
		return ts.NewSeriesList(), err
	}

	return out.Interface().(ts.SeriesList), nil
}

func (f *funcExpression) PathExpression() (string, bool) {
	return "", false
}

func (f *funcExpression) CallExpression() (CallASTNode, bool) {
	return f, true
}

func (f *funcExpression) FunctionInfo() FunctionInfo {
	return f.call.f.info
}

func (f *funcExpression) String() string { return f.call.String() }

var _ ASTNode = noopExpression{}

// A noopExpression is an empty expression that returns nothing
type noopExpression struct{}

// Execute returns nothing
func (noop noopExpression) Execute(ctx *common.Context) (ts.SeriesList, error) {
	return ts.NewSeriesList(), nil
}

func (noop noopExpression) Name() string {
	return "noop"
}

func (noop noopExpression) Arguments() []ASTNode {
	return nil
}

func (noop noopExpression) ReplaceArguments(args []ASTNode) error {
	return fmt.Errorf("cannot replace arguments on fetch expression")
}

func (noop noopExpression) String() string {
	return noop.Name()
}

func (noop noopExpression) PathExpression() (string, bool) {
	return "", false
}

func (noop noopExpression) CallExpression() (CallASTNode, bool) {
	return noop, true
}

func (noop noopExpression) FunctionInfo() FunctionInfo {
	return FunctionInfo{}
}

var _ ASTNode = rootASTNode{}

// A rootASTNode is the root AST node which returns child nodes
// when parsing the grammar.
type rootASTNode struct {
	expr Expression
}

func (r rootASTNode) Name() string {
	return r.expr.Name()
}

func (r rootASTNode) Arguments() []ASTNode {
	return r.expr.Arguments()
}

func (r rootASTNode) String() string {
	return r.expr.(ASTNode).String()
}

func (r rootASTNode) PathExpression() (string, bool) {
	return "", false
}

func (r rootASTNode) CallExpression() (CallASTNode, bool) {
	return r.expr, true
}
