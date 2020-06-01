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

package promql

import (
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/parser"
	pql "github.com/prometheus/prometheus/promql/parser"
)

// ParseFunctionExpr parses arguments to a function expression, returning
// a function, a bool indicating whether the function is a noop,
// and any errors during execution.
type ParseFunctionExpr func(
	name string,
	argValues []interface{},
	stringValues []string,
	hasArgValue bool,
	inner string,
	tagOptions models.TagOptions,
) (parser.Params, bool, error)

// ParseFn is a function that parses a query to a Prometheus expression.
type ParseFn func(query string) (pql.Expr, error)

func defaultParseFn(query string) (pql.Expr, error) {
	return pql.ParseExpr(query)
}

// ParseOptions are options for the Prometheus parser.
type ParseOptions interface {
	// ParseFn gets the parse function.
	ParseFn() ParseFn
	// SetParseFn sets the parse function.
	SetParseFn(f ParseFn) ParseOptions

	// FunctionParseExpr gets the parsing function.
	FunctionParseExpr() ParseFunctionExpr
	// SetFunctionParseExpr sets the parsing function.
	SetFunctionParseExpr(f ParseFunctionExpr) ParseOptions
}

type parseOptions struct {
	fn          ParseFn
	fnParseExpr ParseFunctionExpr
}

// NewParseOptions creates a new parse options.
func NewParseOptions() ParseOptions {
	return &parseOptions{
		fn:          defaultParseFn,
		fnParseExpr: NewFunctionExpr,
	}
}

func (o *parseOptions) ParseFn() ParseFn {
	return o.fn
}

func (o *parseOptions) SetParseFn(f ParseFn) ParseOptions {
	opts := *o
	opts.fn = f
	return &opts
}

func (o *parseOptions) FunctionParseExpr() ParseFunctionExpr {
	return o.fnParseExpr
}

func (o *parseOptions) SetFunctionParseExpr(f ParseFunctionExpr) ParseOptions {
	opts := *o
	opts.fnParseExpr = f
	return &opts
}
