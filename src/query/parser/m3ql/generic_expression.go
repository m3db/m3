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

	"github.com/m3db/m3/src/query/ts"
)

// genericInterface represents a value with an arbitrary type
type genericInterface interface{}

type genericThunkFn func(context.Context, ts.SeriesList, genericInterface) (ts.SeriesList, error)

type genericExpression struct {
	name  string
	inner genericInterface
	thunk genericThunkFn
}

func newGenericExpression(n string, i genericInterface, f genericThunkFn) Expression {
	return &genericExpression{name: n, inner: i, thunk: f}
}

func (e *genericExpression) Execute(ctx context.Context, in ts.SeriesList) (ts.SeriesList, error) {
	return e.thunk(ctx, in, e.inner)
}

func (e *genericExpression) Clone() Expression {
	if exp, ok := e.inner.(Expression); ok {
		return newGenericExpression(e.name, exp.Clone(), e.thunk)
	}
	return newGenericExpression(e.name, e.inner, e.thunk)
}

func (e *genericExpression) String() string {
	return e.name
}
