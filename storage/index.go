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

package storage

import (
	"errors"

	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
)

var (
	errIndexingNotImplemented = errors.New("indexing is not implemented")
)

type dbIndex struct {
	opts Options
}

func newDatabaseIndex(o Options) (databaseIndex, error) {
	return &dbIndex{
		opts: o,
	}, nil
}

func (i *dbIndex) Write(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	tags ident.TagIterator,
) error {
	return errIndexingNotImplemented
}

func (i *dbIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	return index.QueryResults{}, errIndexingNotImplemented
}

type databaseIndexWriter interface {
	Write(
		ctx context.Context,
		namespace ident.ID,
		id ident.ID,
		tags ident.TagIterator,
	) error
}

type databaseIndexWriteFn func(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	tags ident.TagIterator,
) error

func (fn databaseIndexWriteFn) Write(
	ctx context.Context,
	namespace ident.ID,
	id ident.ID,
	tags ident.TagIterator,
) error {
	return fn(ctx, namespace, id, tags)
}

type dbIndexNoOp struct{}

func (n dbIndexNoOp) Write(context.Context, ident.ID, ident.ID, ident.TagIterator) error {
	return nil
}

func (n dbIndexNoOp) Query(context.Context, index.Query, index.QueryOptions) (index.QueryResults, error) {
	return index.QueryResults{}, nil
}

var databaseIndexNoOp databaseIndex = dbIndexNoOp{}
