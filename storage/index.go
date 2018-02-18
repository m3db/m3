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
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3ninx/doc"
	"github.com/m3db/m3ninx/index/segment/mem"
	"github.com/m3db/m3x/context"
	"github.com/m3db/m3x/ident"
)

type dbIndex struct {
	segment mem.Segment

	idPool ident.Pool

	opts  Options
	mopts mem.Options // TODO(prateek): migrate mem.Options -> Options
}

func newDatabaseIndex(o Options) (databaseIndex, error) {
	mopts := mem.NewOptions().
		SetInstrumentOptions(o.InstrumentOptions())

	seg, err := mem.New(1, mopts)
	if err != nil {
		return nil, err
	}

	return &dbIndex{
		segment: seg,

		idPool: o.IdentifierPool(),
		opts:   o,
		mopts:  mopts,
	}, nil
}

func (i *dbIndex) Write(
	namespace ident.ID,
	id ident.ID,
	tags ident.Tags,
) error {
	d := i.doc(namespace, id, tags)
	return i.segment.Insert(d)
}

func (i *dbIndex) Query(
	ctx context.Context,
	query index.Query,
	opts index.QueryOptions,
) (index.QueryResults, error) {
	// TODO(prateek): use QueryOptions to restrict results
	iter, err := i.segment.Query(query.Query)
	if err != nil {
		return index.QueryResults{}, err
	}

	return index.QueryResults{
		Iterator:   index.NewIterator(iter, i.idPool),
		Exhaustive: true,
	}, nil
}

func (i *dbIndex) doc(ns, id ident.ID, tags ident.Tags) doc.Document {
	// TODO(prateek): need to figure out copy/release semantics for ident.ID cloning.
	// Could probably keep a clone registered to a context per mem.Segment. With it,
	// we could release the ids once the segment is cleared, without requiring any
	// clone semantics within m3ninx itself.
	nsCopy := i.idPool.Clone(ns)
	idCopy := i.idPool.Clone(id)
	fields := make([]doc.Field, 0, 1+len(tags))
	fields = append(fields, doc.Field{
		Name:      index.ReservedFieldNameNamespace,
		Value:     nsCopy.Data().Get(),
		ValueType: doc.StringValueType,
	})
	for j := 0; j < len(tags); j++ {
		t := tags[j]
		fields = append(fields, doc.Field{
			Name:      i.idPool.Clone(t.Name).Data().Get(),
			Value:     i.idPool.Clone(t.Value).Data().Get(),
			ValueType: doc.StringValueType,
		})
	}
	return doc.Document{
		ID:     idCopy.Data().Get(),
		Fields: fields,
	}
}

type databaseIndexWriteFn func(
	namespace ident.ID,
	id ident.ID,
	tags ident.Tags,
) error

type dbIndexNoOp struct{}

func (n dbIndexNoOp) Write(ident.ID, ident.ID, ident.Tags) error {
	return nil
}

func (n dbIndexNoOp) Query(context.Context, index.Query, index.QueryOptions) (index.QueryResults, error) {
	return index.QueryResults{}, nil
}

var databaseIndexNoOp databaseIndex = dbIndexNoOp{}

var databaseIndexNoOpWriteFn databaseIndexWriteFn = databaseIndexNoOp.Write
