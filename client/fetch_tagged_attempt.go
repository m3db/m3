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

package client

import (
	"github.com/m3db/m3db/encoding"
	"github.com/m3db/m3db/storage/index"
	xerrors "github.com/m3db/m3x/errors"
	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
	xretry "github.com/m3db/m3x/retry"
)

var (
	fetchTaggedAttemptArgsZeroed fetchTaggedAttemptArgs
)

type fetchTaggedAttempt struct {
	args    fetchTaggedAttemptArgs
	session *session

	idsAttemptFn xretry.Fn
	idsResult    index.QueryResults

	dataAttemptFn        xretry.Fn
	dataResultIters      encoding.SeriesIterators
	dataResultExhaustive bool
}

type fetchTaggedAttemptArgs struct {
	ns    ident.ID
	query index.Query
	opts  index.QueryOptions
}

func (f *fetchTaggedAttempt) reset() {
	f.args = fetchTaggedAttemptArgsZeroed

	f.idsResult = index.QueryResults{}
	f.dataResultIters = nil
	f.dataResultExhaustive = false
}

func (f *fetchTaggedAttempt) performIDsAttempt() error {
	result, err := f.session.fetchTaggedIDsAttempt(
		f.args.ns, f.args.query, f.args.opts)
	f.idsResult = result

	if IsBadRequestError(err) {
		// Do not retry bad request errors
		err = xerrors.NewNonRetryableError(err)
	}

	return err
}

func (f *fetchTaggedAttempt) performDataAttempt() error {
	var err error
	f.dataResultIters, f.dataResultExhaustive, err = f.session.fetchTaggedAttempt(
		f.args.ns, f.args.query, f.args.opts)
	if IsBadRequestError(err) {
		// Do not retry bad request errors
		err = xerrors.NewNonRetryableError(err)
	}
	return err
}

type fetchTaggedAttemptPool struct {
	pool    pool.ObjectPool
	session *session
}

func newFetchTaggedAttemptPool(
	session *session,
	opts pool.ObjectPoolOptions,
) *fetchTaggedAttemptPool {
	p := pool.NewObjectPool(opts)
	return &fetchTaggedAttemptPool{pool: p, session: session}
}

func (p *fetchTaggedAttemptPool) Init() {
	p.pool.Init(func() interface{} {
		f := &fetchTaggedAttempt{session: p.session}
		// NB(prateek): Bind fn once to avoid creating receiver
		// and function method pointer over and over again
		f.idsAttemptFn = f.performIDsAttempt
		f.dataAttemptFn = f.performDataAttempt
		f.reset()
		return f
	})
}

func (p *fetchTaggedAttemptPool) Get() *fetchTaggedAttempt {
	return p.pool.Get().(*fetchTaggedAttempt)
}

func (p *fetchTaggedAttemptPool) Put(f *fetchTaggedAttempt) {
	f.reset()
	p.pool.Put(f)
}
