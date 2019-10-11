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

package main

import (
	"context"
	"fmt"
	"net"

	"github.com/m3db/m3/src/dbnode/encoding"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/pools"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/test"
	"github.com/m3db/m3/src/query/tsdb/remote"
	"github.com/m3db/m3/src/x/instrument"
)

func main() {
	poolWrapper := pools.NewPoolsWrapper(pools.BuildIteratorPools())
	server := remote.NewGRPCServer(
		&querier{},
		models.QueryContextOptions{},
		poolWrapper,
		instrument.NewOptions(),
	)

	addr := "0.0.0.0:9000"
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		fmt.Printf("listener err: %s", err.Error())
		return
	}

	if err := server.Serve(listener); err != nil {
		fmt.Printf("serve err: %s", err.Error())
	}
}

type querier struct{}

var _ m3.Querier = &querier{}

// FetchCompressed fetches timeseries data based on a query.
func (q *querier) FetchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) (encoding.SeriesIterators, m3.Cleanup, error) {
	iter, err := test.BuildTestSeriesIterator("foo")
	if err != nil {
		return nil, nil, err
	}

	iters := encoding.NewSeriesIterators([]encoding.SeriesIterator{iter}, nil)
	cleanup := func() error {
		iters.Close()
		return nil
	}

	return iters, cleanup, nil
}

// SearchCompressed fetches matching tags based on a query.
func (q *querier) SearchCompressed(
	ctx context.Context,
	query *storage.FetchQuery,
	options *storage.FetchOptions,
) ([]m3.MultiTagResult, m3.Cleanup, error) {
	return nil, nil, nil
}

// CompleteTagsCompressed returns autocompleted tag results.
func (q *querier) CompleteTagsCompressed(
	ctx context.Context,
	query *storage.CompleteTagsQuery,
	options *storage.FetchOptions,
) (*storage.CompleteTagsResult, error) {
	return nil, nil
}
