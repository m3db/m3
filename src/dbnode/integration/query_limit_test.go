// +build integration

// Copyright (c) 2020 Uber Technologies, Inc.
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

package integration

import (
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/dbnode/storage/limits"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"
)

func TestQueryLimitExceededError(t *testing.T) {
	testOpts, ns := newTestOptionsWithIndexedNamespace(t)
	testSetup := newTestSetupWithQueryLimits(t, testOpts)
	defer testSetup.Close()

	require.NoError(t, testSetup.StartServer())
	defer func() {
		require.NoError(t, testSetup.StopServer())
	}()

	var (
		nowFn     = testSetup.StorageOpts().ClockOptions().NowFn()
		end       = nowFn().Truncate(time.Hour)
		start     = end.Add(-time.Hour)
		query     = index.Query{Query: idx.NewTermQuery([]byte("tag"), []byte("value"))}
		queryOpts = index.QueryOptions{StartInclusive: start, EndExclusive: end}
	)

	session, err := testSetup.M3DBClient().DefaultSession()
	require.NoError(t, err)

	for i := 0; i < 2; i++ {
		var (
			metricName = fmt.Sprintf("metric_%v", i)
			tags       = ident.StringTag("tag", "value")
			timestamp  = nowFn().Add(-time.Minute * time.Duration(i+1))
		)
		session.WriteTagged(ns.ID(), ident.StringID(metricName),
			ident.NewTagsIterator(ident.NewTags(tags)), timestamp, 0.0, xtime.Second, nil)
	}

	_, _, err = session.FetchTagged(ns.ID(), query, queryOpts)
	require.True(t, client.IsResourceExhaustedError(err),
		"expected resource exhausted error, got: %v", err)
}

func newTestOptionsWithIndexedNamespace(t *testing.T) (TestOptions, namespace.Metadata) {
	idxOpts := namespace.NewIndexOptions().SetEnabled(true)
	nsOpts := namespace.NewOptions().SetIndexOptions(idxOpts)
	ns, err := namespace.NewMetadata(testNamespaces[0], nsOpts)
	require.NoError(t, err)

	testOpts := NewTestOptions(t).SetNamespaces([]namespace.Metadata{ns})
	return testOpts, ns
}

func newTestSetupWithQueryLimits(t *testing.T, opts TestOptions) TestSetup {
	storageLimitsFn := func(storageOpts storage.Options) storage.Options {
		queryLookback := limits.DefaultLookbackLimitOptions()
		queryLookback.Limit = 1
		queryLookback.Lookback = time.Hour

		limitOpts := limits.NewOptions().
			SetBytesReadLimitOpts(queryLookback).
			SetDocsLimitOpts(queryLookback).
			SetInstrumentOptions(storageOpts.InstrumentOptions())
		queryLimits, err := limits.NewQueryLimits(limitOpts)
		require.NoError(t, err)

		indexOpts := storageOpts.IndexOptions().SetQueryLimits(queryLimits)
		return storageOpts.SetIndexOptions(indexOpts)
	}

	setup, err := NewTestSetup(t, opts, nil, storageLimitsFn)
	require.NoError(t, err)

	return setup
}
