// +build integration
//
// Copyright (c) 2016 Uber Technologies, Inc.
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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/m3db/m3db/client"
	"github.com/m3db/m3db/storage/index"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3ninx/idx"
	xclock "github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/ident"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// This test writes a larget number of unique series' with tags concurrently.
func TestIndexLargeCardinalityHighConcurrency(t *testing.T) {
	if testing.Short() {
		t.SkipNow() // Just skip if we're doing a short run
	}

	concurrency := 10
	writeEach := 100
	numTags := 10

	genIDTags := func(i int, j int) (ident.ID, ident.TagIterator) {
		id := fmt.Sprintf("foo.%d.%d", i, j)
		tags := make([]ident.Tag, 0, numTags)
		for i := 0; i < numTags; i++ {
			tags = append(tags, ident.StringTag(
				fmt.Sprintf("%s.tagname.%d", id, i),
				fmt.Sprintf("%s.tagvalue.%d", id, i),
			))
		}
		return ident.StringID(id), ident.NewTagSliceIterator(tags)
	}

	// Test setup
	md, err := namespace.NewMetadata(testNamespaces[0],
		namespace.NewOptions().
			SetRetentionOptions(defaultIntegrationTestRetentionOpts).
			SetCleanupEnabled(false).
			SetSnapshotEnabled(false).
			SetFlushEnabled(false))
	require.NoError(t, err)

	testOpts := newTestOptions(t).
		SetNamespaces([]namespace.Metadata{md}).
		SetIndexingEnabled(true).
		SetWriteNewSeriesAsync(true)
	testSetup, err := newTestSetup(t, testOpts, nil)
	require.NoError(t, err)
	defer testSetup.close()

	// Start the server
	log := testSetup.storageOpts.InstrumentOptions().Logger()
	require.NoError(t, testSetup.startServer())

	// Stop the server
	defer func() {
		require.NoError(t, testSetup.stopServer())
		log.Debug("server is now down")
	}()

	client := testSetup.m3dbClient
	session, err := client.DefaultSession()
	require.NoError(t, err)

	var (
		insertWg       sync.WaitGroup
		numTotalErrors uint32
	)
	now := testSetup.db.Options().ClockOptions().NowFn()()
	start := time.Now()
	log.Info("starting data write")

	for i := 0; i < concurrency; i++ {
		insertWg.Add(1)
		idx := i
		go func() {
			numErrors := uint32(0)
			for j := 0; j < writeEach; j++ {
				id, tags := genIDTags(idx, j)
				err := session.WriteTagged(md.ID(), id, tags, now, float64(1.0), xtime.Second, nil)
				if err != nil {
					numErrors++
				}
			}
			atomic.AddUint32(&numTotalErrors, numErrors)
			insertWg.Done()
		}()
	}

	insertWg.Wait()
	require.Zero(t, numTotalErrors)
	log.Infof("test data written in %v", time.Since(start))
	log.Infof("waiting to see if data is indexed")

	var (
		fetchWg sync.WaitGroup
	)
	for i := 0; i < concurrency; i++ {
		fetchWg.Add(1)
		idx := i
		go func() {
			id, tags := genIDTags(idx, writeEach-1)
			indexed := xclock.WaitUntil(func() bool {
				found := isIndexed(t, session, md.ID(), id, tags)
				return found
			}, 5*time.Second)
			assert.True(t, indexed)
			fetchWg.Done()
		}()
	}
	fetchWg.Wait()
	log.Infof("data is indexed in %v", time.Since(start))
}

func isIndexed(t *testing.T, s client.Session, ns ident.ID, id ident.ID, tags ident.TagIterator) bool {
	q := newQuery(t, tags)
	results, err := s.FetchTaggedIDs(ns, index.Query{q}, index.QueryOptions{
		StartInclusive: time.Now(),
		EndExclusive:   time.Now(),
		Limit:          10})
	if err != nil {
		return false
	}
	iter := results.Iterator
	if !iter.Next() {
		return false
	}
	cuNs, cuID, cuTag := iter.Current()
	if ns.String() != cuNs.String() {
		return false
	}
	if id.String() != cuID.String() {
		return false
	}
	return newTagIterMatcher(tags).Matches(cuTag)
}

func newQuery(t *testing.T, tags ident.TagIterator) idx.Query {
	tags = tags.Duplicate()
	filters := make([]idx.Query, 0, tags.Remaining())
	for tags.Next() {
		tag := tags.Current()
		tq := idx.NewTermQuery(tag.Name.Bytes(), tag.Value.Bytes())
		filters = append(filters, tq)
		break // TODO(prateek): remove this line once the fix for conjunction searchers is landed.
	}
	q, err := idx.NewConjunctionQuery(filters...)
	require.NoError(t, err)
	return q
}

func newTagIterMatcher(tags ident.TagIterator) ident.TagIterMatcher {
	tags = tags.Duplicate()
	vals := make([]string, 0, tags.Remaining()*2)
	for tags.Next() {
		t := tags.Current()
		vals = append(vals, t.Name.String(), t.Value.String())
	}
	return ident.MustNewTagIterMatcher(vals...)
}
