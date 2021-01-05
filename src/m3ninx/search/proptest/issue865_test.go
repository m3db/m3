// +build big

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

package proptest

import (
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3/src/m3ninx/search/query"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

// NB(prateek): this test simulates the issues described in issue: https://github.com/m3db/m3/issues/865

var (
	doc1 = doc.Metadata{
		ID: []byte("__name__=node_cpu_seconds_total,cpu=1,instance=m3db-node01:9100,job=node-exporter,mode=system,"),
		Fields: []doc.Field{
			{[]byte("cpu"), []byte("1")},
			{[]byte("__name__"), []byte("node_cpu_seconds_total")},
			{[]byte("instance"), []byte("m3db-node01:9100")},
			{[]byte("job"), []byte("node-exporter")},
			{[]byte("mode"), []byte("system")},
		},
	}
	doc2 = doc.Metadata{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=m3db-node01:9100,job=node-exporter,"),
		Fields: []doc.Field{
			{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			{[]byte("instance"), []byte("m3db-node01:9100")},
			{[]byte("job"), []byte("node-exporter")},
		},
	}
	doc3 = doc.Metadata{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=alertmanager03:9100,job=node-exporter,"),
		Fields: []doc.Field{
			{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			{[]byte("instance"), []byte("alertmanager03:9100")},
			{[]byte("job"), []byte("node-exporter")},
		},
	}
	doc4 = doc.Metadata{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=prometheus01:9100,job=node-exporter,"),
		Fields: []doc.Field{
			{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			{[]byte("instance"), []byte("prometheus01:9100")},
			{[]byte("job"), []byte("node-exporter")},
		},
	}
	simpleTestDocs = []doc.Metadata{doc1, doc2, doc3, doc4}
)

func TestAnyDistributionOfDocsDoesNotAffectQuery(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	docMatcher, err := newDocumentIteratorMatcher(t, doc.NewDocumentFromMetadata(doc2))
	require.NoError(t, err)
	properties.Property("Any distribution of simple documents does not affect query results", prop.ForAll(
		func(i propTestInput) (bool, error) {
			segments := i.generate(t, simpleTestDocs)
			readers := make([]index.Reader, 0, len(segments))
			for _, s := range segments {
				r, err := s.Reader()
				if err != nil {
					return false, err
				}
				readers = append(readers, r)
			}

			q := query.NewConjunctionQuery([]search.Query{
				query.NewTermQuery([]byte("__name__"), []byte("node_memory_SwapTotal_bytes")),
				query.NewTermQuery([]byte("instance"), []byte("m3db-node01:9100")),
			})

			e := executor.NewExecutor(readers)
			d, err := e.Execute(q)
			if err != nil {
				return false, err
			}

			if err := docMatcher.Matches(d); err != nil {
				return false, err
			}

			return true, nil
		},
		genPropTestInput(len(simpleTestDocs)),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
