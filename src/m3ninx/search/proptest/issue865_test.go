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
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"
	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"
	"github.com/m3db/m3/src/m3ninx/search/query"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

// NB(prateek): this test simulates the issues described in issue: https://github.com/m3db/m3/issues/865
// tl;dr - the searcher code assumes the input readers had disjoint doc ID ranges; it caused issues when that
// was not true.

var (
	memOptions = mem.NewOptions()
	fstOptions = fst.NewOptions()

	doc1 = doc.Document{
		ID: []byte("__name__=node_cpu_seconds_total,cpu=1,instance=m3db-node01:9100,job=node-exporter,mode=system,"),
		Fields: []doc.Field{
			doc.Field{[]byte("cpu"), []byte("1")},
			doc.Field{[]byte("__name__"), []byte("node_cpu_seconds_total")},
			doc.Field{[]byte("instance"), []byte("m3db-node01:9100")},
			doc.Field{[]byte("job"), []byte("node-exporter")},
			doc.Field{[]byte("mode"), []byte("system")},
		},
	}
	doc2 = doc.Document{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=m3db-node01:9100,job=node-exporter,"),
		Fields: []doc.Field{
			doc.Field{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			doc.Field{[]byte("instance"), []byte("m3db-node01:9100")},
			doc.Field{[]byte("job"), []byte("node-exporter")},
		},
	}
	doc3 = doc.Document{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=alertmanager03:9100,job=node-exporter,"),
		Fields: []doc.Field{
			doc.Field{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			doc.Field{[]byte("instance"), []byte("alertmanager03:9100")},
			doc.Field{[]byte("job"), []byte("node-exporter")},
		},
	}
	doc4 = doc.Document{
		ID: []byte("__name__=node_memory_SwapTotal_bytes,instance=prometheus01:9100,job=node-exporter,"),
		Fields: []doc.Field{
			doc.Field{[]byte("__name__"), []byte("node_memory_SwapTotal_bytes")},
			doc.Field{[]byte("instance"), []byte("prometheus01:9100")},
			doc.Field{[]byte("job"), []byte("node-exporter")},
		},
	}
	simpleTestDocs = []doc.Document{doc1, doc2, doc3, doc4}
)

func TestAnyDistributionOfDocsDoesNotAffectQuery(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

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

			if !d.Next() {
				return false, fmt.Errorf("unable to find any documents")
			}

			curr := d.Current()
			if !curr.Equal(doc2) {
				return false, fmt.Errorf("returned document [%+v] did not match exepcted document [%+v]",
					curr, doc2)
			}

			if d.Next() {
				return false, fmt.Errorf("found too many documents")
			}

			if err := d.Err(); err != nil {
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

func (i propTestInput) generate(t *testing.T, docs []doc.Document) []segment.Segment {
	var result []segment.Segment
	for j := 0; j < len(i.segments); j++ {
		initialOffset := postings.ID(i.segments[j].initialDocIDOffset)
		s, err := mem.NewSegment(initialOffset, memOptions)
		require.NoError(t, err)
		for k := 0; k < len(i.docIds[j]); k++ {
			idx := i.docIds[j][k]
			_, err = s.Insert(docs[idx])
			require.NoError(t, err)
		}

		if i.segments[j].simpleSegment {
			result = append(result, s)
			continue
		}

		result = append(result, fst.ToTestSegment(t, s, fstOptions))
	}
	return result
}

type propTestInput struct {
	segments []generatedSegment
	docIds   [][]int
}

func genPropTestInput(numDocs int) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		numSegmentsRes, ok := gen.IntRange(1, numDocs)(genParams).Retrieve()
		if !ok {
			panic("unable to generate segments")
		}
		numSegments := numSegmentsRes.(int)

		docIds := make([]int, 0, numDocs)
		for i := 0; i < numDocs; i++ {
			docIds = append(docIds, i)
		}

		randomIds := randomDocIds(docIds)
		randomIds.shuffle(genParams.Rng)

		genSegments := make([]generatedSegment, 0, numSegments)
		partitionedDocs := make([][]int, 0, numSegments)
		for i := 0; i < numSegments; i++ {
			partitionedDocs = append(partitionedDocs, []int{})
			segRes, ok := genSegment()(genParams).Retrieve()
			if !ok {
				panic("unable to generate segments")
			}
			genSegments = append(genSegments, segRes.(generatedSegment))
		}

		for i := 0; i < numDocs; i++ {
			idx := i % numSegments
			partitionedDocs[idx] = append(partitionedDocs[idx], randomIds[i])
		}

		result := propTestInput{
			segments: genSegments,
			docIds:   partitionedDocs,
		}
		if len(genSegments) != len(partitionedDocs) {
			panic(fmt.Errorf("unequal lengths of segments and docs: %+v", result))
		}

		return gopter.NewGenResult(result, gopter.NoShrinker)
	}
}

func genSegment() gopter.Gen {
	return gopter.CombineGens(
		gen.Bool(),         // simple segment
		gen.IntRange(1, 5), // initial doc id offset
	).Map(func(val interface{}) generatedSegment {
		inputs := val.([]interface{})
		return generatedSegment{
			simpleSegment:      inputs[0].(bool),
			initialDocIDOffset: inputs[1].(int),
		}
	})
}

type generatedSegment struct {
	simpleSegment      bool
	initialDocIDOffset int
}

type randomDocIds []int

func (d randomDocIds) shuffle(rng *rand.Rand) {
	// Start from the last element and swap one by one.
	// NB: We don't need to run for the first element that's why i > 0
	for i := len(d) - 1; i > 0; i-- {
		j := rng.Intn(i)
		d[i], d[j] = d[j], d[i]
	}
}
