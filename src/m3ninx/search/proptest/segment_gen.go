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
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/index/segment/mem"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/gen"
	"github.com/stretchr/testify/require"
)

var (
	memOptions = mem.NewOptions()
	fstOptions = fst.NewOptions()
)

func collectDocs(iter doc.Iterator) ([]doc.Document, error) {
	var docs []doc.Document
	for iter.Next() {
		docs = append(docs, iter.Current())
	}

	if err := iter.Err(); err != nil {
		return nil, err
	}

	return docs, nil
}

func newTestMemSegment(t *testing.T, docs []doc.Metadata) segment.MutableSegment {
	opts := mem.NewOptions()
	s, err := mem.NewSegment(opts)
	require.NoError(t, err)
	for _, d := range docs {
		_, err := s.Insert(d)
		require.NoError(t, err)
	}
	return s
}

func (i propTestInput) generate(t *testing.T, docs []doc.Metadata) []segment.Segment {
	var result []segment.Segment
	for j := 0; j < len(i.segments); j++ {
		s, err := mem.NewSegment(memOptions)
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
	numDocs  int
	segments []generatedSegment
	docIds   [][]int
}

func genPropTestInput(numDocs int) gopter.Gen {
	return func(genParams *gopter.GenParameters) *gopter.GenResult {
		maxNumSegments := numDocs
		if maxNumSegments > 10 {
			maxNumSegments = 10
		}

		numSegmentsRes, ok := gen.IntRange(1, maxNumSegments)(genParams).Retrieve()
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
			numDocs:  numDocs,
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
		gen.Bool(), // simple segment
	).Map(func(val interface{}) generatedSegment {
		var inputs []interface{}
		if x, ok := val.(*gopter.GenResult); ok {
			res, rOk := x.Retrieve()
			if !rOk {
				panic("should never happen")
			}
			inputs = res.([]interface{})
		} else {
			inputs = val.([]interface{})
		}
		return generatedSegment{
			simpleSegment: inputs[0].(bool),
		}
	})
}

type generatedSegment struct {
	simpleSegment bool
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
