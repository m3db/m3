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

	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/index/segment/fst"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/m3ninx/search/executor"

	"github.com/leanovate/gopter"
	"github.com/leanovate/gopter/prop"
	"github.com/stretchr/testify/require"
)

func TestSegmentDistributionDoesNotAffectQuery(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	simpleSeg := newTestMemSegment(t, lotsTestDocuments)
	properties.Property("Any distribution of test documents in segments does not affect query results", prop.ForAll(
		func(i propTestInput, q search.Query) (bool, error) {
			r, err := simpleSeg.Reader()
			require.NoError(t, err)
			eOrg := executor.NewExecutor([]index.Reader{r})
			dOrg, err := eOrg.Execute(q)
			if err != nil {
				return false, err
			}
			matchedDocs, err := collectDocs(dOrg)
			require.NoError(t, err)
			docMatcher, err := newDocumentIteratorMatcher(t, matchedDocs...)
			require.NoError(t, err)

			segments := i.generate(t, lotsTestDocuments)
			readers := make([]index.Reader, 0, len(segments))
			for _, s := range segments {
				r, err := s.Reader()
				if err != nil {
					return false, err
				}
				readers = append(readers, r)
			}

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
		genPropTestInput(len(lotsTestDocuments)),
		GenQuery(lotsTestDocuments),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}

func TestFSTSimpleSegmentsQueryTheSame(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	simpleSeg := newTestMemSegment(t, lotsTestDocuments)
	fstSeg := fst.ToTestSegment(t, simpleSeg, fstOptions)

	properties.Property("Simple & FST Segments Query the same results", prop.ForAll(
		func(q search.Query) (bool, error) {
			r, err := simpleSeg.Reader()
			require.NoError(t, err)
			eOrg := executor.NewExecutor([]index.Reader{r})
			dOrg, err := eOrg.Execute(q)
			if err != nil {
				return false, err
			}
			matchedDocs, err := collectDocs(dOrg)
			require.NoError(t, err)
			docMatcher, err := newDocumentIteratorMatcher(t, matchedDocs...)
			require.NoError(t, err)

			rFst, err := fstSeg.Reader()
			require.NoError(t, err)
			e := executor.NewExecutor([]index.Reader{rFst})
			d, err := e.Execute(q)
			if err != nil {
				return false, err
			}

			if err := docMatcher.Matches(d); err != nil {
				return false, err
			}

			return true, nil
		},
		GenQuery(lotsTestDocuments),
	))

	reporter := gopter.NewFormatedReporter(true, 160, os.Stdout)
	if !properties.Run(reporter) {
		t.Errorf("failed with initial seed: %d", seed)
	}
}
