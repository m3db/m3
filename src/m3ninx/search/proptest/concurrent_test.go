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
	"sync"
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

func TestConcurrentQueries(t *testing.T) {
	parameters := gopter.DefaultTestParameters()
	seed := time.Now().UnixNano()
	parameters.MinSuccessfulTests = 100
	parameters.MaxSize = 20
	parameters.Rng = rand.New(rand.NewSource(seed))
	properties := gopter.NewProperties(parameters)

	simpleSeg := newTestMemSegment(t, lotsTestDocuments)
	simpleReader, err := simpleSeg.Reader()
	require.NoError(t, err)
	simpleExec := executor.NewExecutor([]index.Reader{simpleReader})

	fstSeg := fst.ToTestSegment(t, simpleSeg, fstOptions)
	fstReader, err := fstSeg.Reader()
	require.NoError(t, err)
	fstExec := executor.NewExecutor([]index.Reader{fstReader})

	properties.Property("Any concurrent queries segments does not affect fst segments", prop.ForAll(
		func(q search.Query) (bool, error) {
			dOrg, err := simpleExec.Execute(q)
			require.NoError(t, err)
			matchedDocs, err := collectDocs(dOrg)
			require.NoError(t, err)
			docMatcher, err := newDocumentIteratorMatcher(t, matchedDocs...)
			require.NoError(t, err)

			var (
				numConcurrentWorkers = 2
				wg                   sync.WaitGroup
				errLock              sync.Mutex
				matchErr             error
			)
			wg.Add(numConcurrentWorkers)

			for i := 0; i < numConcurrentWorkers; i++ {
				go func() {
					defer wg.Done()
					fstDocs, err := fstExec.Execute(q)
					require.NoError(t, err)
					if err := docMatcher.Matches(fstDocs); err != nil {
						errLock.Lock()
						matchErr = err
						errLock.Unlock()
					}
				}()
			}
			wg.Wait()
			errLock.Lock()
			defer errLock.Unlock()
			if matchErr != nil {
				return false, matchErr
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
