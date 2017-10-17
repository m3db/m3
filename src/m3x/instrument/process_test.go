// Copyright (c) 2017 Uber Technologies, Inc.
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

package instrument

import (
	"fmt"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/fortytw2/leaktest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestProcessReporter(t *testing.T) {
	defer leaktest.Check(t)()

	scope := tally.NewTestScope("", nil)

	countOpen := 32
	closeFds := func() {}
	for i := 0; i < countOpen; i++ {
		tmpFile, err := ioutil.TempFile("", "example")
		if err != nil {
			require.FailNow(t, fmt.Sprintf("could not open temp file: %v", err))
		}

		// Explicitly close with closeFds call so we can defer the leaktest
		// and it always executes last
		currCloseFds := closeFds
		closeFds = func() {
			currCloseFds()
			assert.NoError(t, tmpFile.Close())
			assert.NoError(t, os.Remove(tmpFile.Name()))
		}
	}

	every := 10 * time.Millisecond

	r := NewProcessReporter(scope, every)
	require.NoError(t, r.Start())

	time.Sleep(2 * every)

	_, ok := scope.Snapshot().Gauges()["process.num-fds+"]
	if !ok {
		require.FailNow(t, "metric for fds not found after waiting 2x interval")
	}

	require.NoError(t, r.Stop())

	closeFds()
}

func TestProcessReporterDoesNotOpenMoreThanOnce(t *testing.T) {
	r := NewProcessReporter(tally.NoopScope, 10*time.Millisecond)
	assert.NoError(t, r.Start())
	assert.Error(t, r.Start())
	assert.NoError(t, r.Stop())
}

func TestProcessReporterDoesNotCloseMoreThanOnce(t *testing.T) {
	r := NewProcessReporter(tally.NoopScope, 10*time.Millisecond)
	assert.NoError(t, r.Start())
	assert.NoError(t, r.Stop())
	assert.Error(t, r.Stop())
}

func TestProcessReporterDoesNotOpenWithInvalidReportInterval(t *testing.T) {
	r := NewProcessReporter(tally.NoopScope, 0)
	assert.Error(t, r.Start())
}
