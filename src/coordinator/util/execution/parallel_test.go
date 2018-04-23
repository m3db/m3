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

package execution

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type request struct {
	order     int
	processed bool
	err       error
}

func (f *request) Process(ctx context.Context) error {
	if f.err != nil {
		return f.err
	}

	if f.order == 0 {
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
		f.processed = true
		return nil
	}
}

func (f *request) String() string {
	return fmt.Sprintf("%v %v %v", f.order, f.processed, f.err)
}

func TestOrderedParallel(t *testing.T) {
	requests := make([]Request, 3)
	requests[0] = &request{order: 0}
	requests[1] = &request{order: 1}
	requests[2] = &request{order: 2}

	err := ExecuteParallel(context.Background(), requests)
	require.NoError(t, err, "no error during parallel execute")
	assert.True(t, requests[0].(*request).processed, "slowest request processed")
}

func TestSingleError(t *testing.T) {
	requests := make([]Request, 3)
	requests[0] = &request{order: 0}
	requests[1] = &request{order: 1, err: fmt.Errorf("problem executing")}
	requests[2] = &request{order: 2}

	err := ExecuteParallel(context.Background(), requests)
	assert.Error(t, err, "error in second request")
	assert.False(t, requests[0].(*request).processed, "skip request on error")
}
