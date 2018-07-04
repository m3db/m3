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

package native

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/m3db/m3db/src/coordinator/test"
	"github.com/m3db/m3db/src/coordinator/storage/mock"
	"github.com/m3db/m3db/src/coordinator/block"
)

func TestPromRead(t *testing.T) {
	logging.InitWithCores(nil)
	values, bounds := test.GenerateValuesAndBounds(nil, nil)
	b := test.NewBlockFromValues(bounds, values)
	mockStorage := mock.NewMockStorageWithBlocks([]block.Block{b})

	promRead := &PromReadHandler{engine: executor.NewEngine(mockStorage)}
	req, _ := http.NewRequest("GET", PromReadURL, nil)
	req.URL.RawQuery = defaultParams().Encode()

	r, parseErr := parseParams(req)
	require.Nil(t, parseErr)
	seriesList, err := promRead.read(context.TODO(), httptest.NewRecorder(), r)
	require.NoError(t, err)
	require.Len(t, seriesList, 2)
	s := seriesList[0]
	assert.Equal(t, 5, s.Values().Len())
	for i := 0; i < s.Values().Len(); i++ {
		assert.Equal(t, float64(i), s.Values().ValueAt(i))
	}
}
