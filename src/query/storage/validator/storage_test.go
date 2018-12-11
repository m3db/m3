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

package validator

import (
	"testing"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus"
	"github.com/m3db/m3/src/query/models"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConverter(t *testing.T) {
	promResult := prometheus.PromResp{
		Status: "success",
	}

	vals := [][]interface{}{
		{1543434975.200, "10"},
		{1543434985.200, "12"},
		{1543434995.200, "14"},
	}

	metrics := map[string]string{
		"__name__": "test_name",
		"tag_one":  "val_one",
	}

	promResult.Data.ResultType = "matrix"
	promResult.Data.Result = append(promResult.Data.Result,
		struct {
			Metric map[string]string `json:"metric"`
			Values [][]interface{}   `json:"values"`
		}{
			Values: vals,
			Metric: metrics,
		},
	)

	tsList, err := PromResultToSeriesList(promResult, models.NewTagOptions())
	require.NoError(t, err)

	assert.Equal(t, 3, tsList[0].Len())
	assert.Equal(t, 10.0, tsList[0].Values().Datapoints()[0].Value)
	assert.Equal(t, "test_name", tsList[0].Name())
}
