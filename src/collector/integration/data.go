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

package integration

import (
	"bytes"

	"github.com/m3db/m3/src/metrics/metadata"
	"github.com/m3db/m3/src/metrics/metric/id"
	"github.com/m3db/m3/src/metrics/metric/unaggregated"
)

type idGenerator func(int) id.ID

// nolint:structcheck
type outputResult struct {
	idGen     idGenerator
	metadatas metadata.StagedMetadatas
}

type outputResults []outputResult

// nolint:structcheck
type metricWithMetadatas struct {
	metric    unaggregated.MetricUnion
	metadatas metadata.StagedMetadatas
}

type resultsByTypeAscIDAsc []metricWithMetadatas

func (r resultsByTypeAscIDAsc) Len() int      { return len(r) }
func (r resultsByTypeAscIDAsc) Swap(i, j int) { r[i], r[j] = r[j], r[i] }

func (r resultsByTypeAscIDAsc) Less(i, j int) bool {
	if r[i].metric.Type < r[j].metric.Type {
		return true
	}
	if r[i].metric.Type > r[j].metric.Type {
		return false
	}
	return bytes.Compare(r[i].metric.ID, r[j].metric.ID) < 0
}
