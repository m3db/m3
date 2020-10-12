// Copyright (c) 2020 Uber Technologies, Inc.
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

package block

// NewUnconsolidatedSeriesIter returns a new unconsolidated series iterator.
func NewUnconsolidatedSeriesIter(
	series []UnconsolidatedSeries,
) SeriesIter {
	metas := make([]SeriesMeta, 0, len(series))
	for _, elem := range series {
		metas = append(metas, elem.Meta)
	}
	return &unconsolidatedSeriesIter{
		idx:    -1,
		series: series,
		metas:  metas,
	}
}

type unconsolidatedSeriesIter struct {
	idx    int
	series []UnconsolidatedSeries
	metas  []SeriesMeta
}

func (i *unconsolidatedSeriesIter) Next() bool {
	i.idx++
	if i.idx >= len(i.series) {
		return false
	}
	return true
}

func (i *unconsolidatedSeriesIter) Err() error {
	return nil
}

func (i *unconsolidatedSeriesIter) Close() {
}

func (i *unconsolidatedSeriesIter) SeriesMeta() []SeriesMeta {
	return i.metas
}

func (i *unconsolidatedSeriesIter) SeriesCount() int {
	return len(i.series)
}

func (i *unconsolidatedSeriesIter) Current() UnconsolidatedSeries {
	return i.series[i.idx]
}
