// Copyright (c) 2019 Uber Technologies, Inc.
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

type emptyBlock struct {
	meta Metadata
}

// NewEmptyBlock creates an empty block with the given metadata.
func NewEmptyBlock(meta Metadata) Block {
	return &emptyBlock{meta: meta}
}

func (b *emptyBlock) Close() error { return nil }

func (b *emptyBlock) WithMetadata(meta Metadata, _ []SeriesMeta) (Block, error) {
	return NewEmptyBlock(meta), nil
}

func (b *emptyBlock) StepIter() (StepIter, error) {
	return &emptyStepIter{meta: b.meta}, nil
}

type emptyStepIter struct {
	meta Metadata
}

func (it *emptyStepIter) Close()                   {}
func (it *emptyStepIter) Err() error               { return nil }
func (it *emptyStepIter) StepCount() int           { return it.meta.Bounds.Steps() }
func (it *emptyStepIter) SeriesMeta() []SeriesMeta { return []SeriesMeta{} }
func (it *emptyStepIter) Next() bool               { return false }
func (it *emptyStepIter) Meta() Metadata           { return it.meta }
func (it *emptyStepIter) Current() Step            { return nil }

func (b *emptyBlock) SeriesIter() (SeriesIter, error) {
	return &emptySeriesIter{
		meta: b.meta,
	}, nil
}

type emptySeriesIter struct {
	meta Metadata
}

func (it *emptySeriesIter) Close()                   {}
func (it *emptySeriesIter) Err() error               { return nil }
func (it *emptySeriesIter) SeriesCount() int         { return 0 }
func (it *emptySeriesIter) SeriesMeta() []SeriesMeta { return []SeriesMeta{} }
func (it *emptySeriesIter) Next() bool               { return false }
func (it *emptySeriesIter) Current() Series          { return Series{} }
func (it *emptySeriesIter) Meta() Metadata           { return it.meta }

// Unconsolidated returns the unconsolidated version for the block
func (b *emptyBlock) Unconsolidated() (UnconsolidatedBlock, error) {
	return &ucEmptyBlock{
		meta: b.meta,
	}, nil
}

type ucEmptyBlock struct {
	meta Metadata
}

func (b *ucEmptyBlock) Close() error { return nil }

func (b *ucEmptyBlock) WithMetadata(
	meta Metadata, _ []SeriesMeta) (UnconsolidatedBlock, error) {
	return &ucEmptyBlock{
		meta: meta,
	}, nil
}

func (b *ucEmptyBlock) Consolidate() (Block, error) {
	return NewEmptyBlock(b.meta), nil
}

func (b *ucEmptyBlock) StepIter() (UnconsolidatedStepIter, error) {
	return &ucEmptyStepIter{
		meta: b.meta,
	}, nil
}

type ucEmptyStepIter struct {
	meta Metadata
}

func (it *ucEmptyStepIter) Close()                      {}
func (it *ucEmptyStepIter) Err() error                  { return nil }
func (it *ucEmptyStepIter) StepCount() int              { return it.meta.Bounds.Steps() }
func (it *ucEmptyStepIter) SeriesMeta() []SeriesMeta    { return []SeriesMeta{} }
func (it *ucEmptyStepIter) Next() bool                  { return false }
func (it *ucEmptyStepIter) Meta() Metadata              { return it.meta }
func (it *ucEmptyStepIter) Current() UnconsolidatedStep { return nil }

func (b *ucEmptyBlock) SeriesIter() (UnconsolidatedSeriesIter, error) {
	return &ucEmptySeriesIter{
		meta: b.meta,
	}, nil
}

type ucEmptySeriesIter struct {
	meta Metadata
}

func (it *ucEmptySeriesIter) Close()                        {}
func (it *ucEmptySeriesIter) Err() error                    { return nil }
func (it *ucEmptySeriesIter) SeriesCount() int              { return 0 }
func (it *ucEmptySeriesIter) SeriesMeta() []SeriesMeta      { return []SeriesMeta{} }
func (it *ucEmptySeriesIter) Next() bool                    { return false }
func (it *ucEmptySeriesIter) Current() UnconsolidatedSeries { return UnconsolidatedSeries{} }
func (it *ucEmptySeriesIter) Meta() Metadata                { return it.meta }
