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

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildMeta(start time.Time) Metadata {
	return buildMetaExhaustive(start, false)
}

func buildMetaExhaustive(start time.Time, exhaustive bool) Metadata {
	return Metadata{
		Bounds: models.Bounds{
			Start:    start,
			Duration: time.Minute,
			StepSize: time.Hour,
		},
		Tags:           models.NewTags(0, models.NewTagOptions()),
		ResultMetadata: ResultMetadata{Exhaustive: exhaustive},
	}
}

func buildTestSeriesMeta(name string) []SeriesMeta {
	tags := models.NewTags(1, models.NewTagOptions()).
		AddTag(models.Tag{Name: []byte("a"), Value: []byte("b")})
	return []SeriesMeta{
		SeriesMeta{
			Name: []byte(name),
			Tags: tags,
		},
	}

}

func testLazyOpts(timeOffset time.Duration, valOffset float64) LazyOptions {
	tt := func(t time.Time) time.Time { return t.Add(timeOffset) }
	vt := func(val float64) float64 { return val * valOffset }
	mt := func(meta Metadata) Metadata {
		meta.Bounds.Start = meta.Bounds.Start.Add(timeOffset)
		return meta
	}

	smt := func(sm []SeriesMeta) []SeriesMeta {
		for i, m := range sm {
			sm[i].Name = append(m.Name, []byte("_mutated")...)
		}

		return sm
	}

	return NewLazyOptions().
		SetTimeTransform(tt).
		SetValueTransform(vt).
		SetMetaTransform(mt).
		SetSeriesMetaTransform(smt)
}

func TestLazyOpts(t *testing.T) {
	off := time.Minute
	lazyOpts := testLazyOpts(off, 1.0)

	now := xtime.ToUnixNano(time.Now())
	equalTimes := lazyOpts.TimeTransform()(now).Equal(now.Add(off))
	assert.True(t, equalTimes)

	meta := buildMeta(now)
	updated := lazyOpts.MetaTransform()(meta)
	expected := buildMeta(now.Add(off))
	require.Equal(t, expected, updated)

	seriesMeta := buildTestSeriesMeta("name")
	expectSM := buildTestSeriesMeta("name_mutated")
	require.Equal(t, expectSM, lazyOpts.SeriesMetaTransform()(seriesMeta))

	require.Equal(t, 1.0, lazyOpts.ValueTransform()(1.0))
}

func TestValidOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Minute
	off := NewLazyBlock(b, testLazyOpts(offset, 1.0))

	b.EXPECT().Info().Return(NewBlockInfo(BlockM3TSZCompressed))
	info := off.Info()
	assert.Equal(t, BlockLazy, info.Type())
	assert.Equal(t, BlockM3TSZCompressed, info.BaseType())

	// ensure functions are marshalled to the underlying block.
	b.EXPECT().Close().Return(nil)
	err := off.Close()
	assert.NoError(t, err)

	msg := "err"
	e := errors.New(msg)
	b.EXPECT().Close().Return(e)
	err = off.Close()
	assert.EqualError(t, err, msg)

	b.EXPECT().StepIter().Return(nil, e)
	_, err = off.StepIter()
	assert.EqualError(t, err, msg)

	b.EXPECT().Close().Return(nil)
	err = off.Close()
	assert.NoError(t, err)
}

func TestStepIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Minute
	off := NewLazyBlock(b, testLazyOpts(offset, 1.0))
	msg := "err"
	e := errors.New(msg)
	now := time.Now()

	b.EXPECT().Meta().Return(buildMeta(now))
	ex := buildMeta(now.Add(offset))
	require.Equal(t, ex, off.Meta())

	iter := NewMockStepIter(ctrl)
	b.EXPECT().StepIter().Return(iter, nil)
	it, err := off.StepIter()
	require.NoError(t, err)

	seriesMetas := buildTestSeriesMeta("name")
	expected := buildTestSeriesMeta("name_mutated")

	iter.EXPECT().SeriesMeta().Return(seriesMetas)
	assert.Equal(t, expected, it.SeriesMeta())

	// ensure functions are marshalled to the block's underlying step iterator.
	iter.EXPECT().Close()
	it.Close()

	iter.EXPECT().Err().Return(e)
	assert.EqualError(t, it.Err(), msg)

	iter.EXPECT().StepCount().Return(12)
	assert.Equal(t, 12, it.StepCount())

	iter.EXPECT().Next().Return(true)
	assert.True(t, it.Next())

	vals := []float64{1, 2, 3}
	step := NewMockStep(ctrl)
	step.EXPECT().Values().Return(vals)
	step.EXPECT().Time().Return(now)

	iter.EXPECT().Current().Return(step)
	actual := it.Current()
	assert.Equal(t, vals, actual.Values())
	assert.Equal(t, now.Add(offset), actual.Time())
}

func TestSeriesIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	b := NewMockBlock(ctrl)
	defer ctrl.Finish()
	offset := time.Minute
	offblock := NewLazyBlock(b, testLazyOpts(offset, 1.0))
	now := time.Now()
	msg := "err"
	e := errors.New(msg)

	// ensure functions are marshalled to the underlying unconsolidated block.
	iter := NewMockSeriesIter(ctrl)
	b.EXPECT().SeriesIter().Return(iter, nil)
	it, err := offblock.SeriesIter()
	require.NoError(t, err)

	seriesMetas := buildTestSeriesMeta("name")
	expected := buildTestSeriesMeta("name_mutated")

	iter.EXPECT().SeriesMeta().Return(seriesMetas)
	assert.Equal(t, expected, it.SeriesMeta())

	// ensure functions are marshalled to the block's underlying series iterator.
	iter.EXPECT().Close()
	it.Close()

	iter.EXPECT().Err().Return(e)
	assert.EqualError(t, it.Err(), msg)

	iter.EXPECT().SeriesCount().Return(12)
	assert.Equal(t, 12, it.SeriesCount())

	iter.EXPECT().Next().Return(true)
	assert.True(t, it.Next())

	vals := ts.Datapoints{
		ts.Datapoint{
			Timestamp: now,
			Value:     12,
		},
	}

	unconsolidated := UnconsolidatedSeries{
		datapoints: vals,
	}

	iter.EXPECT().Current().Return(unconsolidated)
	actual := it.Current()
	xts := ts.Datapoints{
		ts.Datapoint{
			Timestamp: now.Add(offset),
			Value:     12,
		},
	}

	assert.Equal(t, xts, actual.Datapoints())
}

// negative value offset tests

func TestStepIterWithNegativeValueOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Duration(0)
	off := NewLazyBlock(b, testLazyOpts(offset, -1.0))
	msg := "err"
	e := errors.New(msg)
	now := time.Now()

	iter := NewMockStepIter(ctrl)
	b.EXPECT().StepIter().Return(iter, nil)
	it, err := off.StepIter()
	require.NoError(t, err)

	// ensure functions are marshalled to the block's underlying step iterator.
	iter.EXPECT().Close()
	it.Close()

	iter.EXPECT().Err().Return(e)
	assert.EqualError(t, it.Err(), msg)

	iter.EXPECT().StepCount().Return(12)
	assert.Equal(t, 12, it.StepCount())

	seriesMetas := []SeriesMeta{}
	iter.EXPECT().SeriesMeta().Return(seriesMetas)
	assert.Equal(t, seriesMetas, it.SeriesMeta())

	iter.EXPECT().Next().Return(true)
	assert.True(t, it.Next())

	vals := []float64{1, 2, 3}
	step := NewMockStep(ctrl)
	step.EXPECT().Values().Return(vals)
	step.EXPECT().Time().Return(now)

	expectedVals := []float64{-1, -2, -3}
	iter.EXPECT().Current().Return(step)
	actual := it.Current()
	assert.Equal(t, expectedVals, actual.Values())
	assert.Equal(t, now, actual.Time())
}

func TestUnconsolidatedSeriesIterWithNegativeValueOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	b := NewMockBlock(ctrl)
	defer ctrl.Finish()
	offset := time.Duration(0)
	offblock := NewLazyBlock(b, testLazyOpts(offset, -1.0))
	now := time.Now()
	msg := "err"
	e := errors.New(msg)

	iter := NewMockSeriesIter(ctrl)
	b.EXPECT().SeriesIter().Return(iter, nil)
	it, err := offblock.SeriesIter()
	require.NoError(t, err)

	concurrency := 5
	batched := []SeriesIterBatch{
		SeriesIterBatch{},
		SeriesIterBatch{},
		SeriesIterBatch{},
	}

	b.EXPECT().MultiSeriesIter(concurrency).Return(batched, nil)
	bs, err := offblock.MultiSeriesIter(concurrency)
	require.NoError(t, err)
	assert.Equal(t, batched, bs)

	// ensure functions are marshalled to the block's underlying series iterator.
	iter.EXPECT().Close()
	it.Close()

	iter.EXPECT().Err().Return(e)
	assert.EqualError(t, it.Err(), msg)

	iter.EXPECT().SeriesCount().Return(12)
	assert.Equal(t, 12, it.SeriesCount())

	seriesMetas := []SeriesMeta{}
	iter.EXPECT().SeriesMeta().Return(seriesMetas)
	assert.Equal(t, seriesMetas, it.SeriesMeta())

	iter.EXPECT().Next().Return(true)
	assert.True(t, it.Next())

	vals := ts.Datapoints{
		ts.Datapoint{
			Timestamp: now,
			Value:     12,
		},
	}

	unconsolidated := UnconsolidatedSeries{
		datapoints: vals,
	}

	iter.EXPECT().Current().Return(unconsolidated)
	actual := it.Current()
	expected := ts.Datapoints{
		ts.Datapoint{
			Timestamp: now,
			Value:     -12,
		},
	}

	assert.Equal(t, expected, actual.Datapoints())
}
