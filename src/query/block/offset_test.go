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

package block

import (
	"errors"
	"testing"
	"time"

	"github.com/golang/mock/gomock"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func buildMeta(start time.Time) Metadata {
	return Metadata{
		Bounds: models.Bounds{
			Start:    start,
			Duration: time.Minute,
			StepSize: time.Hour,
		},
		Tags: models.NewTags(0, models.NewTagOptions()),
	}
}

func TestUpdateMeta(t *testing.T) {
	now := time.Now()
	meta := buildMeta(now)
	updated := updateMeta(meta, time.Hour)
	expected := buildMeta(now.Add(time.Hour))
	require.Equal(t, expected, updated)
}

func TestInvalidOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	off := NewOffsetBlock(b, 0)
	assert.Equal(t, b, off)
	off = NewOffsetBlock(b, -1)
	assert.Equal(t, b, off)
}

func TestValidOffset(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Minute
	off := NewOffsetBlock(b, offset)

	// ensure functions are marshalled to the underlying block.
	b.EXPECT().Close().Return(nil)
	err := off.Close()
	assert.NoError(t, err)

	msg := "err"
	e := errors.New(msg)
	b.EXPECT().Close().Return(e)
	err = off.Close()
	assert.EqualError(t, err, msg)

	b.EXPECT().SeriesIter().Return(nil, e)
	_, err = off.SeriesIter()
	assert.EqualError(t, err, msg)

	b.EXPECT().StepIter().Return(nil, e)
	_, err = off.StepIter()
	assert.EqualError(t, err, msg)

	b.EXPECT().Unconsolidated().Return(nil, e)
	_, err = off.Unconsolidated()
	assert.EqualError(t, err, msg)

	now := time.Now()
	meta := buildMeta(now)
	seriesMetas := []SeriesMeta{}

	// ensure WithMetadata marshalls to underlying block.
	b.EXPECT().WithMetadata(meta, seriesMetas).Return(nil, e)
	_, err = off.WithMetadata(meta, seriesMetas)
	assert.EqualError(t, err, msg)

	b2 := NewMockBlock(ctrl)
	b.EXPECT().WithMetadata(meta, seriesMetas).Return(b2, nil)
	bl, err := off.WithMetadata(meta, seriesMetas)
	assert.Equal(t, off, bl)
	assert.NoError(t, err)

	// ensure WithMetadata has updated the underlying block.
	b2.EXPECT().Close().Return(nil)
	err = off.Close()
	assert.NoError(t, err)
}

func TestStepIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Minute
	off := NewOffsetBlock(b, offset)
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

	iter.EXPECT().Meta().Return(buildMeta(now))
	ex := buildMeta(now.Add(offset))
	require.Equal(t, ex, it.Meta())

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
	defer ctrl.Finish()
	b := NewMockBlock(ctrl)
	offset := time.Minute
	off := NewOffsetBlock(b, offset)
	msg := "err"
	e := errors.New(msg)
	now := time.Now()

	iter := NewMockSeriesIter(ctrl)
	b.EXPECT().SeriesIter().Return(iter, nil)
	it, err := off.SeriesIter()
	require.NoError(t, err)

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

	iter.EXPECT().Meta().Return(buildMeta(now))
	ex := buildMeta(now.Add(offset))
	require.Equal(t, ex, it.Meta())

	vals := []float64{1, 2, 3}
	series := Series{
		Meta:   SeriesMeta{},
		values: vals,
	}

	iter.EXPECT().Current().Return(series)
	assert.Equal(t, series, it.Current())
}

func TestUnconsolidated(t *testing.T) {
	ctrl := gomock.NewController(t)
	bb := NewMockBlock(ctrl)
	defer ctrl.Finish()
	offset := time.Minute
	offblock := NewOffsetBlock(bb, offset)

	// ensure functions are marshalled to the underlying unconsolidated block.
	b := NewMockUnconsolidatedBlock(ctrl)
	bb.EXPECT().Unconsolidated().Return(b, nil)

	off, err := offblock.Unconsolidated()
	assert.NoError(t, err)

	b.EXPECT().Close().Return(nil)
	err = off.Close()
	assert.NoError(t, err)

	msg := "err"
	e := errors.New(msg)
	b.EXPECT().Close().Return(e)
	err = off.Close()
	assert.EqualError(t, err, msg)

	b.EXPECT().SeriesIter().Return(nil, e)
	_, err = off.SeriesIter()
	assert.EqualError(t, err, msg)

	b.EXPECT().StepIter().Return(nil, e)
	_, err = off.StepIter()
	assert.EqualError(t, err, msg)

	b.EXPECT().Consolidate().Return(nil, e)
	_, err = off.Consolidate()
	assert.EqualError(t, err, msg)

	// ensure consolidated block uses the new block.
	b.EXPECT().Consolidate().Return(bb, nil)
	revert, err := off.Consolidate()
	assert.NoError(t, err)
	bb.EXPECT().Close().Return(nil)
	revert.Close()

	now := time.Now()
	meta := buildMeta(now)
	seriesMetas := []SeriesMeta{}

	// ensure WithMetadata marshalls to underlying block.
	b.EXPECT().WithMetadata(meta, seriesMetas).Return(nil, e)
	_, err = off.WithMetadata(meta, seriesMetas)
	assert.EqualError(t, err, msg)

	b2 := NewMockUnconsolidatedBlock(ctrl)
	b.EXPECT().WithMetadata(meta, seriesMetas).Return(b2, nil)
	bl, err := off.WithMetadata(meta, seriesMetas)
	assert.Equal(t, off, bl)
	assert.NoError(t, err)

	// ensure WithMetadata has updated the underlying block.
	b2.EXPECT().Close().Return(nil)
	err = off.Close()
	assert.NoError(t, err)
}

func TestUnconsolidatedStepIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	bb := NewMockBlock(ctrl)
	defer ctrl.Finish()
	offset := time.Minute
	offblock := NewOffsetBlock(bb, offset)
	now := time.Now()
	msg := "err"
	e := errors.New(msg)

	// ensure functions are marshalled to the underlying unconsolidated block.
	b := NewMockUnconsolidatedBlock(ctrl)
	bb.EXPECT().Unconsolidated().Return(b, nil)

	off, err := offblock.Unconsolidated()
	assert.NoError(t, err)

	iter := NewMockUnconsolidatedStepIter(ctrl)
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

	iter.EXPECT().Meta().Return(buildMeta(now))
	ex := buildMeta(now.Add(offset))
	require.Equal(t, ex, it.Meta())

	vals := []ts.Datapoints{
		{
			ts.Datapoint{
				Timestamp: now,
				Value:     12,
			},
		},
	}

	step := NewMockUnconsolidatedStep(ctrl)
	step.EXPECT().Values().Return(vals).AnyTimes()
	step.EXPECT().Time().Return(now)

	iter.EXPECT().Current().Return(step)
	actual := it.Current()
	expected := []ts.Datapoints{
		{
			ts.Datapoint{
				Timestamp: now.Add(offset),
				Value:     12,
			},
		},
	}

	assert.Equal(t, expected, actual.Values())
	assert.Equal(t, now.Add(offset), actual.Time())
}

func TestUnconsolidatedSeriesIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	bb := NewMockBlock(ctrl)
	defer ctrl.Finish()
	offset := time.Minute
	offblock := NewOffsetBlock(bb, offset)
	now := time.Now()
	msg := "err"
	e := errors.New(msg)

	// ensure functions are marshalled to the underlying unconsolidated block.
	b := NewMockUnconsolidatedBlock(ctrl)
	bb.EXPECT().Unconsolidated().Return(b, nil)

	off, err := offblock.Unconsolidated()
	assert.NoError(t, err)

	iter := NewMockUnconsolidatedSeriesIter(ctrl)
	b.EXPECT().SeriesIter().Return(iter, nil)
	it, err := off.SeriesIter()
	require.NoError(t, err)

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

	iter.EXPECT().Meta().Return(buildMeta(now))
	ex := buildMeta(now.Add(offset))
	require.Equal(t, ex, it.Meta())

	vals := []ts.Datapoints{
		{
			ts.Datapoint{
				Timestamp: now,
				Value:     12,
			},
		},
	}

	unconsolidated := UnconsolidatedSeries{
		datapoints: vals,
	}

	iter.EXPECT().Current().Return(unconsolidated)
	actual := it.Current()
	expected := []ts.Datapoints{
		{
			ts.Datapoint{
				Timestamp: now.Add(offset),
				Value:     12,
			},
		},
	}

	assert.Equal(t, expected, actual.Datapoints())
}
