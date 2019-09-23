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
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidContainerBlock(t *testing.T) {
	ctrl := gomock.NewController(t)
	now := time.Now()
	defer ctrl.Finish()

	_, err := NewContainerBlock()
	assert.Error(t, err)

	b := NewMockBlock(ctrl)
	b.EXPECT().Meta().Return(buildMeta(now)).AnyTimes()

	b2 := NewMockBlock(ctrl)
	b2.EXPECT().Meta().Return(buildMeta(now.Add(100))).AnyTimes()

	_, err = NewContainerBlock(b, b2)
	require.Error(t, err)

	container, err := NewContainerBlock(b)
	require.NoError(t, err)

	err = container.AddBlock(b)
	require.NoError(t, err)

	err = container.AddBlock(b2)
	require.Error(t, err)
}

var (
	opts            = models.NewTagOptions()
	step            = time.Second
	numSteps        = 10
	now             = time.Now()
	containerBounds = models.Bounds{
		Start:    now,
		StepSize: step,
		Duration: step * time.Duration(numSteps),
	}
)

func TestContainerBlockMergesResultMeta(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	b := NewMockBlock(ctrl)
	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
		ResultMetadata: ResultMetadata{
			Exhaustive: false,
			LocalOnly:  true,
		},
	}

	b.EXPECT().Meta().Return(meta).AnyTimes()

	bTwo := NewMockBlock(ctrl)
	metaTwo := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
		ResultMetadata: ResultMetadata{
			Exhaustive: true,
			LocalOnly:  true,
			Warnings:   []Warning{Warning{"foo", "bar"}},
		},
	}

	bTwo.EXPECT().Meta().Return(metaTwo).AnyTimes()

	container, err := NewContainerBlock(b, bTwo)
	assert.NoError(t, err)

	resultMeta := container.Meta().ResultMetadata
	assert.False(t, resultMeta.Exhaustive)
	assert.True(t, resultMeta.LocalOnly)
	assert.Equal(t, 1, len(resultMeta.Warnings))
}

func buildStepBlock(ctrl *gomock.Controller, v float64, first bool) Block {
	b := NewMockBlock(ctrl)
	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
	}

	b.EXPECT().Meta().Return(meta)
	it := NewMockStepIter(ctrl)
	it.EXPECT().Close()
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Next().Return(true).Times(numSteps)
	it.EXPECT().Next().Return(false)
	for i := 0; i < numSteps; i++ {
		s := NewMockStep(ctrl)
		if first {
			s.EXPECT().Time().Return(now.Add(time.Duration(i) * step))
		}

		s.EXPECT().Values().Return([]float64{v})
		it.EXPECT().Current().Return(s)
	}

	it.EXPECT().SeriesMeta().
		Return([]SeriesMeta{{Name: []byte(fmt.Sprint(v))}}).Times(2)
	if first {
		it.EXPECT().StepCount().Return(numSteps)
	}

	b.EXPECT().StepIter().Return(it, nil)
	return b
}

func TestContainerStepIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := buildStepBlock(ctrl, 1, true)
	blockTwo := buildStepBlock(ctrl, 2, false)

	container, err := NewContainerBlock(block, blockTwo)
	require.NoError(t, err)

	assert.True(t, containerBounds.Equals(container.Meta().Bounds))
	assert.True(t, opts.Equals(container.Meta().Tags.Opts))

	it, err := container.StepIter()
	require.NoError(t, err)

	count := 0
	ex := []float64{1, 2}
	for it.Next() {
		st := it.Current()
		assert.Equal(t, ex, st.Values())
		assert.Equal(t, now.Add(step*time.Duration(count)), st.Time())
		count++
	}

	assert.Equal(t, count, numSteps)

	metas := it.SeriesMeta()
	assert.Equal(t, 2, len(metas))
	assert.Equal(t, []byte("1"), metas[0].Name)
	assert.Equal(t, []byte("2"), metas[1].Name)
	assert.Equal(t, numSteps, it.StepCount())

	assert.NoError(t, it.Err())
	assert.NotPanics(t, func() { it.Close() })
}

func buildSeriesBlock(ctrl *gomock.Controller, v float64, first bool) Block {
	b := NewMockBlock(ctrl)
	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
	}

	b.EXPECT().Meta().Return(meta).AnyTimes()
	it := NewMockSeriesIter(ctrl)
	it.EXPECT().Close()
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Next().Return(true)
	it.EXPECT().Next().Return(false)
	vals := make([]float64, numSteps)
	for i := range vals {
		vals[i] = v
	}

	it.EXPECT().Current().Return(Series{
		values: vals,
		Meta:   SeriesMeta{Name: []byte(fmt.Sprint(v))},
	})

	it.EXPECT().SeriesMeta().
		Return([]SeriesMeta{{Name: []byte(fmt.Sprint(v))}}).Times(2)
	b.EXPECT().SeriesIter().Return(it, nil)
	return b
}

func TestContainerSeriesIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := buildSeriesBlock(ctrl, 1, true)
	blockTwo := buildSeriesBlock(ctrl, 2, false)

	container, err := NewContainerBlock(block, blockTwo)
	require.NoError(t, err)

	assert.True(t, containerBounds.Equals(container.Meta().Bounds))
	assert.True(t, opts.Equals(container.Meta().Tags.Opts))

	it, err := container.SeriesIter()
	require.NoError(t, err)

	ex := 1.0
	for it.Next() {
		current := it.Current()
		assert.Equal(t, numSteps, current.Len())
		for _, ac := range current.Values() {
			assert.Equal(t, ex, ac)
		}

		assert.Equal(t, []byte(fmt.Sprint(ex)), current.Meta.Name)
		ex++
	}

	metas := it.SeriesMeta()
	assert.Equal(t, 2, len(metas))
	assert.Equal(t, []byte("1"), metas[0].Name)
	assert.Equal(t, []byte("2"), metas[1].Name)

	assert.NoError(t, it.Err())
	assert.NotPanics(t, func() { it.Close() })
}

func buildUnconsolidatedStepBlock(ctrl *gomock.Controller,
	v float64, first bool) Block {
	base := NewMockBlock(ctrl)
	b := NewMockUnconsolidatedBlock(ctrl)
	base.EXPECT().Unconsolidated().Return(b, nil)

	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
	}

	base.EXPECT().Meta().Return(meta).AnyTimes()
	it := NewMockUnconsolidatedStepIter(ctrl)
	it.EXPECT().Close()
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Next().Return(true).Times(numSteps)
	it.EXPECT().Next().Return(false)
	for i := 0; i < numSteps; i++ {
		s := NewMockUnconsolidatedStep(ctrl)
		tt := now.Add(time.Duration(i) * step)
		if first {
			s.EXPECT().Time().Return(tt)
		}

		s.EXPECT().Values().Return([]ts.Datapoints{
			ts.Datapoints{
				ts.Datapoint{Timestamp: tt, Value: v},
				ts.Datapoint{Timestamp: tt, Value: v},
			},
		})

		it.EXPECT().Current().Return(s)
	}

	it.EXPECT().SeriesMeta().
		Return([]SeriesMeta{{Name: []byte(fmt.Sprint(v))}}).Times(2)
	if first {
		it.EXPECT().StepCount().Return(numSteps)
	}

	b.EXPECT().StepIter().Return(it, nil)
	return base
}

func TestUnconsolidatedContainerStepIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := buildUnconsolidatedStepBlock(ctrl, 1, true)
	blockTwo := buildUnconsolidatedStepBlock(ctrl, 2, false)

	c, err := NewContainerBlock(block, blockTwo)
	require.NoError(t, err)

	consolidated, err := c.Unconsolidated()
	require.NoError(t, err)

	assert.True(t, containerBounds.Equals(consolidated.Meta().Bounds))
	assert.True(t, opts.Equals(consolidated.Meta().Tags.Opts))

	it, err := consolidated.StepIter()
	require.NoError(t, err)

	count := 0
	for it.Next() {
		st := it.Current()
		tt := now.Add(step * time.Duration(count))
		ex := []ts.Datapoints{
			ts.Datapoints{
				ts.Datapoint{Timestamp: tt, Value: 1},
				ts.Datapoint{Timestamp: tt, Value: 1},
			},
			ts.Datapoints{
				ts.Datapoint{Timestamp: tt, Value: 2},
				ts.Datapoint{Timestamp: tt, Value: 2},
			},
		}

		assert.Equal(t, ex, st.Values())
		count++
	}

	assert.Equal(t, count, numSteps)

	metas := it.SeriesMeta()
	assert.Equal(t, 2, len(metas))
	assert.Equal(t, []byte("1"), metas[0].Name)
	assert.Equal(t, []byte("2"), metas[1].Name)
	assert.Equal(t, numSteps, it.StepCount())

	assert.NoError(t, it.Err())
	assert.NotPanics(t, func() { it.Close() })
}

func buildUnconsolidatedSeriesBlock(ctrl *gomock.Controller,
	v float64, first bool) Block {
	base := NewMockBlock(ctrl)
	b := NewMockUnconsolidatedBlock(ctrl)
	base.EXPECT().Unconsolidated().Return(b, nil)

	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
	}

	base.EXPECT().Meta().Return(meta).AnyTimes()
	it := NewMockUnconsolidatedSeriesIter(ctrl)
	it.EXPECT().Close()
	it.EXPECT().Err().Return(nil).AnyTimes()
	it.EXPECT().Next().Return(true)
	it.EXPECT().Next().Return(false)
	vals := make([]ts.Datapoints, numSteps)
	for i := range vals {
		tt := now.Add(time.Duration(i) * step)
		vals[i] = ts.Datapoints{
			ts.Datapoint{Timestamp: tt, Value: v},
			ts.Datapoint{Timestamp: tt, Value: v},
		}
	}

	it.EXPECT().Current().Return(UnconsolidatedSeries{
		datapoints: vals,
		Meta:       SeriesMeta{Name: []byte(fmt.Sprint(v))},
	})

	it.EXPECT().SeriesMeta().
		Return([]SeriesMeta{{Name: []byte(fmt.Sprint(v))}}).Times(2)
	b.EXPECT().SeriesIter().Return(it, nil)
	return base
}

func TestUnconsolidatedContainerSeriesIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := buildUnconsolidatedSeriesBlock(ctrl, 1, true)
	blockTwo := buildUnconsolidatedSeriesBlock(ctrl, 2, false)

	c, err := NewContainerBlock(block, blockTwo)
	require.NoError(t, err)

	consolidated, err := c.Unconsolidated()
	require.NoError(t, err)

	assert.True(t, containerBounds.Equals(consolidated.Meta().Bounds))
	assert.True(t, opts.Equals(consolidated.Meta().Tags.Opts))

	it, err := consolidated.SeriesIter()
	require.NoError(t, err)

	buildExpected := func(v float64) []ts.Datapoints {
		expected := make([]ts.Datapoints, numSteps)
		for i := range expected {
			expected[i] = make(ts.Datapoints, 2)
			for j := range expected[i] {
				expected[i][j] = ts.Datapoint{
					Timestamp: now.Add(time.Duration(i) * step),
					Value:     float64(v),
				}
			}
		}

		return expected
	}

	expected := [][]ts.Datapoints{buildExpected(1), buildExpected(2)}
	ex := 0
	for it.Next() {
		current := it.Current()
		assert.Equal(t, numSteps, current.Len())
		assert.Equal(t, expected[ex], current.Datapoints())
		ex++

		assert.Equal(t, []byte(fmt.Sprint(ex)), current.Meta.Name)
	}

	metas := it.SeriesMeta()
	assert.Equal(t, 2, len(metas))
	assert.Equal(t, []byte("1"), metas[0].Name)
	assert.Equal(t, []byte("2"), metas[1].Name)

	assert.NoError(t, it.Err())
	assert.NotPanics(t, func() { it.Close() })
}
