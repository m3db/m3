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
	xtest "github.com/m3db/m3/src/x/test"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInvalidContainerBlock(t *testing.T) {
	ctrl := xtest.NewController(t)
	now := xtime.Now()
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
	now             = xtime.Now()
	containerBounds = models.Bounds{
		Start:    now,
		StepSize: step,
		Duration: step * time.Duration(numSteps),
	}
)

func TestContainerBlockMergesResultMeta(t *testing.T) {
	ctrl := xtest.NewController(t)
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
			Warnings:   []Warning{{"foo", "bar"}},
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
	ctrl := xtest.NewController(t)
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

func buildUnconsolidatedSeriesBlock(ctrl *gomock.Controller,
	v float64, first bool) Block {
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
	vals := make(ts.Datapoints, 0, numSteps)
	for i := 0; i < numSteps; i++ {
		tt := now.Add(time.Duration(i) * step)
		vals = append(vals,
			ts.Datapoint{Timestamp: tt, Value: v},
			ts.Datapoint{Timestamp: tt, Value: v},
		)
	}

	it.EXPECT().Current().Return(UnconsolidatedSeries{
		datapoints: vals,
		Meta:       SeriesMeta{Name: []byte(fmt.Sprint(v))},
	})

	it.EXPECT().SeriesMeta().
		Return([]SeriesMeta{{Name: []byte(fmt.Sprint(v))}}).Times(2)
	b.EXPECT().SeriesIter().Return(it, nil)
	return b
}

func buildExpected(v float64) ts.Datapoints {
	expected := make(ts.Datapoints, 0, numSteps)
	for i := 0; i < numSteps; i++ {
		expected = append(expected, ts.Datapoint{
			Timestamp: now.Add(time.Duration(i) * step),
			Value:     float64(v),
		}, ts.Datapoint{
			Timestamp: now.Add(time.Duration(i) * step),
			Value:     float64(v),
		})
	}

	return expected
}

func TestUnconsolidatedContainerSeriesIter(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	block := buildUnconsolidatedSeriesBlock(ctrl, 1, true)
	blockTwo := buildUnconsolidatedSeriesBlock(ctrl, 2, false)

	c, err := NewContainerBlock(block, blockTwo)
	require.NoError(t, err)

	assert.True(t, containerBounds.Equals(c.Meta().Bounds))
	assert.True(t, opts.Equals(c.Meta().Tags.Opts))

	it, err := c.SeriesIter()
	require.NoError(t, err)

	expected := []ts.Datapoints{buildExpected(1), buildExpected(2)}
	ex := 0
	for it.Next() {
		current := it.Current()
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

func buildMultiSeriesBlock(
	ctrl *gomock.Controller,
	count int,
	concurrency int,
	v float64,
) Block {
	b := NewMockBlock(ctrl)

	meta := Metadata{
		Tags:   models.NewTags(0, models.NewTagOptions()),
		Bounds: containerBounds,
	}

	b.EXPECT().Meta().Return(meta).AnyTimes()
	batches := make([]SeriesIterBatch, 0, concurrency)
	for i := 0; i < count; i++ {
		it := NewMockSeriesIter(ctrl)
		it.EXPECT().Close()
		it.EXPECT().Err().Return(nil).AnyTimes()
		it.EXPECT().Next().Return(true)
		it.EXPECT().Next().Return(false)
		vals := make(ts.Datapoints, 0, numSteps)
		for i := 0; i < numSteps; i++ {
			tt := now.Add(time.Duration(i) * step)
			vals = append(vals,
				ts.Datapoint{Timestamp: tt, Value: v},
				ts.Datapoint{Timestamp: tt, Value: v},
			)
		}

		it.EXPECT().Current().Return(UnconsolidatedSeries{
			datapoints: vals,
			Meta:       SeriesMeta{Name: []byte(fmt.Sprintf("%d_%d", i, int(v)))},
		})

		batches = append(batches, SeriesIterBatch{Iter: it, Size: 1})
	}

	b.EXPECT().MultiSeriesIter(concurrency).Return(batches, nil)
	return b
}

func TestUnconsolidatedContainerMultiSeriesIter(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	concurrency := 5
	c, err := NewContainerBlock(
		buildMultiSeriesBlock(ctrl, 0, concurrency, 1),
		buildMultiSeriesBlock(ctrl, 4, concurrency, 2),
		buildMultiSeriesBlock(ctrl, 5, concurrency, 3),
		buildMultiSeriesBlock(ctrl, 1, concurrency, 4),
	)

	require.NoError(t, err)
	assert.True(t, containerBounds.Equals(c.Meta().Bounds))
	assert.True(t, opts.Equals(c.Meta().Tags.Opts))

	batch, err := c.MultiSeriesIter(concurrency)
	require.NoError(t, err)
	require.Equal(t, concurrency, len(batch))

	expected := [][]ts.Datapoints{
		[]{buildExpected(2), buildExpected(3), buildExpected(4)},
		[]{buildExpected(2), buildExpected(3)},
		[]{buildExpected(2), buildExpected(3)},
		[]{buildExpected(2), buildExpected(3)},
		[]{buildExpected(3)},
	}

	expectedNames := []string{
		"0_2", "0_3", "0_4",
		"1_2", "1_3",
		"2_2", "2_3",
		"3_2", "3_3",
		"4_3",
	}

	n := 0
	for i, b := range batch {
		ex := expected[i]
		assert.Equal(t, len(ex), b.Size)

		i := 0
		for b.Iter.Next() {
			current := b.Iter.Current()
			assert.Equal(t, ex[i], current.Datapoints())
			i++

			assert.Equal(t, expectedNames[n], string(current.Meta.Name))
			n++
		}
	}

	for _, b := range batch {
		assert.NoError(t, b.Iter.Err())
		assert.NotPanics(t, func() { b.Iter.Close() })
	}
}
