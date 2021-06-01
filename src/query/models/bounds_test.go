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

package models

import (
	"testing"
	"time"

	xtime "github.com/m3db/m3/src/x/time"

	"github.com/stretchr/testify/assert"
)

func TestBounds(t *testing.T) {
	now := xtime.Now()
	bounds := Bounds{
		Start:    now,
		Duration: 0,
		StepSize: time.Minute,
	}

	assert.Equal(t, bounds.Steps(), 0)
	_, err := bounds.TimeForIndex(0)
	assert.Error(t, err, "no valid index in this block")

	bounds = Bounds{
		Start:    now,
		Duration: time.Second,
		StepSize: time.Minute,
	}

	assert.Equal(t, bounds.Steps(), 0)
	_, err = bounds.TimeForIndex(0)
	assert.Error(t, err, "no valid index in this block")

	bounds = Bounds{
		Start:    now,
		Duration: time.Minute,
		StepSize: time.Minute,
	}
	assert.Equal(t, bounds.Steps(), 1)
	it, err := bounds.TimeForIndex(0)
	assert.NoError(t, err)
	assert.Equal(t, it, now)
	_, err = bounds.TimeForIndex(1)
	assert.Error(t, err)

	b := bounds.Next(1)
	assert.Equal(t, b.Start, bounds.End())
	assert.Equal(t, b.StepSize, bounds.StepSize)

	bounds = Bounds{
		Start:    now,
		Duration: 10 * time.Minute,
		StepSize: time.Minute,
	}
	assert.Equal(t, bounds.Steps(), 10)
	it, err = bounds.TimeForIndex(9)
	assert.NoError(t, err)
	assert.Equal(t, it, now.Add(9*bounds.StepSize))
	b = bounds.Next(9)
	assert.Equal(t, b.Start, now.Add(9*bounds.Duration))
	assert.Equal(t, b.StepSize, bounds.StepSize)
	b = b.Previous(9)
	assert.Equal(t, b.Start, bounds.Start)
	assert.Equal(t, b.Duration, bounds.Duration)
}
