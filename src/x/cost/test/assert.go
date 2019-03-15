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

// Package test contains testing utilities for the cost package.
package test

import (
	"github.com/m3db/m3/src/x/cost"

	"github.com/stretchr/testify/assert"
)

// AssertCurrentCost is a helper assertion to check that an enforcer has the
// given cost.
func AssertCurrentCost(t assert.TestingT, expectedCost cost.Cost, ef cost.Enforcer) {
	actual, _ := ef.State()
	assert.Equal(t, expectedCost, actual.Cost)
}

// AssertLimitError checks that err is a limit error with the given parameters
func AssertLimitError(t assert.TestingT, err error, current, threshold cost.Cost) {
	AssertLimitErrorWithMsg(t, err, "", current, threshold)
}

// AssertLimitErrorWithMsg checks that err is a limit error with the given parameters
func AssertLimitErrorWithMsg(t assert.TestingT, err error, msg string, current, threshold cost.Cost) {
	assert.EqualError(t, err, cost.NewCostExceededError(msg, current, threshold).Error())
}
