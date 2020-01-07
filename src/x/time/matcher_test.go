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

package time

import (
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestMatcher(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	t1 := time.Now().UTC()
	t2 := t1.In(loc)

	// Make sure t1 and t2 don't == eachother
	require.NotEqual(t, t1, t2)

	// Make sure t1 and t2 Match eachother
	t1Matcher := NewMatcher(t1)
	require.True(t, t1Matcher.Matches(t2))

	// Make sure the matcher doesn't always return true
	require.NotEqual(t, t1Matcher, t2.Add(1*time.Hour))
	require.NotEqual(t, t1Matcher, 10)
}
