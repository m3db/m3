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

package test

import (
	"fmt"

	"github.com/golang/mock/gomock"
	"github.com/google/go-cmp/cmp"
)

// CmpMatcher returns a new matcher backed by go-cmp/cmp.Equal.
func CmpMatcher(x interface{}, opts ...cmp.Option) gomock.Matcher {
	return &matcher{x, opts}
}

type matcher struct {
	expected interface{}
	opts     cmp.Options
}

func (m matcher) Matches(x interface{}) bool {
	return cmp.Equal(m.expected, x, m.opts...)
}

func (m matcher) String() string {
	return fmt.Sprintf("%v", m.expected)
}
