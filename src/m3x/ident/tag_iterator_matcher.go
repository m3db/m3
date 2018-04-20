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

package ident

import (
	"errors"
	"fmt"

	"github.com/golang/mock/gomock"
)

var (
	errInvalidNumberInputsToIteratorMatcher = errors.New("inputs must be specified in name-value pairs (i.e. divisible by 2)")
)

// TagIterMatcher is a gomock.Matcher that matches TagIterator
type TagIterMatcher interface {
	gomock.Matcher
}

// NewTagIterMatcher returns a new TagIterMatcher
func NewTagIterMatcher(inputs ...string) (TagIterMatcher, error) {
	if len(inputs)%2 != 0 {
		return nil, errInvalidNumberInputsToIteratorMatcher
	}
	return &tagIterMatcher{inputs: inputs}, nil
}

// MustNewTagIterMatcher returns a new TagIterMatcher, panic'ing if
// it's unable to do so.
func MustNewTagIterMatcher(inputs ...string) TagIterMatcher {
	iter, err := NewTagIterMatcher(inputs...)
	if err != nil {
		panic(err.Error())
	}
	return iter
}

type tagIterMatcher struct {
	inputs []string
}

func (m *tagIterMatcher) Matches(x interface{}) bool {
	t, ok := x.(TagIterator)
	if !ok {
		return false
	}
	t = t.Duplicate()
	i := 0
	for i < len(m.inputs) {
		name, value := m.inputs[i], m.inputs[i+1]
		i += 2
		if !t.Next() {
			return false
		}
		current := t.Current()
		if name != current.Name.String() {
			return false
		}
		if value != current.Value.String() {
			return false
		}
	}
	if t.Next() {
		return false
	}
	if t.Err() != nil {
		return false
	}
	return true
}

func (m *tagIterMatcher) String() string {
	return fmt.Sprintf("tagIter %v", m.inputs)
}
