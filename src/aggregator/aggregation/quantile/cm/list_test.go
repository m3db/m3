// Copyright (c) 2016 Uber Technologies, Inc.
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

package cm

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func validateList(t *testing.T, l *sampleList, expected []float64) {
	t.Helper()
	require.Equal(t, l.Len(), len(expected))
	i := 0
	for sample := l.Front(); sample != nil; sample = sample.next {
		require.Equal(t, expected[i], sample.value)
		i++
	}
	if len(expected) == 0 {
		require.Nil(t, l.head)
		require.Nil(t, l.tail)
	} else {
		require.Equal(t, l.Front(), l.head)
		require.Nil(t, l.head.prev)
		require.Equal(t, l.Back(), l.tail)
		require.Nil(t, l.tail.next)
	}
}

func TestSampleListPushBack(t *testing.T) {
	var (
		l      sampleList
		iter   = 10
		inputs = make([]float64, iter)
	)
	for i := 0; i < iter; i++ {
		inputs[i] = float64(i)
		s := l.Acquire()
		s.value = float64(i)
		l.PushBack(s)
	}
	validateList(t, &l, inputs)
}

func TestSampleListInsertBefore(t *testing.T) {
	var (
		l      sampleList
		iter   = 10
		inputs = make([]float64, iter)
	)
	var prev *Sample
	for i := iter - 1; i >= 0; i-- {
		inputs[i] = float64(i)
		sample := l.Acquire()
		sample.value = float64(i)
		if i == iter-1 {
			l.PushBack(sample)
		} else {
			l.InsertBefore(sample, prev)
		}
		prev = sample
	}
	validateList(t, &l, inputs)
}

func TestSampleListRemove(t *testing.T) {
	var (
		l      sampleList
		iter   = 10
		inputs = make([]float64, iter)
	)
	for i := 0; i < iter; i++ {
		inputs[i] = float64(i)
		sample := l.Acquire()
		sample.value = float64(i)
		l.PushBack(sample)
	}
	for i := 0; i < iter; i++ {
		elem := l.Front()
		l.Remove(elem)
		validateList(t, &l, inputs[i+1:])
	}
}
