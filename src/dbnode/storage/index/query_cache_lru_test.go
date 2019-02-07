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

package index

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/postings"
	"github.com/pborman/uuid"
)

func TestLRU(t *testing.T) {
	evictCounter := 0
	onEvicted := func(uuid uuid.UUID, pattern string, postingsList postings.List) {
		if k != v {
			t.Fatalf("Evict values not equal (%v!=%v)", k, v)
		}
		evictCounter++
	}

	l, err := NewLRU(128, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	for i := 0; i < 256; i++ {
		l.Add(i, i)
	}
	if l.Len() != 128 {
		t.Fatalf("bad len: %v", l.Len())
	}

	if evictCounter != 128 {
		t.Fatalf("bad evict count: %v", evictCounter)
	}

	for i, k := range l.Keys() {
		if v, ok := l.Get(k); !ok || v != k || v != i+128 {
			t.Fatalf("bad key: %v", k)
		}
	}
	for i := 0; i < 128; i++ {
		_, ok := l.Get(i)
		if ok {
			t.Fatalf("should be evicted")
		}
	}
	for i := 128; i < 256; i++ {
		_, ok := l.Get(i)
		if !ok {
			t.Fatalf("should not be evicted")
		}
	}
	for i := 128; i < 192; i++ {
		ok := l.Remove(i)
		if !ok {
			t.Fatalf("should be contained")
		}
		ok = l.Remove(i)
		if ok {
			t.Fatalf("should not be contained")
		}
		_, ok = l.Get(i)
		if ok {
			t.Fatalf("should be deleted")
		}
	}

	l.Get(192) // expect 192 to be last key in l.Keys()

	for i, k := range l.Keys() {
		if (i < 63 && k != i+193) || (i == 63 && k != 192) {
			t.Fatalf("out of order key: %v", k)
		}
	}
}

// Test that Add returns true/false if an eviction occurred
func TestLRU_Add(t *testing.T) {
	evictCounter := 0
	onEvicted := func(k interface{}, v interface{}) {
		evictCounter++
	}

	l, err := NewLRU(1, onEvicted)
	if err != nil {
		t.Fatalf("err: %v", err)
	}

	if l.Add(1, 1) == true || evictCounter != 0 {
		t.Errorf("should not have an eviction")
	}
	if l.Add(2, 2) == false || evictCounter != 1 {
		t.Errorf("should have an eviction")
	}
}
