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

// This implementation is a modification from Go's container/list source code.
// Here is its license:

// Copyright (c) 2009 The Go Authors. All rights reserved.

// Redistribution and use in source and binary forms, with or without
// modification, are permitted provided that the following conditions are
// met:

//    * Redistributions of source code must retain the above copyright
// notice, this list of conditions and the following disclaimer.
//    * Redistributions in binary form must reproduce the above
// copyright notice, this list of conditions and the following disclaimer
// in the documentation and/or other materials provided with the
// distribution.
//    * Neither the name of Google Inc. nor the names of its
// contributors may be used to endorse or promote products derived from
// this software without specific prior written permission.

// THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
// "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
// LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
// A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
// OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
// SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
// LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
// DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
// THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
// (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
// OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
package list

import (
	"testing"
)

func checkListLen(t *testing.T, l *List, len int) bool {
	if n := l.Len(); n != len {
		t.Errorf("l.Len() = %d, want %d", n, len)
		return false
	}
	return true
}

func checkListPointers(t *testing.T, l *List, es []*Element) {
	root := &l.root

	if !checkListLen(t, l, len(es)) {
		return
	}

	// zero length lists must be the zero value or properly initialized (sentinel circle)
	if len(es) == 0 {
		if l.root.next != nil && l.root.next != root || l.root.prev != nil && l.root.prev != root {
			t.Errorf("l.root.next = %p, l.root.prev = %p; both should both be nil or %p", l.root.next, l.root.prev, root)
		}
		return
	}
	// len(es) > 0

	// check internal and external prev/next connections
	for i, e := range es {
		prev := root
		Prev := (*Element)(nil)
		if i > 0 {
			prev = es[i-1]
			Prev = prev
		}
		if p := e.prev; p != prev {
			t.Errorf("elt[%d](%p).prev = %p, want %p", i, e, p, prev)
		}
		if p := e.Prev(); p != Prev {
			t.Errorf("elt[%d](%p).Prev() = %p, want %p", i, e, p, Prev)
		}

		next := root
		Next := (*Element)(nil)
		if i < len(es)-1 {
			next = es[i+1]
			Next = next
		}
		if n := e.next; n != next {
			t.Errorf("elt[%d](%p).next = %p, want %p", i, e, n, next)
		}
		if n := e.Next(); n != Next {
			t.Errorf("elt[%d](%p).Next() = %p, want %p", i, e, n, Next)
		}
	}
}

func TestList(t *testing.T) {
	l := newList(nil)
	checkListPointers(t, l, []*Element{})

	// Single element list
	e := l.PushFront("a")
	checkListPointers(t, l, []*Element{e})
	l.MoveToFront(e)
	checkListPointers(t, l, []*Element{e})
	l.MoveToBack(e)
	checkListPointers(t, l, []*Element{e})
	l.Remove(e)
	checkListPointers(t, l, []*Element{})

	// Bigger list
	e2 := l.PushFront(2)
	e1 := l.PushFront(1)
	e3 := l.PushBack(3)
	e4 := l.PushBack("banana")
	checkListPointers(t, l, []*Element{e1, e2, e3, e4})

	l.Remove(e2)
	checkListPointers(t, l, []*Element{e1, e3, e4})

	l.MoveToFront(e3) // move from middle
	checkListPointers(t, l, []*Element{e3, e1, e4})

	l.MoveToFront(e1)
	l.MoveToBack(e3) // move from middle
	checkListPointers(t, l, []*Element{e1, e4, e3})

	l.MoveToFront(e3) // move from back
	checkListPointers(t, l, []*Element{e3, e1, e4})
	l.MoveToFront(e3) // should be no-op
	checkListPointers(t, l, []*Element{e3, e1, e4})

	l.MoveToBack(e3) // move from front
	checkListPointers(t, l, []*Element{e1, e4, e3})
	l.MoveToBack(e3) // should be no-op
	checkListPointers(t, l, []*Element{e1, e4, e3})

	e2 = l.InsertBefore(2, e1) // insert before front
	checkListPointers(t, l, []*Element{e2, e1, e4, e3})
	l.Remove(e2)
	e2 = l.InsertBefore(2, e4) // insert before middle
	checkListPointers(t, l, []*Element{e1, e2, e4, e3})
	l.Remove(e2)
	e2 = l.InsertBefore(2, e3) // insert before back
	checkListPointers(t, l, []*Element{e1, e4, e2, e3})
	l.Remove(e2)

	e2 = l.InsertAfter(2, e1) // insert after front
	checkListPointers(t, l, []*Element{e1, e2, e4, e3})
	l.Remove(e2)
	e2 = l.InsertAfter(2, e4) // insert after middle
	checkListPointers(t, l, []*Element{e1, e4, e2, e3})
	l.Remove(e2)
	e2 = l.InsertAfter(2, e3) // insert after back
	checkListPointers(t, l, []*Element{e1, e4, e3, e2})
	l.Remove(e2)

	// Check standard iteration.
	sum := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if i, ok := e.Value.(int); ok {
			sum += i
		}
	}
	if sum != 4 {
		t.Errorf("sum over l = %d, want 4", sum)
	}

	// Clear all elements by iterating
	var next *Element
	for e := l.Front(); e != nil; e = next {
		next = e.Next()
		l.Remove(e)
	}
	checkListPointers(t, l, []*Element{})
}

func checkList(t *testing.T, l *List, es []interface{}) {
	if !checkListLen(t, l, len(es)) {
		return
	}

	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		le := e.Value.(int)
		if le != es[i] {
			t.Errorf("elt[%d].Value = %v, want %v", i, le, es[i])
		}
		i++
	}
}

func TestExtending(t *testing.T) {
	l1 := newList(nil)
	l2 := newList(nil)

	l1.PushBack(1)
	l1.PushBack(2)
	l1.PushBack(3)

	l2.PushBack(4)
	l2.PushBack(5)

	l3 := newList(nil)
	l3.PushBackList(l1)
	checkList(t, l3, []interface{}{1, 2, 3})
	l3.PushBackList(l2)
	checkList(t, l3, []interface{}{1, 2, 3, 4, 5})

	l3 = newList(nil)
	l3.PushFrontList(l2)
	checkList(t, l3, []interface{}{4, 5})
	l3.PushFrontList(l1)
	checkList(t, l3, []interface{}{1, 2, 3, 4, 5})

	checkList(t, l1, []interface{}{1, 2, 3})
	checkList(t, l2, []interface{}{4, 5})

	l3 = newList(nil)
	l3.PushBackList(l1)
	checkList(t, l3, []interface{}{1, 2, 3})
	l3.PushBackList(l3)
	checkList(t, l3, []interface{}{1, 2, 3, 1, 2, 3})

	l3 = newList(nil)
	l3.PushFrontList(l1)
	checkList(t, l3, []interface{}{1, 2, 3})
	l3.PushFrontList(l3)
	checkList(t, l3, []interface{}{1, 2, 3, 1, 2, 3})

	l3 = newList(nil)
	l1.PushBackList(l3)
	checkList(t, l1, []interface{}{1, 2, 3})
	l1.PushFrontList(l3)
	checkList(t, l1, []interface{}{1, 2, 3})
}

func TestRemove(t *testing.T) {
	l := newList(nil)
	e1 := l.PushBack(1)
	e2 := l.PushBack(2)
	checkListPointers(t, l, []*Element{e1, e2})
	e := l.Front()
	l.Remove(e)
	checkListPointers(t, l, []*Element{e2})
	l.Remove(e)
	checkListPointers(t, l, []*Element{e2})
}

func TestIssue4103(t *testing.T) {
	l1 := newList(nil)
	l1.PushBack(1)
	l1.PushBack(2)

	l2 := newList(nil)
	l2.PushBack(3)
	l2.PushBack(4)

	e := l1.Front()
	l2.Remove(e) // l2 should not change because e is not an element of l2
	if n := l2.Len(); n != 2 {
		t.Errorf("l2.Len() = %d, want 2", n)
	}

	l1.InsertBefore(8, e)
	if n := l1.Len(); n != 3 {
		t.Errorf("l1.Len() = %d, want 3", n)
	}
}

func TestIssue6349(t *testing.T) {
	l := newList(nil)
	l.PushBack(1)
	l.PushBack(2)

	e := l.Front()
	l.Remove(e)
	if e.Value != 1 {
		t.Errorf("e.value = %d, want 1", e.Value)
	}
	if e.Next() != nil {
		t.Errorf("e.Next() != nil")
	}
	if e.Prev() != nil {
		t.Errorf("e.Prev() != nil")
	}
}

func TestMove(t *testing.T) {
	l := newList(nil)
	e1 := l.PushBack(1)
	e2 := l.PushBack(2)
	e3 := l.PushBack(3)
	e4 := l.PushBack(4)

	l.MoveAfter(e3, e3)
	checkListPointers(t, l, []*Element{e1, e2, e3, e4})
	l.MoveBefore(e2, e2)
	checkListPointers(t, l, []*Element{e1, e2, e3, e4})

	l.MoveAfter(e3, e2)
	checkListPointers(t, l, []*Element{e1, e2, e3, e4})
	l.MoveBefore(e2, e3)
	checkListPointers(t, l, []*Element{e1, e2, e3, e4})

	l.MoveBefore(e2, e4)
	checkListPointers(t, l, []*Element{e1, e3, e2, e4})
	e2, e3 = e3, e2

	l.MoveBefore(e4, e1)
	checkListPointers(t, l, []*Element{e4, e1, e2, e3})
	e1, e2, e3, e4 = e4, e1, e2, e3

	l.MoveAfter(e4, e1)
	checkListPointers(t, l, []*Element{e1, e4, e2, e3})
	e2, e3, e4 = e4, e2, e3

	l.MoveAfter(e2, e3)
	checkListPointers(t, l, []*Element{e1, e3, e2, e4})
	e2, e3 = e3, e2
}

// Test PushFront, PushBack, PushFrontList, PushBackList with uninitialized List
func TestZeroList(t *testing.T) {
	var l1 = new(List)
	l1.PushFront(1)
	checkList(t, l1, []interface{}{1})

	var l2 = new(List)
	l2.PushBack(1)
	checkList(t, l2, []interface{}{1})

	var l3 = new(List)
	l3.PushFrontList(l1)
	checkList(t, l3, []interface{}{1})

	var l4 = new(List)
	l4.PushBackList(l2)
	checkList(t, l4, []interface{}{1})
}

// Test that a list l is not modified when calling InsertBefore with a mark that is not an element of l.
func TestInsertBeforeUnknownMark(t *testing.T) {
	var l List
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)
	l.InsertBefore(1, new(Element))
	checkList(t, &l, []interface{}{1, 2, 3})
}

// Test that a list l is not modified when calling InsertAfter with a mark that is not an element of l.
func TestInsertAfterUnknownMark(t *testing.T) {
	var l List
	l.PushBack(1)
	l.PushBack(2)
	l.PushBack(3)
	l.InsertAfter(1, new(Element))
	checkList(t, &l, []interface{}{1, 2, 3})
}

// Test that a list l is not modified when calling MoveAfter or MoveBefore with a mark that is not an element of l.
func TestMoveUnknownMark(t *testing.T) {
	var l1 List
	e1 := l1.PushBack(1)

	var l2 List
	e2 := l2.PushBack(2)

	l1.MoveAfter(e1, e2)
	checkList(t, &l1, []interface{}{1})
	checkList(t, &l2, []interface{}{2})

	l1.MoveBefore(e1, e2)
	checkList(t, &l1, []interface{}{1})
	checkList(t, &l2, []interface{}{2})
}

func TestReset(t *testing.T) {
	var l1 List
	l1.Reset()
	checkList(t, &l1, []interface{}{})

	l1.PushBack(1)
	checkList(t, &l1, []interface{}{1})
	l1.Reset()
	checkList(t, &l1, []interface{}{})

	l1.PushBack(2)
	l1.PushBack(3)
	l1.PushBack(4)
	l1.PushBack(5)
	checkList(t, &l1, []interface{}{2, 3, 4, 5})
	l1.Reset()
	checkList(t, &l1, []interface{}{})
}
