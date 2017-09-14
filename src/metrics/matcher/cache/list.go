// Copyright (c) 2017 Uber Technologies, Inc.
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

package cache

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/m3db/m3metrics/rules"
	xid "github.com/m3db/m3x/id"
)

// element is a list element
type element struct {
	nsHash      xid.Hash
	idHash      xid.Hash
	result      rules.MatchResult
	deleted     bool
	expiryNanos int64
	prev        *element
	next        *element
}

// ShouldPromote determines whether the previous promotion has expired
// and we should perform a new promotion.
func (e *element) ShouldPromote(now time.Time) bool {
	return atomic.LoadInt64(&e.expiryNanos) <= now.UnixNano()
}

// SetPromotionExpiry sets the expiry time of the current promotion.
func (e *element) SetPromotionExpiry(t time.Time) {
	atomic.StoreInt64(&e.expiryNanos, t.UnixNano())
}

// list is a type-specific implementation of doubly linked lists consisting
// of elements so when we retrieve match results, there is no conversion
// between interface{} and the concrete result type.
type list struct {
	head   *element
	tail   *element
	length int
}

// Len returns the number of elements in the list.
func (l *list) Len() int { return l.length }

// Front returns the first element in the list.
func (l *list) Front() *element { return l.head }

// Back returns the last element in the list.
func (l *list) Back() *element { return l.tail }

// PushFront pushes an element to the front of the list.
func (l *list) PushFront(elem *element) {
	if elem == nil || elem.deleted {
		return
	}
	if l.head == nil {
		l.head = elem
		l.tail = elem
		elem.prev = nil
		elem.next = nil
	} else {
		elem.next = l.head
		elem.prev = nil
		l.head.prev = elem
		l.head = elem
	}
	l.length++
}

// PushBack pushes an element to the back of the list.
func (l *list) PushBack(elem *element) {
	if elem == nil || elem.deleted {
		return
	}
	if l.tail == nil {
		l.head = elem
		l.tail = elem
		elem.prev = nil
		elem.next = nil
	} else {
		elem.prev = l.tail
		elem.next = nil
		l.tail.next = elem
		l.tail = elem
	}
	l.length++
}

// Remove removes an element from the list.
func (l *list) Remove(elem *element) {
	if elem == nil || elem.deleted {
		return
	}

	// Remove the element from the list.
	l.remove(elem, true)
}

// MoveToFront moves an element to the beginning of the list.
func (l *list) MoveToFront(elem *element) {
	if elem == nil || elem.deleted {
		return
	}

	// Removing the element from the list.
	l.remove(elem, false)

	// Pushing the element to the front of the list.
	l.PushFront(elem)
}

func (l *list) remove(elem *element, markDeleted bool) {
	prev := elem.prev
	next := elem.next
	if prev == nil {
		l.head = next
	} else {
		prev.next = next
	}
	if next == nil {
		l.tail = prev
	} else {
		next.prev = prev
	}
	l.length--
	elem.prev = nil // avoid memory leak.
	elem.next = nil // avoid memory leak.
	elem.deleted = markDeleted
}

type lockedList struct {
	sync.Mutex
	list
}
