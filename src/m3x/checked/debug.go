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

package checked

import (
	"bytes"
	"fmt"
	"runtime/debug"
	"time"
)

const (
	defaultTraceback       = false
	defaultTracebackCycles = 3
)

var (
	traceback       = defaultTraceback
	tracebackCycles = defaultTracebackCycles
	panicFn         = defaultPanic
)

// PanicFn is a panic function to call on invalid checked state
type PanicFn func(e error)

// SetPanicFn sets the panic function
func SetPanicFn(fn PanicFn) {
	panicFn = fn
}

// Panic will execute the currently set panic function
func Panic(e error) {
	panicFn(e)
}

// ResetPanicFn resets the panic function to the default runtime panic
func ResetPanicFn() {
	panicFn = defaultPanic
}

func defaultPanic(e error) {
	panic(e)
}

// SetTraceback sets whether to traceback events
func SetTraceback(value bool) {
	traceback = value
}

// SetTracebackCycles sets the count of traceback cycles to keep if enabled
func SetTracebackCycles(value int) {
	tracebackCycles = value
}

func panicRef(c *RefCount, err error) {
	if traceback {
		trace := getDebuggerRef(c).String()
		err = fmt.Errorf("%v, traceback:\n\n%s", err, trace)
	}
	panicFn(err)
}

type debuggerEvent int

const (
	incRefEvent debuggerEvent = iota
	decRefEvent
	moveRefEvent
	incReadsEvent
	decReadsEvent
	incWritesEvent
	decWritesEvent
)

func (d debuggerEvent) String() string {
	switch d {
	case incRefEvent:
		return "IncRef"
	case decRefEvent:
		return "DecRef"
	case moveRefEvent:
		return "MoveRef"
	case incReadsEvent:
		return "IncReads"
	case decReadsEvent:
		return "DecReads"
	case incWritesEvent:
		return "IncWrites"
	case decWritesEvent:
		return "DecWrites"
	}
	return "Unknown"
}

type debugger struct {
	entries [][]*debuggerEntry
}

func (d *debugger) append(event debuggerEvent, ref int, stack []byte) {
	if len(d.entries) == 0 {
		d.entries = make([][]*debuggerEntry, 1, tracebackCycles)
	}
	idx := len(d.entries) - 1
	d.entries[idx] = append(d.entries[idx], &debuggerEntry{
		event: event,
		ref:   ref,
		stack: stack,
		t:     time.Now(),
	})
	if event == decRefEvent && ref == 0 {
		if len(d.entries) == tracebackCycles {
			// Shift all tracebacks back one if at end of traceback cycles
			for i := 1; i < len(d.entries); i++ {
				d.entries[i-1] = d.entries[i]
			}
			d.entries[idx] = nil
		} else {
			// Begin writing new events to the next cycle
			d.entries = d.entries[:len(d.entries)+1]
		}
	}
}

func (d *debugger) String() string {
	buffer := bytes.NewBuffer(nil)
	// Reverse the entries for time descending
	for i := len(d.entries) - 1; i >= 0; i-- {
		for j := len(d.entries[i]) - 1; j >= 0; j-- {
			buffer.WriteString(d.entries[i][j].String())
		}
	}
	return buffer.String()
}

type debuggerRef struct {
	debugger
	finalizer Finalizer
}

func (d *debuggerRef) Finalize() {
	if d.finalizer != nil {
		d.finalizer.Finalize()
	}
}

type debuggerEntry struct {
	event debuggerEvent
	ref   int
	stack []byte
	t     time.Time
}

func (e *debuggerEntry) String() string {
	return fmt.Sprintf("%s, ref=%d, unixnanos=%d:\n%s\n",
		e.event.String(), e.ref, e.t.UnixNano(), string(e.stack))
}

func getDebuggerRef(c *RefCount) *debuggerRef {
	if c.finalizer == nil {
		c.finalizer = &debuggerRef{}
		return c.finalizer.(*debuggerRef)
	}

	debugger, ok := c.finalizer.(*debuggerRef)
	if !ok {
		// Wrap the existing finalizer in a debuggerRef
		c.finalizer = &debuggerRef{finalizer: c.finalizer}
		return c.finalizer.(*debuggerRef)
	}

	return debugger
}

func tracebackEvent(c *RefCount, ref int, e debuggerEvent) {
	d := getDebuggerRef(c)
	stack := debug.Stack()
	endlines := 0
	skipPrologue := 1
	skipEntry := 2
	entryLen := 2
	skipAfterIdx := bytes.IndexFunc(stack, func(r rune) bool {
		if r == '\n' {
			endlines++
		}
		return endlines == skipPrologue+(skipEntry*entryLen)
	})
	d.append(e, ref, stack[skipAfterIdx+1:])
}
