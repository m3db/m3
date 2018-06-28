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

package encoding

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
)

var (
	errContainerMismatch  = errors.New("container mismatch")
	errNotInContainer     = errors.New("not in container")
	errFieldNotAllowed    = errors.New("field not allowed")
	errValueNotAllowed    = errors.New("value not allowed")
	errContainerStillOpen = errors.New("container still open")
)

type writeState int
type containerType int

const (
	object containerType = iota
	array
)

const (
	writeStart                   writeState = iota
	writeBeforeFirstField
	writeBeforeNthField
	writeBeforeFieldValue
	writeBeforeFirstArrayElement
	writeBeforeNthArrayElement
	writeEnd
)

var (
	writeValueAllowed = map[writeState]struct{}{
		writeStart:                   struct{}{},
		writeBeforeFieldValue:        struct{}{},
		writeBeforeFirstArrayElement: struct{}{},
		writeBeforeNthArrayElement:   struct{}{},
	}
)

func (s writeState) isValueAllowed() bool {
	_, allowed := writeValueAllowed[s]
	return allowed
}

// A Writer can be used to directly stream JSON results without going through
// an intermediate object layer.
type Writer struct {
	w          *bufio.Writer
	state      writeState
	containers []containerType
	err        error
}

// NewWriter creates a new JSON token writer
func NewWriter(w io.Writer) *Writer {
	return &Writer{
		w:          bufio.NewWriter(w),
		containers: make([]containerType, 0, 5),
	}
}

// BeginObject begins a new object
func (w *Writer) BeginObject() {
	if !w.beginValue() {
		return
	}

	w.containers = append(w.containers, object)
	_, w.err = w.w.WriteRune('{')
	w.state = writeBeforeFirstField
}

// BeginObjectField begins a new object field with the given name
func (w *Writer) BeginObjectField(name string) {
	if w.err != nil {
		return
	}

	if w.state != writeBeforeFirstField && w.state != writeBeforeNthField {
		w.err = errFieldNotAllowed
		return
	}

	if w.state == writeBeforeNthField {
		if _, w.err = w.w.WriteRune(','); w.err != nil {
			return
		}
	}

	w.writeString(name)
	if w.err != nil {
		return
	}

	_, w.err = w.w.WriteRune(':')
	if w.err != nil {
		return
	}

	w.state = writeBeforeFieldValue
}

// EndObject finishes an open object
func (w *Writer) EndObject() {
	if !w.endContainer(object) {
		return
	}

	_, w.err = w.w.WriteRune('}')
}

// BeginArray begins a new array value
func (w *Writer) BeginArray() {
	if !w.beginValue() {
		return
	}

	w.containers = append(w.containers, array)
	_, w.err = w.w.WriteRune('[')
	w.state = writeBeforeFirstArrayElement
}

// EndArray finishes an array value
func (w *Writer) EndArray() {
	if !w.endContainer(array) {
		return
	}

	_, w.err = w.w.WriteRune(']')
}

// endContainer finishes a container of the given type
func (w *Writer) endContainer(expected containerType) bool {
	if w.err != nil {
		return false
	}

	if len(w.containers) == 0 {
		w.err = errNotInContainer
		return false
	}

	container := w.containers[len(w.containers)-1]
	w.containers = w.containers[:len(w.containers)-1]
	if container != expected {
		w.err = errContainerMismatch
		return false
	}

	w.endValue()
	return true
}

// WriteBool writes a boolean value
func (w *Writer) WriteBool(b bool) {
	if !w.beginValue() {
		return
	}

	if b {
		_, w.err = w.w.WriteString("true")
	} else {
		_, w.err = w.w.WriteString("false")
	}
	w.endValue()
}

func (w *Writer) writeNull() {
	_, w.err = w.w.WriteString("null")
}

// WriteNull writes a null value
func (w *Writer) WriteNull() {
	if !w.beginValue() {
		return
	}

	w.writeNull()
	w.endValue()
}

// WriteFloat64 writes a float value
func (w *Writer) WriteFloat64(n float64) {
	if !w.beginValue() {
		return
	}

	// JSON does not support NaNs or infinity
	if math.IsNaN(n) || math.IsInf(n, 0) {
		w.writeNull()
	} else {
		_, w.err = fmt.Fprintf(w.w, "%f", n)
	}

	w.endValue()
}

// WriteInt writes an int value
func (w *Writer) WriteInt(n int) {
	if !w.beginValue() {
		return
	}

	_, w.err = w.w.WriteString(strconv.Itoa(n))
	w.endValue()
}

// WriteString writes a string value
func (w *Writer) WriteString(s string) {
	if !w.beginValue() {
		return
	}

	w.writeString(s)

	w.endValue()
}

func (w *Writer) writeString(s string) {
	if _, w.err = w.w.WriteRune('"'); w.err != nil {
		return
	}

	for _, c := range s {
		w.writeRune(c)
		if w.err != nil {
			return
		}
	}

	_, w.err = w.w.WriteRune('"')
}

func (w *Writer) writeRune(r rune) {
	if r <= 31 || r == '"' || r == '\\' {
		if _, w.err = w.w.WriteRune('\\'); w.err != nil {
			return
		}

		switch r {
		case '"', '\\':
			if _, w.err = w.w.WriteRune(r); w.err != nil {
				return
			}
		case '\n':
			if _, w.err = w.w.WriteRune('n'); w.err != nil {
				return
			}
		case '\r':
			if _, w.err = w.w.WriteRune('r'); w.err != nil {
				return
			}
		case '\t':
			if _, w.err = w.w.WriteRune('t'); w.err != nil {
				return
			}
		default:
			codePoint := fmt.Sprintf("%U", r)
			if _, w.err = w.w.WriteRune('u'); w.err != nil {
				return
			}
			if _, w.err = w.w.WriteString(codePoint[2:]); w.err != nil {
				return
			}
		}

		return
	}

	if _, w.err = w.w.WriteRune(r); w.err != nil {
		return
	}
}

// beginValue begins a new value, confirming that the current position of the
// writer allows a value
func (w *Writer) beginValue() bool {
	if w.err != nil {
		return false
	}

	if !w.state.isValueAllowed() {
		w.err = errValueNotAllowed
		return false
	}

	if w.state == writeBeforeNthArrayElement {
		if _, w.err = w.w.WriteRune(','); w.err != nil {
			return false
		}
	}

	return true
}

// endValue marks as value as being complete
func (w *Writer) endValue() {
	if len(w.containers) == 0 {
		// End of top level object
		w.state = writeEnd
		return
	}

	c := w.containers[len(w.containers)-1]
	switch c {
	case object:
		w.state = writeBeforeNthField
	case array:
		w.state = writeBeforeNthArrayElement
	default:
		panic(fmt.Sprintf("unknown container type %d", c))
	}
}

// Flush flushes the writer
func (w *Writer) Flush() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.w.Flush()
	return w.err
}

// Close closes the writer, returning any write errors that have occurred
func (w *Writer) Close() error {
	if w.err != nil {
		return w.err
	}

	if len(w.containers) > 0 {
		w.err = errContainerStillOpen
		return w.err
	}

	w.err = w.w.Flush()
	return w.err
}
