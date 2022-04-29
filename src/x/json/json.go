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

// Package json contains logic for writing JSON.
package json

import (
	"bufio"
	"bytes"
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

type (
	writeState    int
	containerType int
)

const (
	object containerType = iota
	array
)

const (
	writeStart writeState = iota
	writeBeforeFirstField
	writeBeforeNthField
	writeBeforeFieldValue
	writeBeforeFirstArrayElement
	writeBeforeNthArrayElement
	writeEnd
)

var writeValueAllowed = map[writeState]struct{}{
	writeStart:                   {},
	writeBeforeFieldValue:        {},
	writeBeforeFirstArrayElement: {},
	writeBeforeNthArrayElement:   {},
}

func (s writeState) isValueAllowed() bool {
	_, allowed := writeValueAllowed[s]
	return allowed
}

// A Writer can be used to directly stream JSON results without going through
// an intermediate object layer.
type Writer interface {
	BeginObject()
	BeginObjectField(name string)
	BeginObjectBytesField(name []byte)
	EndObject()
	BeginArray()
	EndArray()
	WriteBool(b bool)
	WriteNull()
	WriteFloat64(n float64)
	WriteInt(n int)
	WriteString(s string)
	WriteBytesString(s []byte)
	Flush() error
	Close() error
}

type writer struct {
	w          *bufio.Writer
	buff       *bytes.Reader
	state      writeState
	containers []containerType
	err        error
}

// NewWriter creates a new JSON token writer
func NewWriter(w io.Writer) Writer {
	return &writer{
		w:          bufio.NewWriter(w),
		buff:       bytes.NewReader(nil),
		containers: make([]containerType, 0, 5),
	}
}

// BeginObject begins a new object
func (w *writer) BeginObject() {
	if !w.beginValue() {
		return
	}

	w.containers = append(w.containers, object)
	w.err = w.w.WriteByte('{')
	w.state = writeBeforeFirstField
}

// BeginObjectField begins a new object field with the given name
func (w *writer) BeginObjectField(name string) {
	w.err = w.beginObjectFieldStart()
	if w.err != nil {
		return
	}

	w.writeString(name)
	if w.err != nil {
		return
	}

	w.err = w.beginObjectFieldEnd()
}

func (w *writer) beginObjectFieldStart() error {
	if w.err != nil {
		return w.err
	}

	if w.state != writeBeforeFirstField && w.state != writeBeforeNthField {
		return errFieldNotAllowed
	}

	if w.state == writeBeforeNthField {
		if err := w.w.WriteByte(','); err != nil {
			return err
		}
	}

	return nil
}

func (w *writer) beginObjectFieldEnd() error {
	if err := w.w.WriteByte(':'); err != nil {
		return err
	}
	w.state = writeBeforeFieldValue
	return nil
}

func (w *writer) BeginObjectBytesField(name []byte) {
	w.err = w.beginObjectFieldStart()
	if w.err != nil {
		return
	}

	w.writeBytesString(name)
	if w.err != nil {
		return
	}

	w.err = w.beginObjectFieldEnd()
}

// EndObject finishes an open object
func (w *writer) EndObject() {
	if !w.endContainer(object) {
		return
	}

	w.err = w.w.WriteByte('}')
}

// BeginArray begins a new array value
func (w *writer) BeginArray() {
	if !w.beginValue() {
		return
	}

	w.containers = append(w.containers, array)
	w.err = w.w.WriteByte('[')
	w.state = writeBeforeFirstArrayElement
}

// EndArray finishes an array value
func (w *writer) EndArray() {
	if !w.endContainer(array) {
		return
	}

	w.err = w.w.WriteByte(']')
}

// endContainer finishes a container of the given type
func (w *writer) endContainer(expected containerType) bool {
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
func (w *writer) WriteBool(b bool) {
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

func (w *writer) writeNull() {
	_, w.err = w.w.WriteString("null")
}

// WriteNull writes a null value
func (w *writer) WriteNull() {
	if !w.beginValue() {
		return
	}

	w.writeNull()
	w.endValue()
}

// WriteFloat64 writes a float value
func (w *writer) WriteFloat64(n float64) {
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
func (w *writer) WriteInt(n int) {
	if !w.beginValue() {
		return
	}

	_, w.err = w.w.WriteString(strconv.Itoa(n))
	w.endValue()
}

// WriteString writes a string value
func (w *writer) WriteString(s string) {
	if !w.beginValue() {
		return
	}

	w.writeString(s)

	w.endValue()
}

func (w *writer) writeString(s string) {
	if w.err = w.w.WriteByte('"'); w.err != nil {
		return
	}

	for _, c := range s {
		w.writeRune(c)
		if w.err != nil {
			return
		}
	}

	w.err = w.w.WriteByte('"')
}

func (w *writer) WriteBytesString(s []byte) {
	if !w.beginValue() {
		return
	}

	w.writeBytesString(s)

	w.endValue()
}

func (w *writer) writeBytesString(s []byte) {
	if w.err = w.w.WriteByte('"'); w.err != nil {
		return
	}

	w.buff.Reset(s)
	defer w.buff.Reset(nil) // Free holding onto byte slice.

	for {
		c, _, err := w.buff.ReadRune()
		if errors.Is(err, io.EOF) {
			break
		}
		if err != nil {
			w.err = err
			return
		}
		w.writeRune(c)
	}

	w.err = w.w.WriteByte('"')
}

func (w *writer) writeRune(r rune) {
	if r <= 31 || r == '"' || r == '\\' {
		if w.err = w.w.WriteByte('\\'); w.err != nil {
			return
		}

		switch r {
		case '"':
			if w.err = w.w.WriteByte('"'); w.err != nil {
				return
			}
		case '\\':
			if w.err = w.w.WriteByte('\\'); w.err != nil {
				return
			}
		case '\n':
			if w.err = w.w.WriteByte('n'); w.err != nil {
				return
			}
		case '\r':
			if w.err = w.w.WriteByte('r'); w.err != nil {
				return
			}
		case '\t':
			if w.err = w.w.WriteByte('t'); w.err != nil {
				return
			}
		default:
			codePoint := fmt.Sprintf("%U", r)
			if w.err = w.w.WriteByte('u'); w.err != nil {
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
func (w *writer) beginValue() bool {
	if w.err != nil {
		return false
	}

	if !w.state.isValueAllowed() {
		w.err = errValueNotAllowed
		return false
	}

	if w.state == writeBeforeNthArrayElement {
		if w.err = w.w.WriteByte(','); w.err != nil {
			return false
		}
	}

	return true
}

// endValue marks as value as being complete
func (w *writer) endValue() {
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
func (w *writer) Flush() error {
	if w.err != nil {
		return w.err
	}

	w.err = w.w.Flush()
	return w.err
}

// Close closes the writer, returning any write errors that have occurred
func (w *writer) Close() error {
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
