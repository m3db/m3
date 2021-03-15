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

package json

type noopWriter struct{}

// NewNoopWriter creates a JSON writer that does nothing.
func NewNoopWriter() Writer {
	return &noopWriter{}
}

// BeginObject begins a new object
func (w *noopWriter) BeginObject() {
}

// BeginObjectField begins a new object field with the given name
func (w *noopWriter) BeginObjectField(name string) {
}

// BeginObjectBytesField begins a new object field with the given bytes name
func (w *noopWriter) BeginObjectBytesField(name []byte) {
}

// EndObject finishes an open object
func (w *noopWriter) EndObject() {
}

// BeginArray begins a new array value
func (w *noopWriter) BeginArray() {
}

// EndArray finishes an array value
func (w *noopWriter) EndArray() {
}

// WriteBool writes a boolean value
func (w *noopWriter) WriteBool(b bool) {
}

// WriteNull writes a null value
func (w *noopWriter) WriteNull() {
}

// WriteFloat64 writes a float value
func (w *noopWriter) WriteFloat64(n float64) {
}

// WriteInt writes an int value
func (w *noopWriter) WriteInt(n int) {
}

// WriteString writes a string value
func (w *noopWriter) WriteString(s string) {
}

// WriteBytesString writes a bytes string value
func (w *noopWriter) WriteBytesString(s []byte) {
}

// Flush flushes the writer
func (w *noopWriter) Flush() error {
	return nil
}

// Close closes the writer, returning any write errors that have occurred
func (w *noopWriter) Close() error {
	return nil
}
