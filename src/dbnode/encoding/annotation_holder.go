// Copyright (c) 2021 Uber Technologies, Inc.
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

import "github.com/m3db/m3/src/dbnode/ts"

// annotationHolder holds annotations up to ts.OptimizedAnnotationLen bytes long within
// inlined backing array, and only allocates a new byte slice for longer annotations.
type annotationHolder struct {
	inlineAnnotationBytes [ts.OptimizedAnnotationLen]byte
	inlineAnnotationLen   int
	allocatedAnnotation   ts.Annotation
}

func (a *annotationHolder) get() ts.Annotation {
	if a.inlineAnnotationLen > 0 {
		return a.inlineAnnotationBytes[:a.inlineAnnotationLen]
	}

	return a.allocatedAnnotation
}

func (a *annotationHolder) set(annotation ts.Annotation) {
	if l := len(annotation); l <= len(a.inlineAnnotationBytes) {
		copy(a.inlineAnnotationBytes[:l], annotation)
		a.inlineAnnotationLen = l
		a.allocatedAnnotation = nil
		return
	}

	a.allocatedAnnotation = append(a.allocatedAnnotation[:0], annotation...)
	a.inlineAnnotationLen = 0
}

func (a *annotationHolder) reset() {
	a.inlineAnnotationLen = 0
	a.allocatedAnnotation = nil
}
