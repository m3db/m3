// Copyright (c) 2020 Uber Technologies, Inc.
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

package tile

import (
	"bytes"
	"fmt"

	"github.com/m3db/m3/src/dbnode/ts"
)

type annotationRecorder struct {
	count int
	a     ts.Annotation
	as    []ts.Annotation
}

var _ SeriesFrameAnnotations = (*annotationRecorder)(nil)

func newAnnotationRecorder() *annotationRecorder {
	return &annotationRecorder{}
}

func (a *annotationRecorder) Value(idx int) (ts.Annotation, error) {
	if idx < 0 || idx >= a.count {
		return nil, fmt.Errorf("annotationRecorder.Value index (%d) out of bounds [0; %d)", idx, a.count)
	}

	if a.singleValue() {
		return a.a, nil
	}

	return a.as[idx], nil
}

func (a *annotationRecorder) singleValue() bool {
	return a.count > 0 && len(a.as) == 0
}

func (a *annotationRecorder) record(annotation ts.Annotation) {
	a.count++
	if a.count == 1 {
		a.a = annotation
		return
	}

	// NB: annotation has already changed in this dataset.
	if len(a.as) > 0 {
		a.as = append(a.as, annotation)
		return
	}

	// NB: same annotation as previously recorded; skip.
	if bytes.Equal(a.a, annotation) {
		return
	}

	if a.as == nil {
		a.as = make([]ts.Annotation, 0, a.count)
	}

	for i := 0; i < a.count-1; i++ {
		a.as = append(a.as, a.a)
	}

	a.as = append(a.as, annotation)
}

func (a *annotationRecorder) reset() {
	a.count = 0
	for i := range a.as {
		a.as[i] = nil
	}

	a.as = a.as[:0]
}
