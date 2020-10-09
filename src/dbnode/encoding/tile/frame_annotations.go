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

func (r *annotationRecorder) Value(idx int) (ts.Annotation, error) {
	if idx < 0 || idx >= r.count {
		return nil, fmt.Errorf("annotationRecorder.Value index (%d) out of bounds [0; %d)", idx, r.count)
	}

	if r.singleValue() {
		return r.a, nil
	}

	return r.as[idx], nil
}

func (r *annotationRecorder) singleValue() bool {
	return r.count > 0 && len(r.as) == 0
}

func (r *annotationRecorder) record(annotation ts.Annotation) {
	r.count++
	if r.count == 1 {
		r.a = annotation
		return
	}

	// NB: annotation has already changed in this dataset.
	if len(r.as) > 0 {
		r.as = append(r.as, annotation)
		return
	}

	// NB: same annotation as previously recorded; skip.
	if bytes.Equal(r.a, annotation) {
		return
	}

	if r.as == nil {
		r.as = make([]ts.Annotation, 0, r.count)
	}

	for i := 0; i < r.count-1; i++ {
		r.as = append(r.as, r.a)
	}

	r.as = append(r.as, annotation)
}

func (r *annotationRecorder) reset() {
	r.count = 0
	for i := range r.as {
		r.as[i] = nil
	}

	r.as = r.as[:0]
}
