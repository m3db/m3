package tile

import (
	"bytes"

	"github.com/m3db/m3/src/dbnode/ts"
)

type annotationRecorder struct {
	set   bool
	count int
	a     ts.Annotation
	as    []ts.Annotation
}

var _ SeriesFrameAnnotations = (*annotationRecorder)(nil)

func newAnnotationRecorder() *annotationRecorder {
	return &annotationRecorder{}
}

func (a *annotationRecorder) SingleValue() (ts.Annotation, bool) {
	return a.a, a.set && len(a.as) == 0
}

func (a *annotationRecorder) Values() []ts.Annotation {
	if len(a.as) == 0 {
		if a.as == nil {
			a.as = make([]ts.Annotation, 0, a.count)
		}

		for i := 0; i < a.count; i++ {
			a.as = append(a.as, a.a)
		}
	}

	return a.as
}

func (a *annotationRecorder) record(annotation ts.Annotation) {
	a.count++
	if !a.set {
		a.set = true
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
	a.set = false
	a.as = a.as[:0]
}
