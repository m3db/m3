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
	} else {
		return a.allocatedAnnotation
	}
}

func (a *annotationHolder) set(annotation ts.Annotation) {
	l := len(annotation)

	if l <= len(a.inlineAnnotationBytes) {
		copy(a.inlineAnnotationBytes[:l], annotation)
		a.inlineAnnotationLen = l
		return
	}

	a.allocatedAnnotation = append(a.allocatedAnnotation[:0], annotation...)
	a.inlineAnnotationLen = 0
}

func (a *annotationHolder) reset() {
	a.inlineAnnotationLen = 0
	a.allocatedAnnotation = nil
}
