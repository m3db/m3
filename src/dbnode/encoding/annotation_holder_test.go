package encoding

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/m3db/m3/src/dbnode/ts"
)

var annotationHolderTests = []struct {
	name       string
	annotation ts.Annotation
}{
	{
		name: "nil",
	},
	{
		name:       "empty",
		annotation: []byte{},
	},
	{
		name:       "short",
		annotation: []byte{1, 2, 3},
	},
	{
		name:       "OptimizedAnnotationLen - 1",
		annotation: repeat(1, ts.OptimizedAnnotationLen-1),
	},
	{
		name:       "OptimizedAnnotationLen",
		annotation: repeat(2, ts.OptimizedAnnotationLen),
	},
	{
		name:       "OptimizedAnnotationLen + 1",
		annotation: repeat(3, ts.OptimizedAnnotationLen+1),
	},
	{
		name:       "OptimizedAnnotationLen * 2",
		annotation: repeat(4, ts.OptimizedAnnotationLen*2),
	},
}

func TestAnnotationHolder(t *testing.T) {
	holder := annotationHolder{}
	assert.Nil(t, holder.get())

	for _, tt := range annotationHolderTests {
		t.Run(tt.name, func(t *testing.T) {
			holder.set(tt.annotation)
			if len(tt.annotation) == 0 {
				assert.Empty(t, holder.get())
			} else {
				assert.Equal(t, tt.annotation, holder.get())
			}
		})
	}
}

func TestAnnotationHolderWithReset(t *testing.T) {
	holder := annotationHolder{}
	holder.reset()
	assert.Nil(t, holder.get())

	for _, tt := range annotationHolderTests {
		t.Run(tt.name, func(t *testing.T) {
			holder.set(tt.annotation)
			if len(tt.annotation) == 0 {
				assert.Empty(t, holder.get())
			} else {
				assert.Equal(t, tt.annotation, holder.get())
			}
			holder.reset()
			assert.Nil(t, holder.get())
		})
	}
}

func repeat(b byte, n int) ts.Annotation {
	a := make(ts.Annotation, 0, n)
	for i := 0; i < n; i++ {
		a = append(a, b)
	}
	return a
}
