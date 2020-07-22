package tile

import (
	"testing"

	"github.com/m3db/m3/src/dbnode/ts"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func a(s string) ts.Annotation { return ts.Annotation(s) }

func annotationEqual(t *testing.T, expected string, actual ts.Annotation) {
	assert.Equal(t, expected, string(actual))
}

func annotationsEqual(t *testing.T, expected []string, actual []ts.Annotation) {
	require.Equal(t, len(expected), len(actual))
	for i, ex := range expected {
		annotationEqual(t, ex, actual[i])
	}
}

func TestSeriesFrameAnnotationsSingle(t *testing.T) {
	rec := newAnnotationRecorder()

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	rec.record(a("foo"))
	v, ok := rec.SingleValue()
	assert.True(t, ok)
	annotationEqual(t, "foo", v)

	rec.record(a("foo"))
	v, ok = rec.SingleValue()
	assert.True(t, ok)
	annotationEqual(t, "foo", v)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "foo"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)
}

func TestSeriesFrameAnnotationsMultiple(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("foo"))
	rec.record(a("bar"))

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "foo", "bar"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}

func TestSeriesFrameAnnotationsMultipleChanges(t *testing.T) {
	rec := newAnnotationRecorder()
	rec.record(a("foo"))
	rec.record(a("bar"))
	rec.record(a("baz"))

	_, ok := rec.SingleValue()
	assert.False(t, ok)

	vals := rec.Values()
	annotationsEqual(t, []string{"foo", "bar", "baz"}, vals)

	rec.reset()
	_, ok = rec.SingleValue()
	assert.False(t, ok)

	vals = rec.Values()
	require.Equal(t, 0, len(vals))
}
