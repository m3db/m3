package builder

import (
	"bytes"
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index/segment"

	"github.com/stretchr/testify/require"
)

func TestFieldPostingsListIterFromSegments(t *testing.T) {
	segments := []segment.Segment{
		newTestSegmentWithDocs(t, []doc.Document{
			{
				ID: []byte("bux_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val0")},
				},
			},
			{
				ID: []byte("bar_0"),
				Fields: []doc.Field{
					{Name: []byte("cat"), Value: []byte("rhymes")},
					{Name: []byte("hat"), Value: []byte("with")},
					{Name: []byte("bat"), Value: []byte("pat")},
				},
			},
		}),
		newTestSegmentWithDocs(t, []doc.Document{
			{
				ID: []byte("foo_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val0")},
				},
			},
			{
				ID: []byte("bux_1"),
				Fields: []doc.Field{
					{Name: []byte("delta"), Value: []byte("22")},
					{Name: []byte("gamma"), Value: []byte("33")},
					{Name: []byte("theta"), Value: []byte("44")},
				},
			},
		}),
		newTestSegmentWithDocs(t, []doc.Document{
			{
				ID: []byte("bar_1"),
				Fields: []doc.Field{
					{Name: []byte("cat"), Value: []byte("rhymes")},
					{Name: []byte("hat"), Value: []byte("with")},
					{Name: []byte("bat"), Value: []byte("pat")},
				},
			},
			{
				ID: []byte("foo_1"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("apple")},
					{Name: []byte("vegetable"), Value: []byte("carrot")},
					{Name: []byte("infrequent"), Value: []byte("val1")},
				},
			},
			{
				ID: []byte("baz_0"),
				Fields: []doc.Field{
					{Name: []byte("fruit"), Value: []byte("watermelon")},
					{Name: []byte("color"), Value: []byte("green")},
					{Name: []byte("alpha"), Value: []byte("0.5")},
				},
			},
			{
				ID: []byte("bux_2"),
				Fields: []doc.Field{
					{Name: []byte("delta"), Value: []byte("22")},
					{Name: []byte("gamma"), Value: []byte("33")},
					{Name: []byte("theta"), Value: []byte("44")},
				},
			},
		}),
	}
	builder := NewBuilderFromSegments(testOptions)
	builder.Reset(0)

	b, ok := builder.(*builderFromSegments)
	require.True(t, ok)
	require.NoError(t, builder.AddSegments(segments))
	iter, err := b.FieldsPostingsList()
	require.NoError(t, err)
	for iter.Next() {
		field, pl := iter.Current()
		plIter := pl.Iterator()
		for plIter.Next() {
			pID := plIter.Current()
			doc, err := b.Doc(pID)
			require.NoError(t, err)
			found := checkValidPostingsListsForField(field, doc)
			require.True(t, found)
		}
	}
}

func checkValidPostingsListsForField(
	field []byte,
	doc doc.Document,
) bool {
	found := false
	for _, f := range doc.Fields {
		if bytes.Equal(field, f.Name) {
			found = true
		}
	}
	return found
}
