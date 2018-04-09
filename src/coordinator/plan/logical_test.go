package plan

import (
	"testing"

	"github.com/m3db/m3coordinator/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleChildParentRelation(t *testing.T) {
	fetchTransform := &parser.Transform{
		ID: parser.TransformID("1"),
	}

	sumTransform := &parser.Transform{
		ID: parser.TransformID("2"),
	}

	transforms := parser.Transforms{fetchTransform, sumTransform}
	edges := parser.Edges {
		&parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID: sumTransform.ID,
		},
	}

	lp, err := GenerateLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Transforms[sumTransform.ID].Parents, 1)
	assert.Len(t, lp.Transforms[fetchTransform.ID].Children, 1)
	assert.Len(t, lp.Transforms[fetchTransform.ID].Parents, 0)
	assert.Len(t, lp.Transforms[sumTransform.ID].Children, 0)
	assert.Equal(t, lp.Transforms[fetchTransform.ID].Children[0], sumTransform.ID)
	assert.Equal(t, lp.Transforms[sumTransform.ID].Parents[0], fetchTransform.ID)
	// Will get better once we implement ops. Then we can test for existence of ops
	assert.Contains(t, lp.String(), "Parents")
}
