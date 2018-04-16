package plan

import (
	"testing"

	"github.com/m3db/m3coordinator/functions"
	"github.com/m3db/m3coordinator/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleChildParentRelation(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(&functions.FetchOp{}, 1)
	countTransform := parser.NewTransformFromOperation(&functions.CountOp{}, 2)
	transforms := parser.Transforms{fetchTransform, countTransform}
	edges := parser.Edges {
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform.ID,
		},
	}

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Steps[countTransform.ID].Parents, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Children, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Parents, 0)
	assert.Len(t, lp.Steps[countTransform.ID].Children, 0)
	assert.Equal(t, lp.Steps[fetchTransform.ID].Children[0], countTransform.ID)
	assert.Equal(t, lp.Steps[countTransform.ID].Parents[0], fetchTransform.ID)
	assert.Equal(t, lp.Steps[countTransform.ID].ID(), countTransform.ID)
	// Will get better once we implement ops. Then we can test for existence of ops
	assert.Contains(t, lp.String(), "Parents")
}
