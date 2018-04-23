package plan

import (
	"testing"

	"github.com/m3db/m3coordinator/functions"
	"github.com/m3db/m3coordinator/parser"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestSingleChildParentRelation(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	countTransform := parser.NewTransformFromOperation(functions.CountOp{}, 2)
	transforms := parser.Nodes{fetchTransform, countTransform}
	edges := parser.Edges{
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

func TestSingleParentMultiChild(t *testing.T) {
	fetchTransform := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	countTransform1 := parser.NewTransformFromOperation(functions.CountOp{}, 2)
	countTransform2 := parser.NewTransformFromOperation(functions.CountOp{}, 3)
	transforms := parser.Nodes{fetchTransform, countTransform1, countTransform2}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform1.ID,
		},
		parser.Edge{
			ParentID: fetchTransform.ID,
			ChildID:  countTransform2.ID,
		},
	}

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Steps[countTransform1.ID].Parents, 1)
	assert.Len(t, lp.Steps[fetchTransform.ID].Children, 2)
	assert.Len(t, lp.Steps[fetchTransform.ID].Parents, 0)
	assert.Len(t, lp.Steps[countTransform2.ID].Parents, 1)
	assert.Equal(t, lp.Steps[countTransform1.ID].Parents[0], lp.Steps[countTransform2.ID].Parents[0])
}

func TestMultiParent(t *testing.T) {
	fetchTransform1 := parser.NewTransformFromOperation(functions.FetchOp{}, 1)
	fetchTransform2 := parser.NewTransformFromOperation(functions.FetchOp{}, 2)
	// TODO: change this to a real multi parent operation such as asPercent
	countTransform := parser.NewTransformFromOperation(functions.CountOp{}, 3)

	transforms := parser.Nodes{fetchTransform1, fetchTransform2, countTransform}
	edges := parser.Edges{
		parser.Edge{
			ParentID: fetchTransform1.ID,
			ChildID:  countTransform.ID,
		},
		parser.Edge{
			ParentID: fetchTransform2.ID,
			ChildID:  countTransform.ID,
		},
	}

	lp, err := NewLogicalPlan(transforms, edges)
	require.NoError(t, err)
	assert.Len(t, lp.Steps[countTransform.ID].Parents, 2)
	assert.Len(t, lp.Steps[fetchTransform1.ID].Children, 1)
	assert.Len(t, lp.Steps[fetchTransform2.ID].Children, 1)
}
