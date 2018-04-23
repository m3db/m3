package parser

import (
	"context"
	"fmt"
)

// Parser consists of the language specific representation of AST and can convert into a common DAG
type Parser interface {
	DAG() (Nodes, Edges, error)
	String() string
}

// NodeID uniquely identifies all transforms in DAG
type NodeID string

// Params is a function definition. It is immutable and contains no state
type Params interface {
	fmt.Stringer
	OpType() string
}

// OpNode represents the execution node
type OpNode interface {
}

// Nodes is a slice of Node
type Nodes []Node

// Node represents an immutable node in the common DAG with a unique identifier.
// TODO: make this serializable
type Node struct {
	ID NodeID
	Op Params
}

func (t Node) String() string {
	return fmt.Sprintf("ID: %s, Op: %s", t.ID, t.Op)
}

// Edge identifies parent-child relation between transforms
type Edge struct {
	ParentID NodeID
	ChildID  NodeID
}

func (e Edge) String() string {
	return fmt.Sprintf("parent: %s, child: %s", e.ParentID, e.ChildID)
}

// Edges is a slice of Edge
type Edges []Edge

// NewTransformFromOperation creates a new transform
func NewTransformFromOperation(Op Params, nextID int) Node {
	return Node{
		Op: Op,
		ID: NodeID(fmt.Sprintf("%v", nextID)),
	}
}

// Source represents data sources which are handled differently than other transforms as they are always independent and can always be parallelized
type Source interface {
	Execute(ctx context.Context) error
}
