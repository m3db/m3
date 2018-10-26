// Copyright (c) 2018 Uber Technologies, Inc.
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

package parser

import (
	"context"
	"fmt"

	"github.com/m3db/m3/src/query/models"
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
	Execute(ctx context.Context, queryCtx *models.QueryContext) error
}
