// Copyright (c) 2019 Uber Technologies, Inc.
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

package binary

import (
	"github.com/m3db/m3/src/query/block"
	"github.com/m3db/m3/src/query/parser"
)

// VectorMatchCardinality describes the cardinality relationship
// of two Vectors in a binary operation.
type VectorMatchCardinality int

const (
	// CardOneToOne is used for one-one relationship
	CardOneToOne VectorMatchCardinality = iota
	// CardManyToOne is used for many-one relationship
	CardManyToOne
	// CardOneToMany is used for one-many relationship
	CardOneToMany
	// CardManyToMany is used for many-many relationship
	CardManyToMany
)

// VectorMatching describes how elements from two Vectors in a binary
// operation are supposed to be matched.
type VectorMatching struct {
	// Set determines if this has been set or is using default values.
	Set bool
	// Card is the cardinality of the two Vectors.
	Card VectorMatchCardinality
	// MatchingLabels contains the labels which define equality of a pair of
	// elements from the Vectors.
	MatchingLabels [][]byte
	// On includes the given label names from matching,
	// rather than excluding them.
	On bool
	// Include contains additional labels that should be included in
	// the result from the side with the lower cardinality.
	Include []string
}

// VectorMatcherBuilder creates a vector matcher based on incoming block types.
type VectorMatcherBuilder func(lhs, rhs block.Block) VectorMatching

// NodeParams describes the types of nodes used for binary operations.
type NodeParams struct {
	LNode, RNode         parser.NodeID
	ReturnBool           bool
	VectorMatcherBuilder VectorMatcherBuilder
}
