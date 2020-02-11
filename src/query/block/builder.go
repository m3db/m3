// Copyright (c) 2020 Uber Technologies, Inc.
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

package block

import (
	"errors"
	"fmt"

	"github.com/m3db/m3/src/query/cost"
	"github.com/m3db/m3/src/query/models"
	xcost "github.com/m3db/m3/src/x/cost"

	"github.com/uber-go/tally"
)

// ColumnBlockBuilder builds a block optimized for column iteration.
type ColumnBlockBuilder struct {
	block           *columnBlock
	enforcer        cost.ChainedEnforcer
	blockDatapoints tally.Counter
}

// NewColumnBlockBuilder creates a new column block builder.
func NewColumnBlockBuilder(
	queryCtx *models.QueryContext,
	meta Metadata,
	seriesMeta []SeriesMeta) Builder {
	return ColumnBlockBuilder{
		enforcer: queryCtx.Enforcer.Child(cost.BlockLevel),
		blockDatapoints: queryCtx.Scope.Tagged(
			map[string]string{"type": "generated"}).Counter("datapoints"),
		block: &columnBlock{
			meta:       meta,
			seriesMeta: seriesMeta,
			blockType:  BlockDecompressed,
		},
	}
}

// AppendValue adds a value to a column at index.
func (cb ColumnBlockBuilder) AppendValue(idx int, value float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	r := cb.enforcer.Add(1)
	if r.Error != nil {
		return r.Error
	}

	cb.blockDatapoints.Inc(1)

	columns[idx] = append(columns[idx], value)
	return nil
}

// AppendValues adds a slice of values to a column at index.
func (cb ColumnBlockBuilder) AppendValues(idx int, values []float64) error {
	columns := cb.block.columns
	if len(columns) <= idx {
		return fmt.Errorf("idx out of range for append: %d", idx)
	}

	r := cb.enforcer.Add(xcost.Cost(len(values)))
	if r.Error != nil {
		return r.Error
	}

	cb.blockDatapoints.Inc(int64(len(values)))
	columns[idx] = append(columns[idx], values...)
	return nil
}

// AddCols adds the given number of columns to the block.
func (cb ColumnBlockBuilder) AddCols(num int) error {
	if num < 1 {
		return fmt.Errorf("must add more than 0 columns, adding: %d", num)
	}

	newCols := make([]column, num)
	cb.block.columns = append(cb.block.columns, newCols...)
	return nil
}

// PopulateColumns sets all columns to the given row size.
func (cb ColumnBlockBuilder) PopulateColumns(size int) {
	cols := make([]float64, size*len(cb.block.columns))
	for i := range cb.block.columns {
		cb.block.columns[i] = cols[size*i : size*(i+1)]
	}
}

// SetRow sets a given block row to the given values and metadata.
func (cb ColumnBlockBuilder) SetRow(
	idx int,
	values []float64,
	meta SeriesMeta,
) error {
	cols := cb.block.columns
	if len(values) == 0 {
		// Sanity check. Should never happen.
		return errors.New("cannot insert empty values")
	}

	if len(values) != len(cols) {
		return fmt.Errorf("inserting column size %d does not match column size: %d",
			len(values), len(cols))
	}

	rows := len(cols[0])
	if idx < 0 || idx >= rows {
		return fmt.Errorf("cannot insert into row %d, have %d rows", idx, rows)
	}

	for i, v := range values {
		cb.block.columns[i][idx] = v
	}

	r := cb.enforcer.Add(xcost.Cost(len(values)))
	if r.Error != nil {
		return r.Error
	}

	cb.block.seriesMeta[idx] = meta
	return nil
}

// Build builds the block.
func (cb ColumnBlockBuilder) Build() Block {
	return NewAccountedBlock(cb.block, cb.enforcer)
}

// BuildAsType builds the block, forcing it to the given BlockType.
func (cb ColumnBlockBuilder) BuildAsType(blockType BlockType) Block {
	cb.block.blockType = blockType
	return NewAccountedBlock(cb.block, cb.enforcer)
}
