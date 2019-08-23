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

package block

import (
	"errors"
	"fmt"
	"time"

	"github.com/m3db/m3/src/query/ts"
	xerrors "github.com/m3db/m3/src/x/errors"
)

var (
	errMismatchedStepIter = errors.New(
		"container step iter has mismatched step size")
	errMismatchedUcStepIter = errors.New(
		"unconsolidated container step iter has mismatched step size")
	errNoContainerBlocks = errors.New(
		"no blocks provided to initialize container block")
)

type containerBlock struct {
	err    error
	meta   Metadata
	blocks []Block
}

func newContainerBlock(blocks []Block) (AccumulatorBlock, error) {
	if len(blocks) == 0 {
		return nil, errNoContainerBlocks
	}

	meta := blocks[0].Meta()
	for _, b := range blocks[1:] {
		m := b.Meta()
		if !m.Equals(meta) {
			return nil, fmt.Errorf("mismatched metadata in container block: "+
				"expected %s, got %s", meta.String(), m.String())
		}
	}

	return &containerBlock{
		blocks: blocks,
		meta:   meta,
	}, nil
}

// NewContainerBlock creates a container block, which allows iterating across
// blocks incoming from multiple data sources, provided they have the same
// bounds.
func NewContainerBlock(blocks ...Block) (AccumulatorBlock, error) {
	return newContainerBlock(blocks)
}

func (b *containerBlock) Meta() Metadata {
	return b.meta
}

func (b *containerBlock) AddBlock(bl Block) error {
	if b.err != nil {
		return b.err
	}

	m, blockMeta := b.Meta(), bl.Meta()
	if !m.Equals(blockMeta) {
		return fmt.Errorf("mismatched metadata adding block to container block: "+
			"expected %s, got %s", m.String(), blockMeta.String())
	}

	b.blocks = append(b.blocks, bl)
	return nil
}

func (c *containerBlock) Info() BlockInfo {
	return NewBlockInfo(BlockContainer)
}

func (b *containerBlock) Close() error {
	multiErr := xerrors.NewMultiError()
	multiErr = multiErr.Add(b.err)
	for _, bl := range b.blocks {
		multiErr = multiErr.Add(bl.Close())
	}

	if err := multiErr.FinalError(); err != nil {
		b.err = err
		return err
	}

	return nil
}

func (b *containerBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (Block, error) {
	if b.err != nil {
		return nil, b.err
	}

	updatedBlockList := make([]Block, 0, len(b.blocks))
	for _, bl := range b.blocks {
		updated, err := bl.WithMetadata(meta, sm)
		if err != nil {
			b.err = err
			return nil, err
		}

		updatedBlockList = append(updatedBlockList, updated)
	}

	return newContainerBlock(updatedBlockList)
}

func (b *containerBlock) StepIter() (StepIter, error) {
	if b.err != nil {
		return nil, b.err
	}

	it := &containerStepIter{its: make([]StepIter, 0, len(b.blocks))}
	for _, bl := range b.blocks {
		iter, err := bl.StepIter()
		if err != nil {
			b.err = err
			return nil, err
		}

		it.its = append(it.its, iter)
	}

	return it, nil
}

// NB: step iterators are constructed "sideways"
type containerStepIter struct {
	err error
	its []StepIter
}

func (it *containerStepIter) Close() {
	for _, iter := range it.its {
		iter.Close()
	}
}

func (it *containerStepIter) Err() error {
	if it.err != nil {
		return it.err
	}

	for _, iter := range it.its {
		if it.err = iter.Err(); it.err != nil {
			return it.err
		}
	}

	return nil
}

func (it *containerStepIter) StepCount() int {
	// NB: when using a step iterator, step count doesn't change, but the length
	// of each step does.
	if len(it.its) == 0 {
		return 0
	}

	return it.its[0].StepCount()
}

func (it *containerStepIter) SeriesMeta() []SeriesMeta {
	length := 0
	for _, iter := range it.its {
		length += len(iter.SeriesMeta())
	}

	metas := make([]SeriesMeta, 0, length)
	for _, iter := range it.its {
		metas = append(metas, iter.SeriesMeta()...)
	}

	return metas
}

func (it *containerStepIter) Next() bool {
	if it.err != nil {
		return false
	}

	// advance all the contained iterators; if any have size mismatches, set an
	// error and stop traversal.
	var next bool
	for i, iter := range it.its {
		n := iter.Next()

		if it.err = iter.Err(); it.err != nil {
			return false
		}

		if i == 0 {
			next = n
		} else if next != n {
			it.err = errMismatchedStepIter
			return false
		}
	}

	return next
}

func (it *containerStepIter) Current() Step {
	if len(it.its) == 0 {
		return ColStep{
			time:   time.Time{},
			values: []float64{},
		}
	}

	curr := it.its[0].Current()
	// NB: to get Current for contained step iterators, append results from all
	// contained step iterators in order.
	accumulatorStep := ColStep{
		time:   curr.Time(),
		values: curr.Values(),
	}

	for _, iter := range it.its[1:] {
		curr := iter.Current()
		accumulatorStep.values = append(accumulatorStep.values, curr.Values()...)
	}

	return accumulatorStep
}

func (b *containerBlock) SeriesIter() (SeriesIter, error) {
	if b.err != nil {
		return nil, b.err
	}

	iters := make([]SeriesIter, 0, len(b.blocks))
	for _, bl := range b.blocks {
		iter, err := bl.SeriesIter()
		if err != nil {
			b.err = err
			return nil, err
		}

		iters = append(iters, iter)
	}

	return &containerSeriesIter{its: iters}, nil
}

type containerSeriesIter struct {
	err error
	idx int
	its []SeriesIter
}

func (it *containerSeriesIter) Close() {
	for _, iter := range it.its {
		iter.Close()
	}
}

func (it *containerSeriesIter) Err() error {
	if it.err != nil {
		return it.err
	}

	for _, iter := range it.its {
		if it.err = iter.Err(); it.err != nil {
			return it.err
		}
	}

	return nil
}

func (it *containerSeriesIter) SeriesCount() int {
	count := 0
	for _, iter := range it.its {
		count += iter.SeriesCount()
	}

	return count
}

func (it *containerSeriesIter) SeriesMeta() []SeriesMeta {
	length := 0
	for _, iter := range it.its {
		length += len(iter.SeriesMeta())
	}

	metas := make([]SeriesMeta, 0, length)
	for _, iter := range it.its {
		metas = append(metas, iter.SeriesMeta()...)
	}

	return metas
}

func (it *containerSeriesIter) Next() bool {
	if it.err != nil {
		return false
	}

	for ; it.idx < len(it.its); it.idx++ {
		iter := it.its[it.idx]
		if iter.Next() {
			// the active iterator has been successfuly incremented.
			return true
		}

		// active iterator errored.
		if it.err = iter.Err(); it.err != nil {
			return false
		}
	}

	// all iterators expanded.
	return false
}

func (it *containerSeriesIter) Current() Series {
	return it.its[it.idx].Current()
}

func (b *containerBlock) Unconsolidated() (UnconsolidatedBlock, error) {
	if b.err != nil {
		return nil, b.err
	}

	blocks := make([]UnconsolidatedBlock, 0, len(b.blocks))
	for _, bl := range b.blocks {
		unconsolidated, err := bl.Unconsolidated()
		if err != nil {
			b.err = err
			return nil, err
		}

		blocks = append(blocks, unconsolidated)
	}

	return &ucContainerBlock{
		blocks: blocks,
		meta:   b.meta,
	}, nil
}

type ucContainerBlock struct {
	err    error
	meta   Metadata
	blocks []UnconsolidatedBlock
}

func (b *ucContainerBlock) Close() error {
	multiErr := xerrors.NewMultiError()
	multiErr = multiErr.Add(b.err)
	for _, bl := range b.blocks {
		multiErr = multiErr.Add(bl.Close())
	}

	if err := multiErr.FinalError(); err != nil {
		b.err = err
		return err
	}

	return nil
}

func (b *ucContainerBlock) Meta() Metadata {
	return b.meta
}

func (b *ucContainerBlock) WithMetadata(
	meta Metadata,
	sm []SeriesMeta,
) (UnconsolidatedBlock, error) {
	if b.err != nil {
		return nil, b.err
	}

	updatedBlockList := make([]UnconsolidatedBlock, 0, len(b.blocks))
	for _, bl := range b.blocks {
		updated, err := bl.WithMetadata(meta, sm)
		if err != nil {
			b.err = err
			return nil, err
		}

		updatedBlockList = append(updatedBlockList, updated)
	}

	return &ucContainerBlock{
		blocks: updatedBlockList,
		meta:   meta,
	}, nil
}

func (b *ucContainerBlock) Consolidate() (Block, error) {
	if b.err != nil {
		return nil, b.err
	}

	consolidated := make([]Block, 0, len(b.blocks))
	for _, bl := range b.blocks {
		block, err := bl.Consolidate()
		if err != nil {
			b.err = err
			return nil, err
		}

		consolidated = append(consolidated, block)
	}

	return newContainerBlock(consolidated)
}

func (b *ucContainerBlock) StepIter() (UnconsolidatedStepIter, error) {
	if b.err != nil {
		return nil, b.err
	}

	it := &ucContainerStepIter{
		its: make([]UnconsolidatedStepIter, 0, len(b.blocks)),
	}

	for _, bl := range b.blocks {
		iter, err := bl.StepIter()
		if err != nil {
			b.err = err
			return nil, err
		}

		it.its = append(it.its, iter)
	}

	return it, nil
}

type ucContainerStepIter struct {
	err error
	its []UnconsolidatedStepIter
}

func (it *ucContainerStepIter) Close() {
	for _, iter := range it.its {
		iter.Close()
	}
}

func (it *ucContainerStepIter) Err() error {
	if it.err != nil {
		return it.err
	}

	for _, iter := range it.its {
		if it.err = iter.Err(); it.err != nil {
			return it.err
		}
	}

	return nil
}

func (it *ucContainerStepIter) StepCount() int {
	// NB: when using a step iterator, step count doesn't change, but the length
	// of each step does.
	if len(it.its) == 0 {
		return 0
	}

	return it.its[0].StepCount()
}

func (it *ucContainerStepIter) SeriesMeta() []SeriesMeta {
	length := 0
	for _, iter := range it.its {
		length += len(iter.SeriesMeta())
	}

	metas := make([]SeriesMeta, 0, length)
	for _, iter := range it.its {
		metas = append(metas, iter.SeriesMeta()...)
	}

	return metas
}

func (it *ucContainerStepIter) Next() bool {
	if it.err != nil {
		return false
	}

	// advance all the contained iterators; if any have size mismatches, set an
	// error and stop traversal.
	var next bool
	for i, iter := range it.its {
		n := iter.Next()

		if it.err = iter.Err(); it.err != nil {
			return false
		}

		if i == 0 {
			next = n
		} else if next != n {
			it.err = errMismatchedUcStepIter
			return false
		}
	}

	return next
}

func (it *ucContainerStepIter) Current() UnconsolidatedStep {
	if len(it.its) == 0 {
		return unconsolidatedStep{
			time:   time.Time{},
			values: []ts.Datapoints{},
		}
	}

	curr := it.its[0].Current()
	// NB: to get Current for contained step iterators, append results from all
	// contained step iterators in order.
	accumulatorStep := unconsolidatedStep{
		time:   curr.Time(),
		values: curr.Values(),
	}

	for _, iter := range it.its[1:] {
		curr := iter.Current()
		accumulatorStep.values = append(accumulatorStep.values, curr.Values()...)
	}

	return accumulatorStep
}

func (b *ucContainerBlock) SeriesIter() (UnconsolidatedSeriesIter, error) {
	if b.err != nil {
		return nil, b.err
	}

	it := &ucContainerSeriesIter{
		its: make([]UnconsolidatedSeriesIter, 0, len(b.blocks)),
	}

	for _, bl := range b.blocks {
		iter, err := bl.SeriesIter()
		if err != nil {
			b.err = err
			return nil, err
		}

		it.its = append(it.its, iter)
	}

	return it, nil
}

type ucContainerSeriesIter struct {
	err error
	idx int
	its []UnconsolidatedSeriesIter
}

func (it *ucContainerSeriesIter) Close() {
	for _, iter := range it.its {
		iter.Close()
	}
}

func (it *ucContainerSeriesIter) Err() error {
	if it.err != nil {
		return it.err
	}

	for _, iter := range it.its {
		if it.err = iter.Err(); it.err != nil {
			return it.err
		}
	}

	return nil
}

func (it *ucContainerSeriesIter) SeriesCount() int {
	count := 0
	for _, iter := range it.its {
		count += iter.SeriesCount()
	}

	return count
}

func (it *ucContainerSeriesIter) SeriesMeta() []SeriesMeta {
	length := 0
	for _, iter := range it.its {
		length += len(iter.SeriesMeta())
	}

	metas := make([]SeriesMeta, 0, length)
	for _, iter := range it.its {
		metas = append(metas, iter.SeriesMeta()...)
	}

	return metas
}

func (it *ucContainerSeriesIter) Next() bool {
	if it.err != nil {
		return false
	}

	for ; it.idx < len(it.its); it.idx++ {
		iter := it.its[it.idx]
		if iter.Next() {
			// the active iterator has been successfuly incremented.
			return true
		}

		// active iterator errored.
		if it.err = iter.Err(); it.err != nil {
			return false
		}
	}

	// all iterators expanded.
	return false
}

func (it *ucContainerSeriesIter) Current() UnconsolidatedSeries {
	return it.its[it.idx].Current()
}
