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

package index

import (
	"errors"

	"github.com/m3db/m3x/ident"
	"github.com/m3db/m3x/pool"
)

var (
	errUnableToAddValueMissingID = errors.New("no id for value")
)

type aggregatedValues struct {
	valuesMap *AggregateValuesMap

	bytesPool pool.CheckedBytesPool

	pool          AggregateValuesPool
	noFinalizeVar bool
}

// NewAggregateValues returns a new AggregateValues object.
func NewAggregateValues(opts Options) AggregateValues {
	return &aggregatedValues{
		valuesMap: newAggregateValuesMap(opts.IdentifierPool()),
		bytesPool: opts.CheckedBytesPool(),
		pool:      opts.AggregateValuesPool(),
	}
}

func (v *aggregatedValues) Map() *AggregateValuesMap {
	return v.valuesMap
}

func (v *aggregatedValues) Size() int {
	return v.valuesMap.Len()
}

func (v *aggregatedValues) reset() {
	// reset all values from map first
	for _, entry := range v.valuesMap.Iter() {
		ident := entry.Key()
		ident.Finalize()
	}

	// reset all keys in the map next
	v.valuesMap.Reset()
}

func (v *aggregatedValues) finalize() {
	if v.noFinalizeVar {
		return
	}

	v.reset()
	if v.pool == nil {
		return
	}

	v.pool.Put(v)
}

func (v *aggregatedValues) noFinalize() {
	// Ensure neither the results object itself, or any of its underlying
	// IDs and tags will be finalized.
	v.noFinalizeVar = true
	for _, entry := range v.valuesMap.Iter() {
		id := entry.Key()
		id.NoFinalize()
	}
}

func (v *aggregatedValues) addValue(value ident.ID) error {
	bytesID := ident.BytesID(value.Bytes())
	if len(bytesID) == 0 {
		return errUnableToAddValueMissingID
	}

	// TODO: check to see if the value should be cloned here prior to insertion.
	v.valuesMap.Set(bytesID, struct{}{})
	return nil
}
