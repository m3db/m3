// Copyright (c) 2016 Uber Technologies, Inc.
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

package commitlog

import (
	"github.com/m3db/m3db/ts"
	"github.com/m3db/m3x/checked"
)

// NewSeries creates a new commit log series descriptor.
func NewSeries(
	uniqueIndex uint64,
	namespace, id ts.ID,
	shard uint32,
	flags SeriesFlags,
) Series {
	return Series{
		UniqueIndex: uniqueIndex,
		Namespace:   namespace,
		ID:          id,
		Shard:       shard,
		Flags:       flags,
	}
}

// Clone will clone the series descriptor, if the ID pool passed is nil it will
// create new IDs otherwise it will use the pool to create new IDs.
func (s *Series) Clone(idPool ts.IdentifierPool) Series {
	var namespace, id ts.ID
	if idPool != nil {
		namespace = idPool.Clone(s.Namespace)
		id = idPool.Clone(s.ID)
	} else {
		namespace = ts.BinaryID(checked.NewBytes(nil, nil))
		namespace.Data().AppendAll(s.Namespace.Data().Get())
		id = ts.BinaryID(checked.NewBytes(nil, nil))
		id.Data().AppendAll(s.ID.Data().Get())
	}
	return NewSeries(
		s.UniqueIndex,
		namespace,
		id,
		s.Shard,
		s.Flags,
	)
}

// Finalize will finalize the IDs on the series descriptor.
func (s *Series) Finalize() {
	if s.Namespace != nil &&
		s.Flags&FinalizeNamespace == FinalizeNamespace {
		s.Namespace.Finalize()
	}
	s.Namespace = nil
	if s.ID != nil &&
		s.Flags&FinalizeID == FinalizeID {
		s.ID.Finalize()
	}
	s.ID = nil
}
