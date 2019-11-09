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

package storage

import (
	"errors"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/m3db/m3/src/x/ident"
	"github.com/stretchr/testify/require"
)

func TestCheckoutSeries(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	ns := NewMockdatabaseNamespace(ctrl)
	acc := NewDatabaseNamespaceDataAccumulator(ns)
	id := ident.StringID("foo")
	var tagIter ident.TagIterator
	acc.CheckoutSeriesWithLock(id, tagIter)

	ns.EXPECT().SeriesReadWriteRef(id, tagIter).Return(nil, errors.New("err"))
	_, err := acc.CheckoutSeriesWithLock(id, tagIter)
	require.Error(t, err)
}
