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

package executor

import (
	"testing"

	"github.com/m3db/m3/src/m3ninx/doc"
	"github.com/m3db/m3/src/m3ninx/index"
	"github.com/m3db/m3/src/m3ninx/search"
	"github.com/m3db/m3/src/x/context"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

type testIterator struct{}

func newTestIterator() testIterator { return testIterator{} }

func (it testIterator) Done() bool            { return true }
func (it testIterator) Next() bool            { return false }
func (it testIterator) Current() doc.Document { return doc.Document{} }
func (it testIterator) Err() error            { return nil }
func (it testIterator) Close() error          { return nil }

func TestExecutor(t *testing.T) {
	mockCtrl := gomock.NewController(t)
	defer mockCtrl.Finish()

	var (
		q  = search.NewMockQuery(mockCtrl)
		r  = index.NewMockReader(mockCtrl)
		rs = index.Readers{r}
	)
	r.EXPECT().Close().Return(nil)

	e := NewExecutor(rs).(*executor)

	// Override newIteratorFn to return test iterator.
	e.newIteratorFn = func(_ context.Context, _ search.Query, _ index.Readers) (doc.QueryDocIterator, error) {
		return newTestIterator(), nil
	}

	it, err := e.Execute(context.NewBackground(), q)
	require.NoError(t, err)

	err = it.Close()
	require.NoError(t, err)

	err = e.Close()
	require.NoError(t, err)
}
