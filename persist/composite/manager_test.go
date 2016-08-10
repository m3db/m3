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

package composite

import (
	"errors"
	"testing"
	"time"

	"github.com/m3db/m3db/persist"
	"github.com/m3db/m3db/ts"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testPersistManager(
	ctrl *gomock.Controller,
) (*persistManager, *persist.MockManager, *persist.MockManager) {
	m1 := persist.NewMockManager(ctrl)
	m2 := persist.NewMockManager(ctrl)
	pm := NewPersistManager(m1, m2)
	return pm.(*persistManager), m1, m2
}

func TestPersistManagerPrepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, m1, m2 := testPersistManager(ctrl)
	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	var (
		persisted bool
		closed    bool
	)
	persistFn := func(id string, segment ts.Segment) error {
		persisted = true
		return nil
	}
	closer := func() {
		closed = true
	}
	expectedErr := errors.New("foo")
	prepared := persist.PreparedPersist{Persist: persistFn, Close: closer}
	m1.EXPECT().Prepare(shard, blockStart).Return(prepared, nil)
	m2.EXPECT().Prepare(shard, blockStart).Return(persist.PreparedPersist{}, expectedErr)

	res, err := pm.Prepare(shard, blockStart)
	require.NotNil(t, res.Persist)
	require.NotNil(t, res.Close)
	require.NotNil(t, err)
	require.Equal(t, "foo", err.Error())

	id := "bar"
	segment := ts.Segment{Head: []byte{0x1}, Tail: []byte{0x2}}
	defer func() {
		res.Close()
		require.True(t, closed)
	}()
	require.Nil(t, res.Persist(id, segment))
	require.True(t, persisted)
}
