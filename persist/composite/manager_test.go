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

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func testPersistenceManager(
	ctrl *gomock.Controller,
) (*persistenceManager, *mocks.MockPersistenceManager, *mocks.MockPersistenceManager) {
	m1 := mocks.NewMockPersistenceManager(ctrl)
	m2 := mocks.NewMockPersistenceManager(ctrl)
	pm := NewPersistenceManager(true, m1, m2)
	return pm.(*persistenceManager), m1, m2
}

func TestPersistenceManagerPrepare(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	pm, m1, m2 := testPersistenceManager(ctrl)
	shard := uint32(0)
	blockStart := time.Unix(1000, 0)
	var (
		persisted bool
		closed    bool
	)
	persistFn := func(id string, segment m3db.Segment) error {
		persisted = true
		return nil
	}
	closer := func() {
		closed = true
	}
	expectedErr := errors.New("foo")
	prepared := m3db.PreparedPersistence{Persist: persistFn, Close: closer}
	m1.EXPECT().Prepare(shard, blockStart).Return(prepared, nil)
	m2.EXPECT().Prepare(shard, blockStart).Return(m3db.PreparedPersistence{}, expectedErr)

	res, err := pm.Prepare(shard, blockStart)
	require.NotNil(t, res.Persist)
	require.NotNil(t, res.Close)
	require.NotNil(t, err)
	require.Equal(t, "foo", err.Error())

	id := "bar"
	segment := m3db.Segment{Head: []byte{0x1}, Tail: []byte{0x2}}
	defer func() {
		res.Close()
		require.True(t, closed)
	}()
	require.Nil(t, res.Persist(id, segment))
	require.True(t, persisted)
}
