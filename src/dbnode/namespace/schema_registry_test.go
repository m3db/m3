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

package namespace

import (
	"sync"
	"testing"
	"time"

	"github.com/m3db/m3/src/x/ident"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockListener struct {
	sync.RWMutex
	value SchemaDescr
}

func (l *mockListener) SetSchemaHistory(value SchemaHistory) {
	l.Lock()
	defer l.Unlock()
	if s, ok := value.GetLatest(); ok {
		l.value = s
	}
}

func (l *mockListener) Schema() SchemaDescr {
	l.RLock()
	defer l.RUnlock()
	return l.value
}

func TestSchemaRegistryUpdate(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sr := NewSchemaRegistry(true, nil)
	sh1 := NewMockSchemaHistory(ctrl)
	nsID1 := ident.StringID("ns1")
	inputSchema1 := NewMockSchemaDescr(ctrl)
	inputSchema1.EXPECT().DeployId().Return("version1").AnyTimes()
	sh1.EXPECT().GetLatest().Return(inputSchema1, true).AnyTimes()
	sh1.EXPECT().Extends(gomock.Any()).Return(true).AnyTimes()
	require.NoError(t, sr.SetSchemaHistory(nsID1, sh1))

	schema1, err := sr.GetLatestSchema(nsID1)
	require.NoError(t, err)
	require.NotNil(t, schema1)
	require.Equal(t, "version1", schema1.DeployId())

	l := &mockListener{}
	assert.Nil(t, l.value)

	// Ensure immediately sets the value
	closer, err := sr.RegisterListener(nsID1, l)
	require.NoError(t, err)
	require.Equal(t, "version1", l.Schema().DeployId())

	// Update and verify
	sh2 := NewMockSchemaHistory(ctrl)
	inputSchema2 := NewMockSchemaDescr(ctrl)
	inputSchema2.EXPECT().DeployId().Return("version2").AnyTimes()
	sh2.EXPECT().GetLatest().Return(inputSchema2, true).AnyTimes()
	sh2.EXPECT().Extends(gomock.Any()).Return(true)
	require.NoError(t, sr.SetSchemaHistory(nsID1, sh2))

	// Verify listener receives update
	for func() bool {
		return l.Schema().DeployId() != "version2"
	}() {
		time.Sleep(10 * time.Millisecond)
	}
	assert.Equal(t, "version2", l.Schema().DeployId())

	// close the listener
	closer.Close()

	// Update and verify
	sh3 := NewMockSchemaHistory(ctrl)
	inputSchema3 := NewMockSchemaDescr(ctrl)
	inputSchema3.EXPECT().DeployId().Return("version3").AnyTimes()
	sh3.EXPECT().GetLatest().Return(inputSchema3, true).AnyTimes()
	sh3.EXPECT().Extends(gomock.Any()).Return(true)
	require.NoError(t, sr.SetSchemaHistory(nsID1, sh3))

	// Verify closed listener does not receive the update.
	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, "version2", l.Schema().DeployId())

	sr.Close()
}

func TestSchemaRegistryLoad(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sr := NewSchemaRegistry(true, nil)
	nsID := ident.StringID("ns1")
	require.NoError(t, LoadSchemaRegistryFromFile(sr, nsID, "first", "mainpkg/main.proto", "mainpkg.TestMessage", "testdata"))

	schema1, err := sr.GetLatestSchema(nsID)
	require.NoError(t, err)
	require.NotNil(t, schema1)
	require.Equal(t, "first", schema1.DeployId())
}

func TestSchemaRegistryProtoDisabled(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sr := NewSchemaRegistry(false, nil)
	nsID := ident.StringID("ns1")

	schema1, err := sr.GetLatestSchema(nsID)
	require.NoError(t, err)
	require.Nil(t, schema1)

	require.NoError(t, sr.SetSchemaHistory(nsID, nil))
	sl := NewMockSchemaListener(ctrl)
	closer, err := sr.RegisterListener(nsID, sl)
	require.NoError(t, err)
	require.Nil(t, closer)
}

func TestSchemaRegistrySchemaNotSet(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	sr := NewSchemaRegistry(true, nil)
	nsID := ident.StringID("ns1")

	sl := &mockListener{}
	closer, err := sr.RegisterListener(nsID, sl)
	require.Error(t, err)
	require.Nil(t, closer)

	sh1 := NewMockSchemaHistory(ctrl)
	sh1.EXPECT().GetLatest().Return(nil, false)
	require.Error(t, sr.SetSchemaHistory(nsID, sh1))

	closer, err = sr.RegisterListener(nsID, sl)
	require.Error(t, err)
	require.Nil(t, closer)
	require.Nil(t, sl.Schema())

	sh2 := NewMockSchemaHistory(ctrl)
	inputSchema1 := NewMockSchemaDescr(ctrl)
	inputSchema1.EXPECT().DeployId().Return("version1")
	sh2.EXPECT().GetLatest().Return(inputSchema1, true)
	require.NoError(t, sr.SetSchemaHistory(nsID, sh2))

	sh2.EXPECT().GetLatest().Return(inputSchema1, true)
	closer, err = sr.RegisterListener(nsID, sl)
	require.NoError(t, err)
	require.NotNil(t, closer)
	require.NotNil(t, sl.Schema())
	require.Equal(t, "version1", sl.Schema().DeployId())

	sh3 := NewMockSchemaHistory(ctrl)
	inputSchema2 := NewMockSchemaDescr(ctrl)
	inputSchema2.EXPECT().DeployId().Return("version2")
	sh3.EXPECT().GetLatest().Return(inputSchema2, true).Times(2)
	sh3.EXPECT().Extends(gomock.Any()).Return(true)
	require.NoError(t, sr.SetSchemaHistory(nsID, sh3))
	time.Sleep(10 * time.Millisecond)
	require.NotNil(t, sl.Schema())
	require.Equal(t, "version2", sl.Schema().DeployId())
}
