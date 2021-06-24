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

package m3db

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/storage/index"
	xcontext "github.com/m3db/m3/src/x/context"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	namespace = ident.StringID("test-namespace")
)

func SetupAsyncSessionTest(t *testing.T) (*client.MockClient, *client.MockSession) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockSession := client.NewMockSession(ctrl)
	require.NotNil(t, mockSession)

	return mockClient, mockSession
}

func TestAsyncSessionError(t *testing.T) {
	mockClient, _ := SetupAsyncSessionTest(t)

	customErr := errors.New("some error")
	expectedErrStr := fmt.Sprintf(errNewSessionFailFmt, customErr)

	mockClient.EXPECT().DefaultSession().Return(nil, customErr)
	done := make(chan struct{}, 1)
	asyncSession := NewAsyncSession(func() (client.Client, error) {
		return mockClient, nil
	}, done)
	require.NotNil(t, asyncSession)
	// Wait for session to be done initializing (which we mock to return an error)
	<-done

	err := asyncSession.Write(nil, nil, xtime.Now(), 0, xtime.Second, nil)
	assert.EqualError(t, err, expectedErrStr)

	err = asyncSession.WriteTagged(xcontext.NewBackground(), nil, nil, nil, xtime.Now(), 0,
		xtime.Second, nil)
	assert.EqualError(t, err, expectedErrStr)

	seriesIterator, err := asyncSession.Fetch(nil, nil, xtime.Now(), xtime.Now())
	assert.Nil(t, seriesIterator)
	assert.EqualError(t, err, expectedErrStr)

	seriesIterators, err := asyncSession.FetchIDs(nil, nil, xtime.Now(), xtime.Now())
	assert.Nil(t, seriesIterators)
	assert.EqualError(t, err, expectedErrStr)

	pools, err := asyncSession.IteratorPools()
	assert.Nil(t, pools)
	assert.EqualError(t, err, expectedErrStr)
}

func TestAsyncSessionUninitialized(t *testing.T) {
	mockClient, _ := SetupAsyncSessionTest(t)

	// Sleep one minute after a NewSession call to ensure we get an "uninitialized" error
	mockClient.EXPECT().DefaultSession().Do(func() { time.Sleep(time.Minute) }).Return(nil, errors.New("some error"))
	asyncSession := NewAsyncSession(func() (client.Client, error) {
		return mockClient, nil
	}, nil)
	require.NotNil(t, asyncSession)

	results, meta, err := asyncSession.FetchTagged(context.Background(),
		namespace, index.Query{}, index.QueryOptions{})
	assert.Nil(t, results)
	assert.False(t, meta.Exhaustive)
	assert.Equal(t, err, errSessionUninitialized)

	_, _, err = asyncSession.FetchTaggedIDs(context.Background(),
		namespace, index.Query{}, index.QueryOptions{})
	assert.Equal(t, err, errSessionUninitialized)

	_, _, err = asyncSession.Aggregate(context.Background(),
		namespace, index.Query{}, index.AggregationOptions{})
	assert.Equal(t, err, errSessionUninitialized)

	id, err := asyncSession.ShardID(nil)
	assert.Equal(t, uint32(0), id)
	assert.Equal(t, err, errSessionUninitialized)

	err = asyncSession.Close()
	assert.Equal(t, err, errSessionUninitialized)

	pools, err := asyncSession.IteratorPools()
	assert.Nil(t, pools)
	assert.Equal(t, err, errSessionUninitialized)
}

func TestAsyncSessionInitialized(t *testing.T) {
	mockClient, mockSession := SetupAsyncSessionTest(t)

	mockClient.EXPECT().DefaultSession().Return(mockSession, nil)
	done := make(chan struct{}, 1)
	asyncSession := NewAsyncSession(func() (client.Client, error) {
		return mockClient, nil
	}, done)
	require.NotNil(t, asyncSession)
	// Wait for session to be done initializing
	<-done

	mockSession.EXPECT().
		Write(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil)
	err := asyncSession.Write(nil, nil, xtime.Now(), 0, xtime.Second, nil)
	assert.NoError(t, err)

	mockSession.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any()).
		Return(nil)
	err = asyncSession.WriteTagged(xcontext.NewBackground(), nil, nil, nil, xtime.Now(), 0,
		xtime.Second, nil)
	assert.NoError(t, err)

	mockSession.EXPECT().
		Fetch(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil)
	_, err = asyncSession.Fetch(nil, nil, xtime.Now(), xtime.Now())
	assert.NoError(t, err)

	mockSession.EXPECT().
		FetchIDs(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, nil)
	_, err = asyncSession.FetchIDs(nil, nil, xtime.Now(), xtime.Now())
	assert.NoError(t, err)

	mockSession.EXPECT().
		FetchTagged(context.Background(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, nil)
	_, _, err = asyncSession.FetchTagged(context.Background(),
		namespace, index.Query{}, index.QueryOptions{})
	assert.NoError(t, err)

	mockSession.EXPECT().
		FetchTaggedIDs(context.Background(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, nil)
	_, _, err = asyncSession.FetchTaggedIDs(context.Background(),
		namespace, index.Query{}, index.QueryOptions{})
	assert.NoError(t, err)

	mockSession.EXPECT().
		Aggregate(context.Background(), gomock.Any(), gomock.Any(), gomock.Any()).
		Return(nil, client.FetchResponseMetadata{Exhaustive: false}, nil)
	_, _, err = asyncSession.Aggregate(context.Background(),
		namespace, index.Query{}, index.AggregationOptions{})
	assert.NoError(t, err)

	mockSession.EXPECT().ShardID(gomock.Any()).Return(uint32(0), nil)
	_, err = asyncSession.ShardID(nil)
	assert.NoError(t, err)

	mockSession.EXPECT().Close().Return(nil)
	err = asyncSession.Close()
	assert.NoError(t, err)

	mockSession.EXPECT().IteratorPools().Return(nil, nil)
	_, err = asyncSession.IteratorPools()
	assert.NoError(t, err)
}
