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
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func SetupAsyncClientTest(t *testing.T) (*client.MockClient, *services.MockServices) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)

	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)

	return mockClient, mockServices
}

func TestAsyncClientError(t *testing.T) {
	customErr := errors.New("some error")
	expectedErrStr := fmt.Sprintf(errNewClientFailFmt, customErr)

	done := make(chan struct{}, 1)
	asyncClient := NewAsyncClient(func() (client.Client, error) {
		return nil, customErr
	}, done)
	require.NotNil(t, asyncClient)
	// Wait for session to be done initializing (which we mock to return an error)
	<-done

	_, err := asyncClient.Services(nil)
	assert.EqualError(t, err, expectedErrStr)

	_, err = asyncClient.KV()
	assert.EqualError(t, err, expectedErrStr)

	_, err = asyncClient.Txn()
	assert.EqualError(t, err, expectedErrStr)

	_, err = asyncClient.Store(nil)
	assert.EqualError(t, err, expectedErrStr)

	_, err = asyncClient.TxnStore(nil)
	assert.EqualError(t, err, expectedErrStr)
}

func TestAsyncClientUninitialized(t *testing.T) {
	mockClient, _ := SetupAsyncClientTest(t)

	// Sleep until test has finished
	var wg sync.WaitGroup
	wg.Add(1)
	defer wg.Done()

	asyncClient := NewAsyncClient(func() (client.Client, error) {
		wg.Wait()
		return mockClient, nil
	}, nil)
	require.NotNil(t, asyncClient)

	_, err := asyncClient.Services(nil)
	assert.EqualError(t, err, errClientUninitialized.Error())

	_, err = asyncClient.KV()
	assert.EqualError(t, err, errClientUninitialized.Error())

	_, err = asyncClient.Txn()
	assert.EqualError(t, err, errClientUninitialized.Error())

	_, err = asyncClient.Store(nil)
	assert.EqualError(t, err, errClientUninitialized.Error())

	_, err = asyncClient.TxnStore(nil)
	assert.EqualError(t, err, errClientUninitialized.Error())
}

func TestAsyncClientInitialized(t *testing.T) {
	mockClient, mockServices := SetupAsyncClientTest(t)

	mockClient.EXPECT().Services(nil).Return(mockServices, nil)

	done := make(chan struct{}, 1)
	asyncClient := NewAsyncClient(func() (client.Client, error) {
		return mockClient, nil
	}, done)
	require.NotNil(t, asyncClient)
	// Wait for session to be done initializing
	<-done

	services, err := asyncClient.Services(nil)
	assert.NoError(t, err)
	assert.True(t, services == mockServices)
}
