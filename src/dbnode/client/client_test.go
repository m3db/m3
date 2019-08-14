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

package client

import (
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func testClient(t *testing.T, ctrl *gomock.Controller) Client {
	opts := NewMockMultiClusterOptions(ctrl)
	opts.EXPECT().Validate().Return(nil)

	client, err := NewClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	return client
}

func TestClientNewClientValidatesOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	testClient(t, ctrl)

	anError := fmt.Errorf("an error")
	opts := NewMockMultiClusterOptions(ctrl)
	opts.EXPECT().Validate().Return(anError)

	_, err := NewClient(opts)
	assert.Error(t, err)
	assert.Equal(t, anError, err)
}

func TestClientNewSessionOpensSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var mockSession Session
	client := testClient(t, ctrl).(*client)
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		session := NewMockclientSession(ctrl)
		session.EXPECT().Open().Return(nil)
		mockSession = session
		return session, nil
	}

	session, err := client.NewSession()
	assert.NoError(t, err)
	assert.Equal(t, mockSession, session)
}

func TestClientNewSessionFailCreateReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := testClient(t, ctrl).(*client)
	anError := fmt.Errorf("an error")
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		return nil, anError
	}

	session, err := client.NewSession()
	assert.Error(t, err)
	assert.Equal(t, anError, err)
	assert.Nil(t, session)
}

func TestClientNewSessionFailOpenReturnsError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := testClient(t, ctrl).(*client)
	anError := fmt.Errorf("an error")
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		session := NewMockclientSession(ctrl)
		session.EXPECT().Open().Return(anError)
		return session, nil
	}

	session, err := client.NewSession()
	assert.Error(t, err)
	assert.Equal(t, anError, err)
	assert.Nil(t, session)
}

func TestClientDefaultSessionAlreadyCreated(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := testClient(t, ctrl).(*client)
	session := NewMockAdminSession(ctrl)
	client.session = session

	defaultSession, err := client.DefaultSession()
	assert.NoError(t, err)
	assert.Equal(t, session, defaultSession)
}

func TestClientDefaultSessionNotCreatedNoError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := testClient(t, ctrl).(*client)
	session := NewMockclientSession(ctrl)
	session.EXPECT().Open().Return(nil)
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		return session, nil
	}

	defaultSession, err := client.DefaultSession()
	assert.NoError(t, err)
	assert.Equal(t, session, defaultSession)
}

func TestClientDefaultSessionNotCreatedWithError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	client := testClient(t, ctrl).(*client)
	expectedErr := errors.New("foo")
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		return nil, expectedErr
	}

	defaultSession, err := client.DefaultSession()
	assert.Equal(t, expectedErr, err)
	assert.Nil(t, defaultSession)
}

func TestClientDefaultSessionMultipleSimultaneousRequests(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	var (
		lock           sync.Mutex
		defaultSession AdminSession
		wg             sync.WaitGroup
	)

	client := testClient(t, ctrl).(*client)
	client.newSessionFn = func(opts MultiClusterOptions, _ ...replicatedSessionOption) (clientSession, error) {
		session := NewMockclientSession(ctrl)
		session.EXPECT().Open().Return(nil)
		lock.Lock()
		if defaultSession == nil {
			defaultSession = session
		}
		lock.Unlock()
		return session, nil
	}

	numRequests := 10
	for i := 0; i < numRequests; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			sess, err := client.DefaultSession()
			assert.NoError(t, err)
			assert.Equal(t, defaultSession, sess)
		}()
	}

	wg.Wait()
}
