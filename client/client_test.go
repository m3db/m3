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
	"fmt"
	"testing"

	"github.com/m3db/m3db/interfaces/m3db"
	"github.com/m3db/m3db/mocks"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
)

func TestClientNewClientValidatesOptions(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := mocks.NewMockClientOptions(ctrl)
	opts.EXPECT().Validate().Return(nil)

	client, err := NewClient(opts)
	assert.NoError(t, err)
	assert.NotNil(t, client)

	anError := fmt.Errorf("an error")
	opts = mocks.NewMockClientOptions(ctrl)
	opts.EXPECT().Validate().Return(anError)

	_, err = NewClient(opts)
	assert.Error(t, err)
	assert.Equal(t, anError, err)
}

func TestClientNewSessionOpensSession(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	opts := mocks.NewMockClientOptions(ctrl)
	opts.EXPECT().Validate().Return(nil)

	cli, err := NewClient(opts)
	assert.NoError(t, err)

	var mockSession m3db.Session
	client := cli.(*client)
	client.newSessionFn = func(opts m3db.ClientOptions) (clientSession, error) {
		session := mocks.NewMockclientSession(ctrl)
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

	opts := mocks.NewMockClientOptions(ctrl)
	opts.EXPECT().Validate().Return(nil)

	cli, err := NewClient(opts)
	assert.NoError(t, err)

	client := cli.(*client)
	anError := fmt.Errorf("an error")
	client.newSessionFn = func(opts m3db.ClientOptions) (clientSession, error) {
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

	opts := mocks.NewMockClientOptions(ctrl)
	opts.EXPECT().Validate().Return(nil)

	cli, err := NewClient(opts)
	assert.NoError(t, err)

	client := cli.(*client)
	anError := fmt.Errorf("an error")
	client.newSessionFn = func(opts m3db.ClientOptions) (clientSession, error) {
		session := mocks.NewMockclientSession(ctrl)
		session.EXPECT().Open().Return(anError)
		return session, nil
	}

	session, err := client.NewSession()
	assert.Error(t, err)
	assert.Equal(t, anError, err)
	assert.Nil(t, session)
}
