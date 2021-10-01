// Copyright (c) 2020 Uber Technologies, Inc.
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

package httpjson

import (
	"bytes"
	"encoding/json"
	"errors"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/network/server/tchannelthrift/cluster"
	"github.com/m3db/m3/src/x/headers"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	apachethrift "github.com/apache/thrift/lib/go/thrift"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
	"golang.org/x/net/context"
)

var (
	// testClientOptions to not allocate multiple times.
	testClientOptions = client.NewOptions()

	errTestClientSessionError = errors.New("session test error")
)

func newTestClient(ctrl *gomock.Controller) *client.MockClient {
	client := client.NewMockClient(ctrl)
	client.EXPECT().Options().Return(testClientOptions).AnyTimes()
	client.EXPECT().NewSessionWithOptions(gomock.Any()).Return(nil, errTestClientSessionError).AnyTimes()
	return client
}

func TestRegisterHandlersRequestSimple(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mux := http.NewServeMux()

	client := newTestClient(ctrl)
	service := cluster.NewService(client)

	opts := NewServerOptions()

	err := RegisterHandlers(mux, service, opts)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	mux.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
}

func TestRegisterHandlersRequestPostRequestFn(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mux := http.NewServeMux()

	client := newTestClient(ctrl)
	service := cluster.NewService(client)

	calledPostRequestFn := 0
	opts := NewServerOptions().
		SetPostResponseFn(func(
			_ context.Context,
			method string,
			_ apachethrift.TStruct,
		) {
			require.Equal(t, "Health", method)
			calledPostRequestFn++
		})

	err := RegisterHandlers(mux, service, opts)
	require.NoError(t, err)

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/health", nil)
	mux.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusOK, recorder.Code)
	require.Equal(t, 1, calledPostRequestFn)
}

func TestRegisterHandlersRequestUnknownFieldsBadRequestError(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mux := http.NewServeMux()

	client := newTestClient(ctrl)
	service := cluster.NewService(client)

	opts := NewServerOptions()

	err := RegisterHandlers(mux, service, opts)
	require.NoError(t, err)

	// Create a payload with unknown field.
	payload := xjson.Map{"unknownFieldName": "unknownFieldValue"}

	body := bytes.NewBuffer(nil)
	require.NoError(t, json.NewEncoder(body).Encode(payload))

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/query", body)
	mux.ServeHTTP(recorder, req)

	require.Equal(t, http.StatusBadRequest, recorder.Code)

	out, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	str := string(out)
	require.True(t, strings.Contains(str, "invalid request body:"))
	require.True(t, strings.Contains(str, "unknown field"))
}

func TestRegisterHandlersRequestDisableUnknownFields(t *testing.T) {
	ctrl := xtest.NewController(t)
	defer ctrl.Finish()

	mux := http.NewServeMux()

	client := newTestClient(ctrl)
	service := cluster.NewService(client)

	opts := NewServerOptions()

	err := RegisterHandlers(mux, service, opts)
	require.NoError(t, err)

	// Create a payload with unknown field.
	payload := xjson.Map{"unknownFieldName": "unknownFieldValue"}

	body := bytes.NewBuffer(nil)
	require.NoError(t, json.NewEncoder(body).Encode(payload))

	recorder := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/query", body)
	req.Header.Set(headers.JSONDisableDisallowUnknownFields, "true")
	mux.ServeHTTP(recorder, req)

	// Make sure not bad request, but expected error.
	require.Equal(t, http.StatusInternalServerError, recorder.Code)

	out, err := ioutil.ReadAll(recorder.Result().Body)
	require.NoError(t, err)
	str := string(out)
	require.True(t, strings.Contains(str, errTestClientSessionError.Error()))
}
