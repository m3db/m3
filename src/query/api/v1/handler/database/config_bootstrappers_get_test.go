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

package database

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigGetBootstrappersHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockStore, _ := SetupDatabaseTest(t, ctrl)
	handler := NewConfigGetBootstrappersHandler(mockClient)
	w := httptest.NewRecorder()

	value := kv.NewMockValue(ctrl)
	value.EXPECT().
		Unmarshal(gomock.Any()).
		Return(nil).
		Do(func(v proto.Message) {
			array, ok := v.(*commonpb.StringArrayProto)
			require.True(t, ok)
			array.Values = []string{"filesystem", "commitlog", "peers", "uninitialized_topology"}
		})

	mockStore.EXPECT().
		Get(kvconfig.BootstrapperKey).
		Return(value, nil)

	req := httptest.NewRequest("GET", "/database/config/bootstrappers", nil)
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResponse := `
	{
		"values": ["filesystem", "commitlog", "peers", "uninitialized_topology"]
	}
	`
	assert.Equal(t, stripAllWhitespace(expectedResponse), string(body),
		xtest.Diff(mustPrettyJSON(t, expectedResponse), mustPrettyJSON(t, string(body))))
}

func TestConfigGetBootstrappersHandlerNotFound(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockStore, _ := SetupDatabaseTest(t, ctrl)
	handler := NewConfigGetBootstrappersHandler(mockClient)
	w := httptest.NewRecorder()

	mockStore.EXPECT().
		Get(kvconfig.BootstrapperKey).
		Return(nil, kv.ErrNotFound)

	req := httptest.NewRequest("GET", "/database/config/bootstrappers", nil)
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusNotFound, resp.StatusCode)
}
