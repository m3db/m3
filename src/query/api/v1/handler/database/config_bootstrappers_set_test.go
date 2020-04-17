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
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/x/instrument"
	xjson "github.com/m3db/m3/src/x/json"
	xtest "github.com/m3db/m3/src/x/test"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigSetBootstrappersHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, mockStore, _ := SetupDatabaseTest(t, ctrl)
	handler := NewConfigSetBootstrappersHandler(mockClient,
		instrument.NewOptions())
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"values": xjson.Array{"filesystem", "commitlog", "peers", "uninitialized_topology"},
	}

	mockStore.EXPECT().
		Set(kvconfig.BootstrapperKey, gomock.Any()).
		Return(int(1), nil).
		Do(func(key string, value *commonpb.StringArrayProto) {
			assert.Equal(t, []string{
				"filesystem", "commitlog", "peers", "uninitialized_topology",
			}, value.Values)
		})

	req := httptest.NewRequest("POST", "/database/config/bootstrappers",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, err := ioutil.ReadAll(resp.Body)
	assert.NoError(t, err)
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	expectedResp := xjson.Map{
		"values": xjson.Array{"filesystem", "commitlog", "peers", "uninitialized_topology"},
	}

	expected := xtest.MustPrettyJSONMap(t, expectedResp)
	actual := xtest.MustPrettyJSONString(t, string(body))

	assert.Equal(t, expected, actual, xtest.Diff(expected, actual))
}

func TestConfigSetBootstrappersHandlerNoValues(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, _ := SetupDatabaseTest(t, ctrl)
	handler := NewConfigSetBootstrappersHandler(mockClient,
		instrument.NewOptions())
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"values": xjson.Array{},
	}

	req := httptest.NewRequest("POST", "/database/config/bootstrappers",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}

func TestConfigSetBootstrappersHandlerInvalidValue(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	mockClient, _, _ := SetupDatabaseTest(t, ctrl)
	handler := NewConfigSetBootstrappersHandler(mockClient,
		instrument.NewOptions())
	w := httptest.NewRecorder()

	jsonInput := xjson.Map{
		"values": xjson.Array{"filesystem", "foo"},
	}

	req := httptest.NewRequest("POST", "/database/config/bootstrappers",
		xjson.MustNewTestReader(t, jsonInput))
	require.NotNil(t, req)

	handler.ServeHTTP(w, req)

	resp := w.Result()
	assert.Equal(t, http.StatusBadRequest, resp.StatusCode)
}
