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

package json

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/test/m3"
)

func TestFailingJSONWriteParsing(t *testing.T) {
	badJSON := `{
		   "tags": { "t
			 "timestamp": "1534952005",
			 "value": 10.0
					}`

	req, _ := http.NewRequest("POST", WriteJSONURL, strings.NewReader(badJSON))
	_, err := parseRequest(req)
	require.Error(t, err)
}

func generateJSONWriteRequest() string {
	return `{
		   "tags": { "tag_one": "val_one", "tag_two": "val_two" },
			 "timestamp": "1534952005",
			 "value": 10.0
		      }`
}

func TestJSONWriteParsing(t *testing.T) {
	jsonReq := generateJSONWriteRequest()
	req := httptest.NewRequest("POST", WriteJSONURL, strings.NewReader(jsonReq))

	r, err := parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, 10.0, r.Value)
	require.Equal(t, map[string]string{"tag_one": "val_one", "tag_two": "val_two"}, r.Tags)
}

func TestJSONWrite(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()
	session.EXPECT().IteratorPools().
		Return(nil, nil).AnyTimes()

	opts := options.EmptyHandlerOptions().
		SetTagOptions(models.NewTagOptions()).
		SetStorage(storage)
	handler := NewWriteJSONHandler(opts).(*WriteJSONHandler)

	jsonReq := generateJSONWriteRequest()
	req, err := http.NewRequest(JSONWriteHTTPMethod, WriteJSONURL,
		strings.NewReader(jsonReq))
	require.NoError(t, err)

	resp := httptest.NewRecorder()
	handler.ServeHTTP(resp, req)

	require.Equal(t, http.StatusOK, resp.Code)
}

func TestJSONWriteError(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()

	expectedErr := fmt.Errorf("an error")

	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes().
		Return(expectedErr)
	session.EXPECT().IteratorPools().
		Return(nil, nil).AnyTimes()

	opts := options.EmptyHandlerOptions().SetStorage(storage)
	jsonWrite := NewWriteJSONHandler(opts).(*WriteJSONHandler)

	jsonReq := generateJSONWriteRequest()
	req, err := http.NewRequest(JSONWriteHTTPMethod, WriteJSONURL,
		strings.NewReader(jsonReq))
	require.NoError(t, err)

	writer := httptest.NewRecorder()
	jsonWrite.ServeHTTP(writer, req)
	resp := writer.Result()
	require.Equal(t, http.StatusInternalServerError, resp.StatusCode)

	body, err := ioutil.ReadAll(resp.Body)
	require.NoError(t, err)

	require.True(t, bytes.Contains(body, []byte(expectedErr.Error())),
		fmt.Sprintf("body: %s", body))
}
