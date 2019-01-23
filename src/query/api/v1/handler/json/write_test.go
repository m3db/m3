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
	"context"
	"net/http"
	"strings"
	"testing"

	"github.com/m3db/m3/src/query/test/m3"
	"github.com/m3db/m3/src/query/util/logging"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/require"
)

func TestFailingJSONWriteParsing(t *testing.T) {
	badJSON := `{
		   "tags": { "t
			 "timestamp": "1534952005",
			 "value": 10.0
					}`

	req, _ := http.NewRequest("POST", WriteJSONURL, strings.NewReader(badJSON))
	jsonWrite := &WriteJSONHandler{store: nil}
	_, err := jsonWrite.parseRequest(req)
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
	logging.InitWithCores(nil)

	jsonWrite := &WriteJSONHandler{store: nil}

	jsonReq := generateJSONWriteRequest()
	req, _ := http.NewRequest("POST", WriteJSONURL, strings.NewReader(jsonReq))

	r, err := jsonWrite.parseRequest(req)
	require.Nil(t, err, "unable to parse request")
	require.Equal(t, 10.0, r.Value)
	require.Equal(t, map[string]string{"tag_one": "val_one", "tag_two": "val_two"}, r.Tags)
}

func TestJSONWrite(t *testing.T) {
	logging.InitWithCores(nil)

	ctrl := gomock.NewController(t)
	storage, session := m3.NewStorageAndSession(t, ctrl)
	session.EXPECT().
		WriteTagged(gomock.Any(), gomock.Any(), gomock.Any(),
			gomock.Any(), gomock.Any(), gomock.Any(), gomock.Any()).
		AnyTimes()
	session.EXPECT().IteratorPools().
		Return(nil, nil).AnyTimes()

	jsonWrite := &WriteJSONHandler{store: storage}

	jsonReq := generateJSONWriteRequest()
	req, err := http.NewRequest(JSONWriteHTTPMethod, WriteJSONURL,
		strings.NewReader(jsonReq))
	require.NoError(t, err)

	r, rErr := jsonWrite.parseRequest(req)
	require.Nil(t, rErr, "unable to parse request")

	writeQuery, err := newStorageWriteQuery(r)
	require.NoError(t, err)

	writeErr := jsonWrite.store.Write(context.TODO(), writeQuery)
	require.NoError(t, writeErr)
}
