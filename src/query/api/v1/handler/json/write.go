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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3x/time"

	"go.uber.org/zap"
)

const (
	// WriteJSONURL is the url for the write json handler
	WriteJSONURL = handler.RoutePrefixV1 + "/json/write"

	// JSONWriteHTTPMethod is the HTTP method used with this resource.
	JSONWriteHTTPMethod = http.MethodPost
)

// WriteJSONHandler represents a handler for the write json endpoint
type WriteJSONHandler struct {
	store storage.Storage
}

// NewWriteJSONHandler returns a new instance of handler.
func NewWriteJSONHandler(store storage.Storage) http.Handler {
	return &WriteJSONHandler{
		store: store,
	}
}

// WriteQuery represents the write request from the user
// NB(braskin): support only writing one datapoint for now
// TODO: build this out to be a legitimate batched endpoint, change
// Tags to take a list of tag structs
type WriteQuery struct {
	Tags      map[string]string `json:"tags" validate:"nonzero"`
	Timestamp string            `json:"timestamp" validate:"nonzero"`
	Value     float64           `json:"value" validate:"nonzero"`
}

func (h *WriteJSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	writeQuery, err := newStorageWriteQuery(req)
	if err != nil {
		logging.WithContext(r.Context()).Error("Parsing error", zap.Any("err", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
	}

	if err := h.store.Write(r.Context(), writeQuery); err != nil {
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
	}
}

func newStorageWriteQuery(req *WriteQuery) (*storage.WriteQuery, error) {
	parsedTime, err := util.ParseTimeString(req.Timestamp)
	if err != nil {
		return nil, err
	}

	tags := models.NewTags(len(req.Tags), nil)
	for n, v := range req.Tags {
		tags = tags.AddTag(models.Tag{Name: []byte(n), Value: []byte(v)})
	}

	return &storage.WriteQuery{
		Tags: tags,
		Datapoints: ts.Datapoints{
			{
				Timestamp: parsedTime,
				Value:     req.Value,
			},
		},
		Unit:       xtime.Millisecond,
		Annotation: nil,
	}, nil
}

func (h *WriteJSONHandler) parseRequest(r *http.Request) (*WriteQuery, *xhttp.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	defer body.Close()

	js, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusInternalServerError)
	}

	var writeQuery *WriteQuery
	json.Unmarshal(js, &writeQuery)

	return writeQuery, nil
}
