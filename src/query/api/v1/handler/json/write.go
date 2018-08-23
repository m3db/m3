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
	"time"

	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util/logging"
	xtime "github.com/m3db/m3x/time"

	"go.uber.org/zap"
)

const (
	// WriteJSONURL is the url for the write json handler
	WriteJSONURL = handler.RoutePrefixV1 + "/write_json"
)

// WriteJSONHandler represents a handler for write json endpoint.
type WriteJSONHandler struct {
	store storage.Storage
}

// WriteQuery represents the write request from the user
type WriteQuery struct {
	Tags      map[string]string `json:"tags"`
	Timestamp int               `json:"timestamp"`
	Value     float64           `json:"value" validate:"nonzero"`
}

func (h *WriteJSONHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	req, rErr := h.parseRequest(r)
	if rErr != nil {
		handler.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	writeQuery := newStorageWriteQuery(req)

	if err := h.store.Write(r.Context(), writeQuery); err != nil {
		logging.WithContext(r.Context()).Error("Write error", zap.Any("err", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}
}

func newStorageWriteQuery(req *WriteQuery) *storage.WriteQuery {
	return &storage.WriteQuery{
		Tags: req.Tags,
		Datapoints: ts.Datapoints{
			{
				Timestamp: time.Unix(int64(req.Timestamp), 0),
				Value:     req.Value,
			},
		},
		Unit:       xtime.Millisecond,
		Annotation: nil,
	}
}

func (h *WriteJSONHandler) parseRequest(r *http.Request) (*WriteQuery, *handler.ParseError) {
	body := r.Body
	if r.Body == nil {
		err := fmt.Errorf("empty request body")
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	defer body.Close()

	js, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusInternalServerError)
	}

	var writeQuery *WriteQuery
	json.Unmarshal(js, &writeQuery)

	return writeQuery, nil
}
