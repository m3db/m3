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

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3/storagemetadata"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/util"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
	xtime "github.com/m3db/m3/src/x/time"

	"go.uber.org/zap"
)

const (
	// WriteJSONURL is the url for the write json handler
	WriteJSONURL = route.Prefix + "/json/write"

	// JSONWriteHTTPMethod is the HTTP method used with this resource.
	JSONWriteHTTPMethod = http.MethodPost
)

// WriteJSONHandler represents a handler for the write json endpoint
type WriteJSONHandler struct {
	opts           options.HandlerOptions
	store          storage.Storage
	instrumentOpts instrument.Options
}

// NewWriteJSONHandler returns a new instance of handler.
func NewWriteJSONHandler(opts options.HandlerOptions) http.Handler {
	return &WriteJSONHandler{
		opts:           opts,
		store:          opts.Storage(),
		instrumentOpts: opts.InstrumentOpts(),
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
	req, rErr := parseRequest(r)
	if rErr != nil {
		xhttp.WriteError(w, rErr)
		return
	}

	writeQuery, err := h.newWriteQuery(req)
	if err != nil {
		logger := logging.WithContext(r.Context(), h.instrumentOpts)
		logger.Error("parsing error",
			zap.String("remoteAddr", r.RemoteAddr),
			zap.Error(err))
		xhttp.WriteError(w, err)
	}

	if err := h.store.Write(r.Context(), writeQuery); err != nil {
		logger := logging.WithContext(r.Context(), h.instrumentOpts)
		logger.Error("write error",
			zap.String("remoteAddr", r.RemoteAddr),
			zap.Error(err))
		xhttp.WriteError(w, err)
	}
}

func (h *WriteJSONHandler) newWriteQuery(req *WriteQuery) (*storage.WriteQuery, error) {
	parsedTime, err := util.ParseTimeString(req.Timestamp)
	if err != nil {
		return nil, err
	}

	tags := models.NewTags(len(req.Tags), h.opts.TagOptions())
	for n, v := range req.Tags {
		tags = tags.AddTag(models.Tag{Name: []byte(n), Value: []byte(v)})
	}

	return storage.NewWriteQuery(storage.WriteQueryOptions{
		Tags: tags,
		Datapoints: ts.Datapoints{
			{
				Timestamp: xtime.ToUnixNano(parsedTime),
				Value:     req.Value,
			},
		},
		Unit:       xtime.Millisecond,
		Annotation: nil,
		Attributes: storagemetadata.Attributes{
			MetricsType: storagemetadata.UnaggregatedMetricsType,
		},
	})
}

func parseRequest(r *http.Request) (*WriteQuery, error) {
	body := r.Body
	if r.Body == nil {
		return nil, xerrors.NewInvalidParamsError(fmt.Errorf("empty request body"))
	}

	defer body.Close()

	js, err := ioutil.ReadAll(body)
	if err != nil {
		return nil, err
	}

	var writeQuery *WriteQuery
	if err = json.Unmarshal(js, &writeQuery); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return writeQuery, nil
}
