// Copyright (c) 2019 Uber Technologies, Inc.
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

package annotated

import (
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/generated/proto/prompb"
	"github.com/m3db/m3/src/query/models"
	xerrors "github.com/m3db/m3/src/x/errors"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/proto"
	"github.com/golang/snappy"
	"github.com/uber-go/tally"
)

const (
	// WriteURL is the URL for the annotated write handler.
	WriteURL = handler.RoutePrefixExperimental + "/prom/remote/annotated/write"

	// WriteHTTPMethod is the HTTP method for the annotated write handler.
	WriteHTTPMethod = http.MethodPost
)

var (
	errEmptyBody = xerrors.NewInvalidParamsError(errors.New("request body is empty"))
)

// Handler is the annotated endpoint handler.
type Handler struct {
	writer     ingest.DownsamplerAndWriter
	tagOptions models.TagOptions
	metrics    handlerMetrics
}

// NewHandler returns a new HTTP handler for writing annotated datapoints.
func NewHandler(
	writer ingest.DownsamplerAndWriter, tagOptions models.TagOptions, scope tally.Scope,
) *Handler {
	return &Handler{
		writer:     writer,
		tagOptions: tagOptions,
		metrics:    newHandlerMetrics(scope.Tagged(map[string]string{"handler": "annotated-write"})),
	}
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Body == nil {
		err := errEmptyBody
		h.metrics.incError(err)
		xhttp.WriteError(w, err)
		return
	}
	defer r.Body.Close()

	req, err := parseRequest(r.Body)
	if err != nil {
		resultError := xhttp.NewError(err, http.StatusBadRequest)
		h.metrics.incError(resultError)
		xhttp.WriteError(w, resultError)
		return
	}

	iter := newIter(req.Timeseries, h.tagOptions)
	batchErr := h.writer.WriteBatch(r.Context(), iter, ingest.WriteOptions{})
	if batchErr != nil {
		var foundInternalErr bool
		for _, err := range batchErr.Errors() {
			if client.IsBadRequestError(err) {
				continue
			}
			foundInternalErr = true
			break
		}

		status := http.StatusBadRequest
		if foundInternalErr {
			status = http.StatusInternalServerError
		}

		err = fmt.Errorf("unable to write metric batch, encountered %d errors: %w",
			len(batchErr.Errors()), batchErr)
		responseError := xhttp.NewError(err, status)
		h.metrics.incError(responseError)
		xhttp.WriteError(w, responseError)
		return
	}

	h.metrics.writeSuccess.Inc(1)
	w.WriteHeader(http.StatusOK)
}

func parseRequest(r io.Reader) (prompb.AnnotatedWriteRequest, error) {
	data, err := ioutil.ReadAll(r)
	if err != nil {
		return prompb.AnnotatedWriteRequest{}, err
	}

	data, err = snappy.Decode(nil, data)
	if err != nil {
		return prompb.AnnotatedWriteRequest{}, fmt.Errorf("unable to decode request body: %v", err)
	}

	var req prompb.AnnotatedWriteRequest
	if err := proto.Unmarshal(data, &req); err != nil {
		return prompb.AnnotatedWriteRequest{}, fmt.Errorf("unable to unmarshal request: %v", err)
	}

	return req, nil
}

type handlerMetrics struct {
	writeSuccess      tally.Counter
	writeErrorsServer tally.Counter
	writeErrorsClient tally.Counter
}

func newHandlerMetrics(s tally.Scope) handlerMetrics {
	return handlerMetrics{
		writeSuccess:      s.SubScope("write").Counter("success"),
		writeErrorsServer: s.SubScope("write").Tagged(map[string]string{"code": "5XX"}).Counter("errors"),
		writeErrorsClient: s.SubScope("write").Tagged(map[string]string{"code": "4XX"}).Counter("errors"),
	}
}

func (m *handlerMetrics) incError(err error) {
	if xhttp.IsClientError(err) {
		m.writeErrorsClient.Inc(1)
	} else {
		m.writeErrorsServer.Inc(1)
	}
}
