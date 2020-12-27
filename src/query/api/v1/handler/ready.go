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

package handler

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"

	"go.uber.org/zap"

	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// ReadyURL is the url to check for readiness.
	ReadyURL = "/ready"

	// ReadyHTTPMethod is the HTTP method used with this resource.
	ReadyHTTPMethod = http.MethodGet
)

// ReadyHandler tests whether the service is connected to underlying storage.
type ReadyHandler struct {
	clusters       m3.Clusters
	instrumentOpts instrument.Options
}

// NewReadyHandler returns a new instance of handler.
func NewReadyHandler(opts options.HandlerOptions) http.Handler {
	return &ReadyHandler{
		clusters:       opts.Clusters(),
		instrumentOpts: opts.InstrumentOpts(),
	}
}

type readyResultNamespace struct {
	ID         string                         `json:"id"`
	Attributes readyResultNamespaceAttributes `json:"attributes"`
}

type readyResultNamespaceAttributes struct {
	MetricsType string `json:"metricsType"`
	Retention   string `json:"retention"`
	Resolution  string `json:"resolution"`
}

type readyResult struct {
	ReadyReads     []readyResultNamespace `json:"readyReads,omitempty"`
	NotReadyReads  []readyResultNamespace `json:"notReadyReads,omitempty"`
	ReadyWrites    []readyResultNamespace `json:"readyWrites,omitempty"`
	NotReadyWrites []readyResultNamespace `json:"notReadyWrites,omitempty"`
}

// ServeHTTP serves HTTP handler. This comment only here so doesn't break
// lint by not being "ServeHTTP" as the comment above this function
// which needs // nolint:gocyclo.
// nolint:gocyclo
func (h *ReadyHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context(), h.instrumentOpts)

	req, err := h.parseReadyRequestChecked(r)
	if err != nil {
		logger.Error("unable to parse ready request", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	var (
		namespaces = h.clusters.ClusterNamespaces()
		result     = &readyResult{}
	)
	for _, ns := range namespaces {
		attrs := ns.Options().Attributes()
		nsResult := readyResultNamespace{
			ID: ns.NamespaceID().String(),
			Attributes: readyResultNamespaceAttributes{
				MetricsType: attrs.MetricsType.String(),
				Retention:   attrs.Retention.String(),
				Resolution:  attrs.Resolution.String(),
			},
		}

		ready, err := ns.Session().ReadClusterAvailability()
		if err != nil {
			logger.Error("check read availability error", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
		if !ready {
			result.NotReadyReads = append(result.NotReadyReads, nsResult)
		} else {
			result.ReadyReads = append(result.ReadyReads, nsResult)
		}

		ready, err = ns.Session().WriteClusterAvailability()
		if err != nil {
			logger.Error("check write availability error", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
		if !ready {
			result.NotReadyWrites = append(result.NotReadyWrites, nsResult)
		} else {
			result.ReadyWrites = append(result.ReadyWrites, nsResult)
		}
	}

	resp, err := json.Marshal(result)
	if err != nil {
		xhttp.WriteError(w, err)
		return
	}

	if n := len(result.NotReadyReads); req.reads && n > 0 {
		err := fmt.Errorf("not ready namespaces for read: %d", n)
		xhttp.WriteError(w, err, xhttp.WithErrorResponse(resp))
		return
	}

	if n := len(result.NotReadyWrites); req.writes && n > 0 {
		err := fmt.Errorf("not ready namespaces for write: %d", n)
		xhttp.WriteError(w, err, xhttp.WithErrorResponse(resp))
		return
	}

	xhttp.WriteJSONResponse(w, result, logger)
}

type readyRequest struct {
	reads  bool
	writes bool
}

func (h *ReadyHandler) parseReadyRequestChecked(r *http.Request) (readyRequest, error) {
	result, err := h.parseReadyRequest(r)
	if err != nil {
		// All request parsing errors should be treated as invalid params err.
		return readyRequest{}, xerrors.NewInvalidParamsError(err)
	}
	return result, nil
}

func (h *ReadyHandler) parseReadyRequest(r *http.Request) (readyRequest, error) {
	// Default to checking for both read and write availability.
	var (
		req = readyRequest{
			reads:  true,
			writes: true,
		}
		err error
	)
	if str := r.URL.Query().Get("reads"); str != "" {
		req.reads, err = strconv.ParseBool(str)
		if err != nil {
			return readyRequest{}, xerrors.NewInvalidParamsError(err)
		}
	}
	if str := r.URL.Query().Get("writes"); str != "" {
		req.writes, err = strconv.ParseBool(str)
		if err != nil {
			return readyRequest{}, xerrors.NewInvalidParamsError(err)
		}
	}
	return req, nil
}
