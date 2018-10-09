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

package httpd

import (
	"encoding/json"
	"net/http"
	"time"

	jsonhandler "github.com/m3db/m3/src/collector/api/v1/handler/json"
	"github.com/m3db/m3/src/collector/reporter"
	"github.com/m3db/m3/src/x/serialize"
	"github.com/m3db/m3x/instrument"

	"github.com/gorilla/mux"
)

const (
	healthURL = "/health"
)

// Handler represents an HTTP handler.
type Handler struct {
	router         *mux.Router
	reporter       reporter.Reporter
	encoderPool    serialize.TagEncoderPool
	decoderPool    serialize.TagDecoderPool
	instrumentOpts instrument.Options
	createdAt      time.Time
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	reporter reporter.Reporter,
	encoderPool serialize.TagEncoderPool,
	decoderPool serialize.TagDecoderPool,
	instrumentOpts instrument.Options,
) (*Handler, error) {
	return &Handler{
		router:         mux.NewRouter(),
		reporter:       reporter,
		encoderPool:    encoderPool,
		decoderPool:    decoderPool,
		instrumentOpts: instrumentOpts,
		createdAt:      time.Now(),
	}, nil
}

// Router returns the handler router.
func (h *Handler) Router() *mux.Router {
	return h.router
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() error {
	// Report handler
	reportHandler := jsonhandler.NewReportJSONHandler(h.reporter, h.encoderPool,
		h.decoderPool, h.instrumentOpts)
	h.router.
		Handle(jsonhandler.ReportJSONURL, reportHandler).
		Methods(jsonhandler.JSONReportHTTPMethod)

	h.registerHealthEndpoints()

	return nil
}

func (h *Handler) registerHealthEndpoints() {
	h.router.HandleFunc(healthURL, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Uptime string `json:"uptime"`
		}{
			Uptime: time.Since(h.createdAt).String(),
		})
	}).Methods(http.MethodGet)
}
