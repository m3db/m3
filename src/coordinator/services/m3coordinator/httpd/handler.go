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
	"log"
	"net/http/pprof"
	"os"

	"github.com/m3db/m3db/src/coordinator/executor"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/config"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler/namespace"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler/placement"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler/prometheus/native"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler/prometheus/remote"
	"github.com/m3db/m3db/src/coordinator/storage"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	pprofURL = "/debug/pprof/profile"
)

// Handler represents an HTTP handler.
type Handler struct {
	Router        *mux.Router
	CLFLogger     *log.Logger
	storage       storage.Storage
	engine        *executor.Engine
	clusterClient m3clusterClient.Client
	config        config.Configuration
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(storage storage.Storage, engine *executor.Engine, clusterClient m3clusterClient.Client, cfg config.Configuration) (*Handler, error) {
	r := mux.NewRouter()
	logger, err := zap.NewProduction()
	if err != nil {
		return nil, err
	}

	defer logger.Sync() // flushes buffer, if any
	h := &Handler{
		CLFLogger:     log.New(os.Stderr, "[httpd] ", 0),
		Router:        r,
		storage:       storage,
		engine:        engine,
		clusterClient: clusterClient,
		config:        cfg,
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() error {
	logged := logging.WithResponseTimeLogging

	h.Router.HandleFunc(remote.PromReadURL, logged(remote.NewPromReadHandler(h.engine)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(remote.PromWriteURL, logged(remote.NewPromWriteHandler(h.storage)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(native.PromReadURL, logged(native.NewPromReadHandler(h.engine)).ServeHTTP).Methods("GET")
	h.Router.HandleFunc(handler.SearchURL, logged(handler.NewSearchHandler(h.storage)).ServeHTTP).Methods("POST")

	h.registerProfileEndpoints()

	if h.clusterClient != nil {
		service, err := placement.Service(h.clusterClient, h.config)
		if err != nil {
			return err
		}

		placement.RegisterRoutes(h.Router, service)

		store, err := h.clusterClient.KV()
		if err != nil {
			return err
		}

		namespace.RegisterRoutes(h.Router, store)
	}

	return nil
}

// Endpoints useful for profiling the service
func (h *Handler) registerProfileEndpoints() {
	h.Router.HandleFunc(pprofURL, pprof.Profile)
}
