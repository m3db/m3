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
	"net/http"
	"net/http/pprof"
	"os"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/storage"
	"github.com/m3db/m3coordinator/util/logging"

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
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(storage storage.Storage, engine *executor.Engine, clusterClient m3clusterClient.Client) (*Handler, error) {
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
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging
	h.Router.HandleFunc(handler.PromReadURL, logged(handler.NewPromReadHandler(h.engine)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(handler.PromWriteURL, logged(handler.NewPromWriteHandler(h.storage)).ServeHTTP).Methods("POST")
	h.Router.HandleFunc(handler.SearchURL, logged(handler.NewSearchHandler(h.storage)).ServeHTTP).Methods("POST")

	if h.clusterClient != nil {
		h.Router.HandleFunc(handler.PlacementInitURL, logged(handler.NewPlacementInitHandler(h.clusterClient)).ServeHTTP).Methods("POST")

		h.Router.HandleFunc(handler.PlacementGetURL, logged(handler.NewPlacementGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")
		h.Router.HandleFunc(handler.PlacementGetHTTPMethodURL, logged(handler.NewPlacementGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")

		h.Router.HandleFunc(handler.PlacementDeleteURL, logged(handler.NewPlacementDeleteHandler(h.clusterClient)).ServeHTTP).Methods("POST")
		h.Router.HandleFunc(handler.PlacementDeleteHTTPMethodURL, logged(handler.NewPlacementDeleteHandler(h.clusterClient)).ServeHTTP).Methods("DELETE")
	}

	h.registerProfileEndpoints()
}

// Endpoints useful for profiling the service
func (h *Handler) registerProfileEndpoints() {
	h.Router.HandleFunc(pprofURL, pprof.Profile)
}

func withResponseTimeLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		startTime := time.Now()
		rqCtx := logging.NewContextWithGeneratedID(r.Context())
		logger := logging.WithContext(rqCtx)

		// Propagate the context with the reqId
		next.ServeHTTP(w, r.WithContext(rqCtx))
		endTime := time.Now()
		d := endTime.Sub(startTime)
		if d > time.Second {
			logger.Info("finished handling request", zap.Time("time", endTime), zap.Duration("response", d), zap.String("url", r.URL.RequestURI()))
		}
	})
}
