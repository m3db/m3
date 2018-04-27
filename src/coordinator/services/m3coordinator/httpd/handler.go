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
	"github.com/m3db/m3coordinator/services/m3coordinator/config"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler/prometheus/native"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler/prometheus/remote"
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
	config        config.Configuration
	lazyHandlers  lazyLoadHandlers
}

// lazyLoadHandlers are handlers that get activated lazily once M3DB is instantiated
type lazyLoadHandlers struct {
	promRemoteRead  *remote.PromReadHandler
	promRemoteWrite *remote.PromWriteHandler
	promNativeRead  *native.PromReadHandler
	search          *handler.SearchHandler
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
func (h *Handler) RegisterRoutes() {
	logged := withResponseTimeLogging

	promRemoteReadHandler := remote.NewPromReadHandler(h.engine)
	h.lazyHandlers.promRemoteRead = promRemoteReadHandler.(*remote.PromReadHandler)
	h.Router.HandleFunc(remote.PromReadURL, logged(promRemoteReadHandler).ServeHTTP).Methods("POST")

	promRemoteWriteHandler := remote.NewPromWriteHandler(h.storage)
	h.lazyHandlers.promRemoteWrite = promRemoteWriteHandler.(*remote.PromWriteHandler)
	h.Router.HandleFunc(remote.PromWriteURL, logged(promRemoteWriteHandler).ServeHTTP).Methods("POST")

	promNativeReadHandler := native.NewPromReadHandler(h.engine)
	h.lazyHandlers.promNativeRead = promNativeReadHandler.(*native.PromReadHandler)
	h.Router.HandleFunc(native.PromReadURL, logged(promNativeReadHandler).ServeHTTP).Methods("GET")

	searchHandler := handler.NewSearchHandler(h.storage)
	h.lazyHandlers.search = searchHandler.(*handler.SearchHandler)
	h.Router.HandleFunc(handler.SearchURL, logged(searchHandler).ServeHTTP).Methods("POST")

	h.registerProfileEndpoints()

	if h.clusterClient != nil {
		h.Router.HandleFunc(handler.PlacementInitURL, logged(handler.NewPlacementInitHandler(h.clusterClient)).ServeHTTP).Methods("POST")

		h.Router.HandleFunc(handler.PlacementGetURL, logged(handler.NewPlacementGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")
		h.Router.HandleFunc(handler.PlacementGetHTTPMethodURL, logged(handler.NewPlacementGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")

		h.Router.HandleFunc(handler.PlacementDeleteURL, logged(handler.NewPlacementDeleteHandler(h.clusterClient)).ServeHTTP).Methods("POST")
		h.Router.HandleFunc(handler.PlacementDeleteHTTPMethodURL, logged(handler.NewPlacementDeleteHandler(h.clusterClient)).ServeHTTP).Methods("DELETE")

		h.Router.HandleFunc(handler.NamespaceGetURL, logged(handler.NewNamespaceGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")
		h.Router.HandleFunc(handler.NamespaceGetHTTPMethodURL, logged(handler.NewNamespaceGetHandler(h.clusterClient)).ServeHTTP).Methods("GET")

		h.Router.HandleFunc(handler.NamespaceAddURL, logged(handler.NewNamespaceAddHandler(h.clusterClient)).ServeHTTP).Methods("POST")

		h.Router.HandleFunc(handler.NamespaceDeleteURL, logged(handler.NewNamespaceDeleteHandler(h.clusterClient)).ServeHTTP).Methods("POST")
	}
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

// LoadLazyHandlers initializes LazyHandlers post-M3DB setup
func (h *Handler) LoadLazyHandlers(storage storage.Storage, engine *executor.Engine) {
	h.lazyHandlers.promRemoteRead.SetEngine(engine)
	h.lazyHandlers.promRemoteWrite.SetStore(storage)
	h.lazyHandlers.promNativeRead.SetEngine(engine)
	h.lazyHandlers.search.SetStore(storage)
}
