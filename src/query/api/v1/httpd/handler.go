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
	_ "net/http/pprof" // needed for pprof handler registration
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/downsample"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/database"
	m3json "github.com/m3db/m3/src/query/api/v1/handler/json"
	"github.com/m3db/m3/src/query/api/v1/handler/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/openapi"
	"github.com/m3db/m3/src/query/api/v1/handler/placement"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/handler/topic"
	"github.com/m3db/m3/src/query/executor"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
	"github.com/uber-go/tally"
)

const (
	healthURL = "/health"
	routesURL = "/routes"
)

var (
	remoteSource = map[string]string{"source": "remote"}
)

// Handler represents an HTTP handler.
type Handler struct {
	Router        *mux.Router
	storage       storage.Storage
	downsampler   downsample.Downsampler
	engine        *executor.Engine
	clusters      m3.Clusters
	clusterClient clusterclient.Client
	config        config.Configuration
	embeddedDbCfg *dbconfig.DBConfiguration
	scope         tally.Scope
	createdAt     time.Time
	tagOptions    models.TagOptions
}

// NewHandler returns a new instance of handler with routes.
func NewHandler(
	storage storage.Storage,
	tagOptions models.TagOptions,
	downsampler downsample.Downsampler,
	engine *executor.Engine,
	m3dbClusters m3.Clusters,
	clusterClient clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	scope tally.Scope,
) (*Handler, error) {
	r := mux.NewRouter()
	h := &Handler{
		Router:        r,
		storage:       storage,
		downsampler:   downsampler,
		engine:        engine,
		clusters:      m3dbClusters,
		clusterClient: clusterClient,
		config:        cfg,
		embeddedDbCfg: embeddedDbCfg,
		scope:         scope,
		createdAt:     time.Now(),
		tagOptions:    tagOptions,
	}
	return h, nil
}

// RegisterRoutes registers all http routes.
func (h *Handler) RegisterRoutes() error {
	logged := logging.WithResponseTimeLogging

	h.Router.HandleFunc(openapi.URL,
		logged(&openapi.DocHandler{}).ServeHTTP,
	).Methods(openapi.HTTPMethod)
	h.Router.PathPrefix(openapi.StaticURLPrefix).Handler(logged(openapi.StaticHandler()))

	// Prometheus remote read/write endpoints
	promRemoteReadHandler := remote.NewPromReadHandler(h.engine, h.scope.Tagged(remoteSource))
	promRemoteWriteHandler, err := remote.NewPromWriteHandler(
		h.storage,
		h.downsampler,
		h.tagOptions,
		h.scope.Tagged(remoteSource),
	)
	if err != nil {
		return err
	}

	h.Router.HandleFunc(remote.PromReadURL,
		logged(promRemoteReadHandler).ServeHTTP,
	).Methods(remote.PromReadHTTPMethod)
	h.Router.HandleFunc(remote.PromWriteURL,
		promRemoteWriteHandler.ServeHTTP,
	).Methods(remote.PromWriteHTTPMethod)
	h.Router.HandleFunc(native.PromReadURL,
		logged(native.NewPromReadHandler(h.engine, h.tagOptions, &h.config.Limits)).ServeHTTP,
	).Methods(native.PromReadHTTPMethod)

	// Native M3 search and write endpoints
	h.Router.HandleFunc(handler.SearchURL,
		logged(handler.NewSearchHandler(h.storage)).ServeHTTP,
	).Methods(handler.SearchHTTPMethod)
	h.Router.HandleFunc(m3json.WriteJSONURL,
		logged(m3json.NewWriteJSONHandler(h.storage)).ServeHTTP,
	).Methods(m3json.JSONWriteHTTPMethod)

	// Tag completion endpoints
	h.Router.HandleFunc(native.CompleteTagsURL,
		logged(native.NewCompleteTagsHandler(h.storage)).ServeHTTP,
	).Methods(native.CompleteTagsHTTPMethod)
	h.Router.HandleFunc(remote.TagValuesURL,
		logged(remote.NewTagValuesHandler(h.storage)).ServeHTTP,
	).Methods(remote.TagValuesHTTPMethod)

	if h.clusterClient != nil {
		placementOpts := placement.HandlerOptions{
			ClusterClient:       h.clusterClient,
			Config:              h.config,
			M3AggServiceOptions: h.m3AggServiceOptions(),
		}

		placement.RegisterRoutes(h.Router, placementOpts)
		namespace.RegisterRoutes(h.Router, h.clusterClient)
		database.RegisterRoutes(h.Router, h.clusterClient, h.config, h.embeddedDbCfg)
		topic.RegisterRoutes(h.Router, h.clusterClient, h.config)
	}

	h.registerHealthEndpoints()
	h.registerProfileEndpoints()
	h.registerRoutesEndpoint()

	return nil
}

func (h *Handler) m3AggServiceOptions() *placement.M3AggServiceOptions {
	if h.clusters == nil {
		return nil
	}

	maxResolution := time.Duration(0)
	for _, ns := range h.clusters.ClusterNamespaces() {
		resolution := ns.Options().Attributes().Resolution
		if resolution > maxResolution {
			maxResolution = resolution
		}
	}

	if maxResolution == 0 {
		return nil
	}

	return &placement.M3AggServiceOptions{
		MaxAggregationWindowSize: maxResolution,
	}
}

// Endpoints useful for profiling the service
func (h *Handler) registerHealthEndpoints() {
	h.Router.HandleFunc(healthURL, func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(struct {
			Uptime string `json:"uptime"`
		}{
			Uptime: time.Since(h.createdAt).String(),
		})
	}).Methods(http.MethodGet)
}

// Endpoints useful for profiling the service
func (h *Handler) registerProfileEndpoints() {
	h.Router.PathPrefix("/debug/pprof/").Handler(http.DefaultServeMux)
}

// Endpoints useful for viewing routes directory
func (h *Handler) registerRoutesEndpoint() {
	h.Router.HandleFunc(routesURL, func(w http.ResponseWriter, r *http.Request) {
		var routes []string
		err := h.Router.Walk(
			func(route *mux.Route, router *mux.Router, ancestors []*mux.Route) error {
				str, err := route.GetPathTemplate()
				if err != nil {
					return err
				}
				routes = append(routes, str)
				return nil
			})
		if err != nil {
			xhttp.Error(w, err, http.StatusInternalServerError)
			return
		}
		json.NewEncoder(w).Encode(struct {
			Routes []string `json:"routes"`
		}{
			Routes: routes,
		})
	}).Methods(http.MethodGet)
}
