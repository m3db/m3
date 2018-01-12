// Copyright (c) 2017 Uber Technologies, Inc.
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

package http

import (
	"net/http"
	"sync"

	mserver "github.com/m3db/m3ctl/server"
	"github.com/m3db/m3ctl/service"
	"github.com/m3db/m3x/log"

	"github.com/gorilla/mux"
)

const (
	publicPathPrefix = "/public"
	publicDirPath    = "public"
	staticPathPrefix = "/static"
	staticDirPath    = "ui/build/static"
	indexFilePath    = "ui/build/index.html"
)

type server struct {
	server   *http.Server
	services []service.Service
	logger   log.Logger
	wg       sync.WaitGroup
}

// NewServer creates a new HTTP server.
func NewServer(address string, opts Options, services ...service.Service) (mserver.Server, error) {
	// Make a copy of the services passed in so they cannot be mutated externally
	// once the server is constructed.
	cloned := make([]service.Service, len(services))
	copy(cloned, services)
	handler, err := initRouter(cloned)
	if err != nil {
		return nil, err
	}
	s := &http.Server{
		Addr:         address,
		Handler:      handler,
		ReadTimeout:  opts.ReadTimeout(),
		WriteTimeout: opts.WriteTimeout(),
	}
	return &server{
		server:   s,
		services: cloned,
		logger:   opts.InstrumentOptions().Logger(),
	}, nil
}

func (s *server) ListenAndServe() error {
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		if err := s.server.ListenAndServe(); err != nil {
			s.logger.Errorf("could not start listening and serving traffic: %v", err)
		}
	}()
	return nil
}

func (s *server) Close() {
	s.server.Close()
	s.wg.Wait()
	for _, service := range s.services {
		service.Close()
	}
}

func initRouter(services []service.Service) (http.Handler, error) {
	router := mux.NewRouter()
	registerStaticRoutes(router)
	if err := registerServiceRoutes(router, services); err != nil {
		return nil, err
	}
	return router, nil
}

func registerStaticRoutes(router *mux.Router) {
	// Register public handler.
	publicHandler := http.StripPrefix(publicPathPrefix, http.FileServer(http.Dir(publicDirPath)))
	router.PathPrefix(publicPathPrefix).Handler(publicHandler)

	// Register static handler.
	staticHandler := http.StripPrefix(staticPathPrefix, http.FileServer(http.Dir(staticDirPath)))
	router.PathPrefix(staticPathPrefix).Handler(staticHandler)

	// Register not found handler.
	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, indexFilePath)
	})
}

func registerServiceRoutes(router *mux.Router, services []service.Service) error {
	for _, service := range services {
		pathPrefix := service.URLPrefix()
		subRouter := router.PathPrefix(pathPrefix).Subrouter()
		if err := service.RegisterHandlers(subRouter); err != nil {
			return err
		}
	}
	return nil
}
