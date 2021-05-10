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

package queryhttp

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gorilla/mux"
)

// NewEndpointRegistry returns a new endpoint registry.
func NewEndpointRegistry(
	router *mux.Router,
	instrumentOpts instrument.Options,
) *EndpointRegistry {
	return &EndpointRegistry{
		router: router,
		instrumentOpts: instrumentOpts.SetMetricsScope(
			instrumentOpts.MetricsScope().SubScope("http_handler")),
		registered: make(map[routeKey]*mux.Route),
	}
}

// EndpointRegistry is an endpoint registry that can register routes
// and instrument them.
type EndpointRegistry struct {
	router         *mux.Router
	instrumentOpts instrument.Options
	registered     map[routeKey]*mux.Route
}

type routeKey struct {
	path       string
	pathPrefix string
	method     string
}

// RegisterOptions are options for registering a handler.
type RegisterOptions struct {
	Path       string
	PathPrefix string
	Handler    http.Handler
	Methods    []string
	Middleware []mux.MiddlewareFunc
}

// Register registers an endpoint.
func (r *EndpointRegistry) Register(opts RegisterOptions) error {
	if len(opts.Middleware) == 0 {
		opts.Middleware = middleware.Default(r.instrumentOpts)
	}
	handler := opts.Handler
	for i := len(opts.Middleware) - 1; i >= 0; i-- {
		handler = opts.Middleware[i].Middleware(handler)
	}

	if p := opts.Path; p != "" && len(opts.Methods) > 0 {
		for _, method := range opts.Methods {
			key := routeKey{
				path:   p,
				method: method,
			}
			if _, ok := r.registered[key]; ok {
				return fmt.Errorf("route already exists: path=%s, method=%s",
					p, method)
			}

			route := r.router.Handle(p, handler).Methods(method)
			r.registered[key] = route
		}
	} else if p := opts.PathPrefix; p != "" {
		key := routeKey{
			pathPrefix: p,
		}
		if _, ok := r.registered[key]; ok {
			return fmt.Errorf("route already exists: pathPrefix=%s", p)
		}
		route := r.router.PathPrefix(p).Handler(handler)
		r.registered[key] = route
	} else {
		return fmt.Errorf("no path and methods or path prefix set: +%v", opts)
	}

	return nil
}

// RegisterPathsOptions is options for registering multiple paths
// with the same handler.
type RegisterPathsOptions struct {
	Handler http.Handler
	Methods []string
}

// RegisterPaths registers multiple paths for the same handler.
func (r *EndpointRegistry) RegisterPaths(
	paths []string,
	opts RegisterPathsOptions) error {
	for _, p := range paths {
		if err := r.Register(RegisterOptions{
			Path:    p,
			Handler: opts.Handler,
			Methods: opts.Methods,
		}); err != nil {
			return err
		}
	}
	return nil
}

// PathRoute resolves a registered route that was registered by path and method,
// not by path prefix.
func (r *EndpointRegistry) PathRoute(path, method string) (*mux.Route, bool) {
	key := routeKey{
		path:   path,
		method: method,
	}
	h, ok := r.registered[key]
	return h, ok
}

// PathPrefixRoute resolves a registered route that was registered by path
// prefix, not by path and method.
func (r *EndpointRegistry) PathPrefixRoute(pathPrefix string) (*mux.Route, bool) {
	key := routeKey{
		pathPrefix: pathPrefix,
	}
	h, ok := r.registered[key]
	return h, ok
}

// Walk walks the router and all its sub-routers, calling walkFn for each route
// in the tree. The routes are walked in the order they were added. Sub-routers
// are explored depth-first.
func (r *EndpointRegistry) Walk(walkFn mux.WalkFunc) error {
	return r.router.Walk(walkFn)
}
