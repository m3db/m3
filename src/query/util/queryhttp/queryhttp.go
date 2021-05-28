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

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gorilla/mux"
)

// NewEndpointRegistry returns a new endpoint registry.
func NewEndpointRegistry(
	router *mux.Router,
	middlewareConfig *config.MiddlewareConfiguration,
	instrumentOpts instrument.Options,
) *EndpointRegistry {
	instrumentOpts = instrumentOpts.SetMetricsScope(
		// NB: the double http_handler was accidentally introduced and now we are
		// stuck with it for backwards compatibility.
		instrumentOpts.MetricsScope().SubScope("http_handler_http_handler"))
	return &EndpointRegistry{
		router:             router,
		instrumentOpts:     instrumentOpts,
		middlewareConfig:   middlewareConfig,
		registeredByRoute:  make(map[routeKey]*mux.Route),
		registeredHandlers: make([]RegistryEntry, 0),
	}
}

// EndpointRegistry is an endpoint registry that can register routes
// and instrument them.
type EndpointRegistry struct {
	router             *mux.Router
	instrumentOpts     instrument.Options
	middlewareConfig   *config.MiddlewareConfiguration
	registeredByRoute  map[routeKey]*mux.Route
	registeredHandlers []RegistryEntry
}

// RegistryEntry is an entry in the Registry.
type RegistryEntry struct {
	// Route is the registered Handler.
	Route *mux.Route
	// Middleware is the set of middleware functions to run before the registered Handler.
	Middleware []mux.MiddlewareFunc
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
	Middleware options.RegisterMiddleware
}

// Register registers an endpoint.
func (r *EndpointRegistry) Register(opts RegisterOptions) error {
	route := r.router.NewRoute()
	handler := opts.Handler
	if opts.Middleware == nil {
		opts.Middleware = middleware.Default
	}
	middle := opts.Middleware(options.MiddlewareOptions{
		InstrumentOpts: r.instrumentOpts,
		Route:          route,
		Config:         r.middlewareConfig,
	})
	// Note: middleware is not applied to the handler until after the custom handlers are resolved. this ensures the
	// middleware is runs before the custom handler.

	r.registeredHandlers = append(r.registeredHandlers, RegistryEntry{
		Route:      route,
		Middleware: middle,
	})

	if p := opts.Path; p != "" && len(opts.Methods) > 0 {
		route.Path(p).Handler(handler).Methods(opts.Methods...)
		for _, method := range opts.Methods {
			key := routeKey{
				path:   p,
				method: method,
			}
			if _, ok := r.registeredByRoute[key]; ok {
				return fmt.Errorf("route already exists: path=%s, method=%s", p, method)
			}
			r.registeredByRoute[key] = route
		}
	} else if p := opts.PathPrefix; p != "" {
		key := routeKey{
			pathPrefix: p,
		}
		if _, ok := r.registeredByRoute[key]; ok {
			return fmt.Errorf("route already exists: pathPrefix=%s", p)
		}
		r.registeredByRoute[key] = route.PathPrefix(p).Handler(handler)
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

// Entries returns all registered entries.
func (r *EndpointRegistry) Entries() []RegistryEntry {
	return r.registeredHandlers
}

// PathEntry resolves a registered route that was registered by path and method,
// not by path prefix.
func (r *EndpointRegistry) PathEntry(path, method string) (*mux.Route, bool) {
	key := routeKey{
		path:   path,
		method: method,
	}
	e, ok := r.registeredByRoute[key]
	return e, ok
}

// Walk walks the router and all its sub-routers, calling walkFn for each route
// in the tree. The routes are walked in the order they were added. Sub-routers
// are explored depth-first.
func (r *EndpointRegistry) Walk(walkFn mux.WalkFunc) error {
	return r.router.Walk(walkFn)
}
