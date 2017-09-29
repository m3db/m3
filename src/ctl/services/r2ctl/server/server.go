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

package server

import (
	"net/http"

	"github.com/gorilla/mux"
)

// NewServer creates a new http server for R2.
func NewServer(address string, serverOpts Options, r2Service, healthService Service) *http.Server {
	router := mux.NewRouter()

	r2Router := router.PathPrefix(r2Service.URLPrefix()).Subrouter()
	r2Service.RegisterHandlers(r2Router)

	healthRouter := router.PathPrefix(healthService.URLPrefix()).Subrouter()
	healthService.RegisterHandlers(healthRouter)

	router.NotFoundHandler = http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "ui/build/index.html")

	})
	router.PathPrefix("/public").Handler(http.StripPrefix("/public", http.FileServer(http.Dir("public"))))

	router.PathPrefix("/static").Handler(http.StripPrefix("/static", http.FileServer(http.Dir("ui/build/static"))))

	return &http.Server{
		WriteTimeout: serverOpts.WriteTimeout(),
		ReadTimeout:  serverOpts.ReadTimeout(),
		Addr:         address,
		Handler:      router,
	}
}
