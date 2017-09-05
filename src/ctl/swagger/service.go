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

package swagger

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3ctl/services/r2ctl/server"
	"github.com/m3db/m3x/instrument"

	"github.com/gorilla/mux"
)

const (
	swaggerPrefix = "/swagger"
	swaggerDir    = "public/swagger/"
)

type service struct {
	iOpts      instrument.Options
	apiVersion string
}

// NewService creates a new swagger service.
func NewService(iOpts instrument.Options, apiVersion string) server.Service {
	return &service{iOpts: iOpts, apiVersion: apiVersion}
}

// RegisterHandlers registers rule handlers.
func (s *service) RegisterHandlers(router *mux.Router) {
	log := s.iOpts.Logger()
	swaggerPath := s.fullPath()
	router.PathPrefix(swaggerPath).Handler(
		http.StripPrefix(
			swaggerPath,
			http.FileServer(http.Dir(swaggerDir)),
		),
	)
	log.Infof("Registered swagger endpoints")
}

func (s *service) fullPath() string {
	return fmt.Sprintf("%s/%s", swaggerPrefix, s.apiVersion)
}
