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

package database

import (
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/x/instrument"
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	// This is used by other namespace Handlers
	// nolint: structcheck, megacheck, unused
	client         clusterclient.Client
	instrumentOpts instrument.Options
}

type AddRouteFn func(path string, handler http.Handler, methods ...string)

// RegisterRoutes registers the namespace routes
func RegisterRoutes(
	addRoute AddRouteFn,
	client clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
) error {
	createHandler, err := NewCreateHandler(client, cfg, embeddedDbCfg,
		defaults, instrumentOpts)
	if err != nil {
		return err
	}

	// Register the same handler under two different endpoints. This just makes explaining things in
	// our documentation easier so we can separate out concepts, but share the underlying code.
	addRoute(CreateURL, createHandler, CreateHTTPMethod)
	addRoute(CreateNamespaceURL, createHandler, CreateNamespaceHTTPMethod)

	return nil
}
