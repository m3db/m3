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

// Package database contains API endpoints for managing the database.
package database

import (
	clusterclient "github.com/m3db/m3/src/cluster/client"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/util/queryhttp"
	"github.com/m3db/m3/src/x/instrument"
)

// Handler represents a generic handler for namespace endpoints.
type Handler struct {
	// This is used by other namespace Handlers
	// nolint: structcheck, megacheck, unused
	client         clusterclient.Client
	instrumentOpts instrument.Options
}

// RegisterRoutes registers the namespace routes
func RegisterRoutes(
	r *queryhttp.EndpointRegistry,
	client clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
	namespaceValidator options.NamespaceValidator,
) error {
	createHandler, err := NewCreateHandler(client, cfg, embeddedDbCfg,
		defaults, instrumentOpts, namespaceValidator)
	if err != nil {
		return err
	}

	kvStoreHandler := NewKeyValueStoreHandler(client, instrumentOpts)

	// Register the same handler under two different endpoints. This just makes explaining things in
	// our documentation easier so we can separate out concepts, but share the underlying code.
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    CreateURL,
		Handler: createHandler,
		Methods: []string{CreateHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    CreateNamespaceURL,
		Handler: createHandler,
		Methods: []string{CreateNamespaceHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    KeyValueStoreURL,
		Handler: kvStoreHandler,
		Methods: []string{KeyValueStoreHTTPMethod},
	}); err != nil {
		return err
	}

	return nil
}
