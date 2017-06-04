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

package rules

import (
	"net/http"

	"github.com/m3db/m3ctl/handler"
	"github.com/m3db/m3x/instrument"

	"github.com/gorilla/mux"
)

const (
	readNamespacesURL  = "/rules/namespaces"
	createNamespaceURL = "/rules/namespaces"
	deleteNamespaceURL = "/rules/namespace/{name}"
	rulesURL           = "/rules/namespace/{name}"
)

// Controller sets up the handlers for rules
type controller struct {
	iOpts instrument.Options
}

// NewController creates a new rules controller
func NewController(iOpts instrument.Options) handler.Controller {
	return &controller{iOpts: iOpts}
}

// RegisterHandlers registers rule handlers
func (c *controller) RegisterHandlers(router *mux.Router) {
	log := c.iOpts.Logger()
	router.HandleFunc(readNamespacesURL, c.getNamespacesHandler).Methods(http.MethodGet)
	router.HandleFunc(createNamespaceURL, c.createNamespaceHandler).Methods(http.MethodPost)
	router.HandleFunc(deleteNamespaceURL, c.deleteNamespaceHandler).Methods(http.MethodDelete)
	router.HandleFunc(rulesURL, c.getRulesInNamespaceHandler).Methods(http.MethodGet)
	router.HandleFunc(rulesURL, c.setRulesInNamespaceHandler).Methods(http.MethodPut, http.MethodPatch)
	log.Infof("Registered rules endpoints")
}
