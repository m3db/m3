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

package handler

import (
	"context"
	"fmt"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"go.uber.org/zap"
)

const (
	// NamespaceGetURL is the url for the placement get handler (with the GET method).
	NamespaceGetURL = "/namespace/get"

	// NamespaceGetHTTPMethodURL is the url for the placement get handler (with the GET method).
	NamespaceGetHTTPMethodURL = "/namespace"
)

// namespaceGetHandler represents a handler for placement get endpoint.
type namespaceGetHandler AdminHandler

// NewNamespaceGetHandler returns a new instance of handler.
func NewNamespaceGetHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &namespaceGetHandler{
		clusterClient: clusterClient,
	}
}

func (h *namespaceGetHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	nsRegistry, err := h.namespaceGet(ctx)

	if err != nil {
		logger.Error("unable to get namespace", zap.Any("error", err))
		Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *namespaceGetHandler) namespaceGet(ctx context.Context) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}
	store, err := h.clusterClient.KV()
	if err != nil {
		return emptyReg, err
	}

	value, err := store.Get(M3DBNodeNamespacesKey)
	if err == kv.ErrNotFound {
		// Having no namespace should not be treated as an error
		return emptyReg, nil
	} else if err != nil {
		return emptyReg, err
	}

	var protoRegistry nsproto.Registry

	if err := value.Unmarshal(&protoRegistry); err != nil {
		return emptyReg, fmt.Errorf("failed to parse namespace version %v: %v", value.Version(), err)
	}
	return protoRegistry, nil
}
