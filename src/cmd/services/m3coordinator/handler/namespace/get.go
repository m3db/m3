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

package namespace

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"

	"go.uber.org/zap"
)

const (
	// GetURL is the url for the namespace get handler (with the GET method).
	GetURL = "/namespace"
)

type getHandler Handler

// NewGetHandler returns a new instance of a namespace get handler.
func NewGetHandler(store kv.Store) http.Handler {
	return &getHandler{store: store}
}

func (h *getHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	nsRegistry, err := h.get()

	if err != nil {
		logger.Error("unable to get namespace", zap.Any("error", err))
		handler.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *getHandler) get() (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}
	value, err := h.store.Get(M3DBNodeNamespacesKey)

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
