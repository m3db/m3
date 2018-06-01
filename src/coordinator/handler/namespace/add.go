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
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"
	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"

	"go.uber.org/zap"
)

const (
	// AddURL is the url for the namespace add handler (with the POST method).
	AddURL = handler.RoutePrefixV1 + "/namespace"
)

type addHandler Handler

// NewAddHandler returns a new instance of a namespace add handler.
func NewAddHandler(client clusterclient.Client) http.Handler {
	return &addHandler{client: client}
}

func (h *addHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	md, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	nsRegistry, err := h.add(md)
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

func (h *addHandler) parseRequest(r *http.Request) (namespace.Metadata, *handler.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)

	}
	defer r.Body.Close()

	addReq := new(admin.NamespaceAddRequest)
	if err := json.Unmarshal(body, addReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	md, err := namespace.ToMetadata(addReq.Name, addReq.Options)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return md, nil
}

func (h *addHandler) add(md namespace.Metadata) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}

	store, err := h.client.KV()
	if err != nil {
		return emptyReg, err
	}

	currentMetadata, version, err := Metadata(store)
	if err != nil {
		return emptyReg, err
	}

	nsMap, err := namespace.NewMap(append(currentMetadata, md))
	if err != nil {
		return emptyReg, err
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to add namespace: %v", err)
	}

	return *protoRegistry, nil
}
