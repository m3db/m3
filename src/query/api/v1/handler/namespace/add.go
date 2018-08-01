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
	"bytes"
	"fmt"
	"net/http"

	clusterclient "github.com/m3db/m3cluster/client"
	nsproto "github.com/m3db/m3db/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3db/src/dbnode/storage/namespace"
	"github.com/m3db/m3db/src/query/api/v1/handler"
	"github.com/m3db/m3db/src/query/generated/proto/admin"
	"github.com/m3db/m3db/src/query/util/logging"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// AddURL is the url for the namespace add handler.
	AddURL = handler.RoutePrefixV1 + "/namespace"

	// AddHTTPMethod is the HTTP method used with this resource.
	AddHTTPMethod = http.MethodPost
)

// AddHandler is the handler for namespace adds.
type AddHandler Handler

// NewAddHandler returns a new instance of AddHandler.
func NewAddHandler(client clusterclient.Client) *AddHandler {
	return &AddHandler{client: client}
}

func (h *AddHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	md, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		handler.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	nsRegistry, err := h.Add(md)
	if err != nil {
		logger.Error("unable to get namespace", zap.Any("error", err))
		handler.Error(w, err, http.StatusBadRequest)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	handler.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *AddHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *handler.ParseError) {
	defer r.Body.Close()
	rBody, err := handler.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	addReq := new(admin.NamespaceAddRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), addReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	return addReq, nil
}

// Add adds a namespace.
func (h *AddHandler) Add(addReq *admin.NamespaceAddRequest) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}

	md, err := namespace.ToMetadata(addReq.Name, addReq.Options)
	if err != nil {
		return emptyReg, fmt.Errorf("unable to get metadata: %v", err)
	}

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
