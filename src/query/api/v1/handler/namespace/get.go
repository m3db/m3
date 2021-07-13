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
	"path"
	"strconv"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/golang/protobuf/proto"
	"go.uber.org/zap"
)

var (
	// M3DBGetURL is the url for the namespace get handler (with the GET method).
	M3DBGetURL = path.Join(route.Prefix, M3DBServiceNamespacePathName)

	// GetHTTPMethod is the HTTP method used with this resource.
	GetHTTPMethod = http.MethodGet
)

const (
	debugParam = "debug"
)

// GetHandler is the handler for namespace gets.
type GetHandler Handler

// NewGetHandler returns a new instance of GetHandler.
func NewGetHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *GetHandler {
	return &GetHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *GetHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)
	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	nsRegistry, err := h.Get(opts)

	if err != nil {
		logger.Error("unable to get namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	if debug, err := strconv.ParseBool(r.URL.Query().Get(debugParam)); err == nil && debug {
		nanosToDurationMap, err := nanosToDuration(resp)
		if err != nil {
			logger.Error("error converting nano fields to duration", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}

		xhttp.WriteJSONResponse(w, nanosToDurationMap, logger)
		return
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

// Get gets the namespaces.
func (h *GetHandler) Get(opts handleroptions.ServiceOptions) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}

	store, err := h.client.Store(opts.KVOverrideOptions())
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

func nanosToDuration(resp proto.Message) (map[string]interface{}, error) {
	marshaler := jsonpb.Marshaler{EmitDefaults: true}
	buf := new(bytes.Buffer)
	marshaler.Marshal(buf, resp)

	toDuration, err := xhttp.NanosToDurationBytes(buf)
	if err != nil {
		return nil, err
	}

	return toDuration, nil
}
