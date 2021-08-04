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
	"path"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/dbnode/namespace/kvadmin"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

var (
	// M3DBSchemaURL is the url for the M3DB schema handler.
	M3DBSchemaURL = path.Join(route.Prefix, M3DBServiceSchemaPathName)

	// SchemaDeployHTTPMethod is the HTTP method used to append to this resource.
	SchemaDeployHTTPMethod = http.MethodPost
)

// SchemaHandler is the handler for namespace schema upserts.
type SchemaHandler Handler

// For unit test purpose.
var newAdminService = kvadmin.NewAdminService

// NewSchemaHandler returns a new instance of SchemaHandler.
func NewSchemaHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *SchemaHandler {
	return &SchemaHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *SchemaHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)

	md, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	resp, err := h.Add(md, opts)
	if err != nil {
		if err == kv.ErrNotFound || xerrors.InnerError(err) == kv.ErrNotFound {
			logger.Error("namespaces metadata key does not exist", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
		if err == kvadmin.ErrNamespaceNotFound || xerrors.InnerError(err) == kvadmin.ErrNamespaceNotFound {
			logger.Error("namespace does not exist", zap.Error(err))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
			return
		}

		logger.Error("unable to deploy schema to namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	xhttp.WriteProtoMsgJSONResponse(w, &resp, logger)
}

func (h *SchemaHandler) parseRequest(r *http.Request) (*admin.NamespaceSchemaAddRequest, error) {
	defer r.Body.Close()

	var schemaAddReq admin.NamespaceSchemaAddRequest
	if err := jsonpb.Unmarshal(r.Body, &schemaAddReq); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}
	return &schemaAddReq, nil
}

// Add adds schema to an existing namespace.
func (h *SchemaHandler) Add(
	addReq *admin.NamespaceSchemaAddRequest,
	opts handleroptions.ServiceOptions,
) (admin.NamespaceSchemaAddResponse, error) {
	var emptyRep = admin.NamespaceSchemaAddResponse{}

	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return emptyRep, err
	}

	schemaAdmin := newAdminService(store, M3DBNodeNamespacesKey, nil)
	deployID, err := schemaAdmin.DeploySchema(addReq.Name, addReq.ProtoName, addReq.MsgName, addReq.ProtoMap)
	if err != nil {
		return emptyRep, err
	}
	return admin.NamespaceSchemaAddResponse{DeployID: deployID}, nil
}

// SchemaResetHandler is the handler for namespace schema reset.
type SchemaResetHandler Handler

// NewSchemaResetHandler returns a new instance of SchemaHandler.
func NewSchemaResetHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *SchemaResetHandler {
	return &SchemaResetHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *SchemaResetHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)

	md, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	resp, err := h.Reset(md, opts)
	if err != nil {
		if err == kv.ErrNotFound || xerrors.InnerError(err) == kv.ErrNotFound {
			logger.Error("namespaces metadata key does not exist", zap.Error(err))
			xhttp.WriteError(w, err)
			return
		}
		if err == kvadmin.ErrNamespaceNotFound || xerrors.InnerError(err) == kvadmin.ErrNamespaceNotFound {
			logger.Error("namespace does not exist", zap.Error(err))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
			return
		}

		logger.Error("unable to reset schema for namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *SchemaResetHandler) parseRequest(r *http.Request) (*admin.NamespaceSchemaResetRequest, error) {
	defer r.Body.Close()

	var schemaResetReq admin.NamespaceSchemaResetRequest
	if err := jsonpb.Unmarshal(r.Body, &schemaResetReq); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}
	return &schemaResetReq, nil
}

// Reset resets schema for an existing namespace.
func (h *SchemaResetHandler) Reset(
	addReq *admin.NamespaceSchemaResetRequest,
	opts handleroptions.ServiceOptions,
) (*admin.NamespaceSchemaResetResponse, error) {
	var emptyRep = admin.NamespaceSchemaResetResponse{}
	if !opts.Force {
		err := fmt.Errorf("CAUTION! Reset schema will prevent proto-enabled namespace from loading, proceed if you know what you are doing, please retry with force set to true")
		return &emptyRep, xerrors.NewInvalidParamsError(err)
	}

	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return &emptyRep, err
	}

	schemaAdmin := newAdminService(store, M3DBNodeNamespacesKey, nil)
	err = schemaAdmin.ResetSchema(addReq.Name)
	if err != nil {
		return &emptyRep, err
	}
	return &emptyRep, nil
}
