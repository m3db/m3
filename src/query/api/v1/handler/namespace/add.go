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
	"errors"
	"fmt"
	"net/http"
	"path"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

var (
	// M3DBAddURL is the url for the M3DB namespace add handler.
	M3DBAddURL = path.Join(handler.RoutePrefixV1, M3DBServiceNamespacePathName)

	// AddHTTPMethod is the HTTP method used with this resource.
	AddHTTPMethod = http.MethodPost

	errNamespaceExists = xerrors.NewInvalidParamsError(errors.New("namespace with same ID already exists"))
)

// AddHandler is the handler for namespace adds.
type AddHandler Handler

// NewAddHandler returns a new instance of AddHandler.
func NewAddHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *AddHandler {
	return &AddHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *AddHandler) ServeHTTP(
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
	nsRegistry, err := h.Add(md, opts)
	if err != nil {
		if err == errNamespaceExists {
			logger.Error("namespace already exists", zap.Error(err))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusConflict))
			return
		}

		logger.Error("unable to get namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *AddHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, error) {
	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	addReq := new(admin.NamespaceAddRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), addReq); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return addReq, nil
}

// Add adds a namespace.
func (h *AddHandler) Add(
	addReq *admin.NamespaceAddRequest,
	opts handleroptions.ServiceOptions,
) (nsproto.Registry, error) {
	var emptyReg nsproto.Registry

	md, err := namespace.ToMetadata(addReq.Name, addReq.Options)
	if err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf("bad namespace metadata: %v", err))
	}

	if err := validateNewMetadata(md); err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf("invalid new namespace metadata: %v", err))
	}

	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return emptyReg, err
	}

	currentMetadata, version, err := Metadata(store)
	if err != nil {
		return emptyReg, err
	}

	// Since this endpoint is `/add` and not in-place update, return an error if
	// the NS already exists. NewMap will return an error if there's duplicate
	// entries with the same name, but it's abstracted away behind a MultiError so
	// we can't easily check that it's a conflict in the handler.
	for _, ns := range currentMetadata {
		if ns.ID().Equal(md.ID()) {
			// NB: errNamespaceExists already an invalid params error.
			return emptyReg, errNamespaceExists
		}
	}

	newMDs := append(currentMetadata, md)
	if err = validateNamespaceAggregationOptions(newMDs); err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(err)
	}

	nsMap, err := namespace.NewMap(newMDs)
	if err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(err)
	}

	protoRegistry, err := namespace.ToProto(nsMap)
	if err != nil {
		return emptyReg, fmt.Errorf("error constructing namespace protobuf: %v", err)
	}

	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to add namespace: %v", err)
	}

	return *protoRegistry, nil
}

// Validate new namespace inputs only. Validation that applies to namespaces regardless of create/update/etc
// belongs in the option-specific Validate functions which are invoked on every change operation.
func validateNewMetadata(m namespace.Metadata) error {
	indexBlockSize := m.Options().RetentionOptions().BlockSize()
	retentionBlockSize := m.Options().IndexOptions().BlockSize()
	if indexBlockSize != retentionBlockSize {
		return fmt.Errorf("index and retention block size must match (%v, %v)",
			indexBlockSize,
			retentionBlockSize,
		)
	}
	return nil
}
