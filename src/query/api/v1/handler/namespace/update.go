// Copyright (c) 2020 Uber Technologies, Inc.
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
	"reflect"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
)

var (
	// M3DBUpdateURL is the url for the M3DB namespace update handler.
	M3DBUpdateURL = path.Join(handler.RoutePrefixV1, M3DBServiceNamespacePathName)

	// UpdateHTTPMethod is the HTTP method used with this resource.
	UpdateHTTPMethod = http.MethodPut

	fieldNameRetentionOptions = "RetentionOptions"
	fieldNameRetetionPeriod   = "RetentionPeriodNanos"

	errEmptyNamespaceName      = errors.New("must specify namespace name")
	errEmptyNamespaceOptions   = errors.New("update options cannot be empty")
	errEmptyRetentionOptions   = errors.New("retention options must be set")
	errNamespaceFieldImmutable = errors.New("namespace option field is immutable")
)

// UpdateHandler is the handler for namespace updates.
type UpdateHandler Handler

// NewUpdateHandler returns a new instance of UpdateHandler.
func NewUpdateHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *UpdateHandler {
	return &UpdateHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *UpdateHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)

	md, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	nsRegistry, err := h.Update(md, opts)
	if err != nil {
		logger.Error("unable to update namespace", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *UpdateHandler) parseRequest(r *http.Request) (*admin.NamespaceUpdateRequest, *xhttp.ParseError) {
	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	updateReq := new(admin.NamespaceUpdateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), updateReq); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	if err := validateUpdateRequest(updateReq); err != nil {
		err := fmt.Errorf("unable to validate update request: %w", err)
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return updateReq, nil
}

// Ensure that only fields we allow to be updated (e.g. retention period) are
// non-zero. Uses reflection to be resilient against adding more immutable
// fields to namespaceOptions but forgetting to validate them here.
func validateUpdateRequest(req *admin.NamespaceUpdateRequest) error {
	if req.Name == "" {
		return errEmptyNamespaceName
	}

	if req.Options == nil {
		return errEmptyNamespaceOptions
	}

	if req.Options.RetentionOptions == nil {
		return errEmptyRetentionOptions
	}

	optsVal := reflect.ValueOf(*req.Options)

	for i := 0; i < optsVal.NumField(); i++ {
		field := optsVal.Field(i)
		fieldName := optsVal.Type().Field(i).Name
		if !field.IsZero() && fieldName != fieldNameRetentionOptions {
			return fmt.Errorf("%s: %w", fieldName, errNamespaceFieldImmutable)
		}
	}

	optsVal = reflect.ValueOf(*req.Options.RetentionOptions)
	for i := 0; i < optsVal.NumField(); i++ {
		field := optsVal.Field(i)
		fieldName := optsVal.Type().Field(i).Name
		if !field.IsZero() && fieldName != fieldNameRetetionPeriod {
			return fmt.Errorf("%s.%s: %w", fieldNameRetentionOptions, fieldName, errNamespaceFieldImmutable)
		}
	}

	return nil
}

// Update updates a namespace.
func (h *UpdateHandler) Update(
	updateReq *admin.NamespaceUpdateRequest,
	opts handleroptions.ServiceOptions,
) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}

	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return emptyReg, err
	}

	currentMetadata, version, err := Metadata(store)
	if err != nil {
		return emptyReg, err
	}

	updateID := ident.StringID(updateReq.Name)
	newMDs := make([]namespace.Metadata, 0, len(currentMetadata))
	for _, ns := range currentMetadata {
		// Don't modify any other namespaces.
		if !ns.ID().Equal(updateID) {
			newMDs = append(newMDs, ns)
			continue
		}

		// Replace targeted namespace with modified retention.
		if newNanos := updateReq.Options.RetentionOptions.RetentionPeriodNanos; newNanos != 0 {
			dur := namespace.FromNanos(newNanos)
			opts := ns.Options().SetRetentionOptions(
				ns.Options().RetentionOptions().SetRetentionPeriod(dur),
			)
			newMD, err := namespace.NewMetadata(ns.ID(), opts)
			if err != nil {
				return emptyReg, fmt.Errorf("error constructing new metadata: %w", err)
			}
			newMDs = append(newMDs, newMD)
		} else {
			// If not modifying, keep the original NS.
			newMDs = append(newMDs, ns)
		}
	}

	nsMap, err := namespace.NewMap(newMDs)
	if err != nil {
		return emptyReg, err
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to update namespace: %w", err)
	}

	return *protoRegistry, nil
}
