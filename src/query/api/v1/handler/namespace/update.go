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
	xerrors "github.com/m3db/m3/src/x/errors"
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

	fieldNameRetentionOptions   = "RetentionOptions"
	fieldNameRetentionPeriod    = "RetentionPeriodNanos"
	fieldNameRuntimeOptions     = "RuntimeOptions"
	fieldNameAggregationOptions = "AggregationOptions"

	errEmptyNamespaceName      = errors.New("must specify namespace name")
	errEmptyNamespaceOptions   = errors.New("update options cannot be empty")
	errNamespaceFieldImmutable = errors.New("namespace option field is immutable")

	allowedUpdateOptionsFields = map[string]struct{}{
		fieldNameRetentionOptions:   {},
		fieldNameRuntimeOptions:     {},
		fieldNameAggregationOptions: {},
	}
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
		logger.Warn("unable to parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	nsRegistry, err := h.Update(md, opts)
	if err != nil {
		logger.Error("unable to update namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.NamespaceGetResponse{
		Registry: &nsRegistry,
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *UpdateHandler) parseRequest(r *http.Request) (*admin.NamespaceUpdateRequest, error) {
	defer r.Body.Close()
	rBody, err := xhttp.DurationToNanosBytes(r.Body)
	if err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	updateReq := new(admin.NamespaceUpdateRequest)
	if err := jsonpb.Unmarshal(bytes.NewReader(rBody), updateReq); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	if err := validateUpdateRequest(updateReq); err != nil {
		err := fmt.Errorf("unable to validate update request: %w", err)
		return nil, xerrors.NewInvalidParamsError(err)
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

	optsVal := reflect.ValueOf(*req.Options)
	allNonZeroFields := true
	for i := 0; i < optsVal.NumField(); i++ {
		field := optsVal.Field(i)
		fieldName := optsVal.Type().Field(i).Name
		if field.IsZero() {
			continue
		}

		allNonZeroFields = false

		_, ok := allowedUpdateOptionsFields[fieldName]
		if !ok {
			return fmt.Errorf("%s: %w", fieldName, errNamespaceFieldImmutable)
		}
	}

	if allNonZeroFields {
		return errEmptyNamespaceOptions
	}

	if opts := req.Options.RetentionOptions; opts != nil {
		optsVal := reflect.ValueOf(*opts)
		for i := 0; i < optsVal.NumField(); i++ {
			field := optsVal.Field(i)
			fieldName := optsVal.Type().Field(i).Name
			if !field.IsZero() && fieldName != fieldNameRetentionPeriod {
				return fmt.Errorf("%s.%s: %w", fieldNameRetentionOptions, fieldName, errNamespaceFieldImmutable)
			}
		}
	}

	return nil
}

// Update updates a namespace.
func (h *UpdateHandler) Update(
	updateReq *admin.NamespaceUpdateRequest,
	opts handleroptions.ServiceOptions,
) (nsproto.Registry, error) {
	var emptyReg nsproto.Registry

	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return emptyReg, err
	}

	currentMetadata, version, err := Metadata(store)
	if err != nil {
		return emptyReg, err
	}

	newMetadata := make(map[string]namespace.Metadata)
	for _, ns := range currentMetadata {
		newMetadata[ns.ID().String()] = ns
	}

	ns, ok := newMetadata[updateReq.Name]
	if !ok {
		return emptyReg, xhttp.NewError(
			fmt.Errorf("namespace not found: err=%s", updateReq.Name),
			http.StatusNotFound)
	}

	// Replace targeted namespace with modified retention.
	if newRetentionOpts := updateReq.Options.RetentionOptions; newRetentionOpts != nil {
		if newNanos := newRetentionOpts.RetentionPeriodNanos; newNanos != 0 {
			dur := namespace.FromNanos(newNanos)
			retentionOpts := ns.Options().RetentionOptions().
				SetRetentionPeriod(dur)
			opts := ns.Options().
				SetRetentionOptions(retentionOpts)
			ns, err = namespace.NewMetadata(ns.ID(), opts)
			if err != nil {
				return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf(
					"error constructing new metadata: %w", err))
			}
		}
	}

	// Update runtime options.
	if newRuntimeOpts := updateReq.Options.RuntimeOptions; newRuntimeOpts != nil {
		runtimeOpts := ns.Options().RuntimeOptions()
		if v := newRuntimeOpts.WriteIndexingPerCPUConcurrency; v != nil {
			runtimeOpts = runtimeOpts.SetWriteIndexingPerCPUConcurrency(&v.Value)
		}
		if v := newRuntimeOpts.FlushIndexingPerCPUConcurrency; v != nil {
			runtimeOpts = runtimeOpts.SetFlushIndexingPerCPUConcurrency(&v.Value)
		}
		opts := ns.Options().
			SetRuntimeOptions(runtimeOpts)
		ns, err = namespace.NewMetadata(ns.ID(), opts)
		if err != nil {
			return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf(
				"error constructing new metadata: %w", err))
		}
	}

	// Update extended options.
	if newExtendedOptions := updateReq.Options.ExtendedOptions; newExtendedOptions != nil {
		newExtOpts, err := namespace.ToExtendedOptions(newExtendedOptions)
		if err != nil {
			return emptyReg, xerrors.NewInvalidParamsError(err)
		}
		opts := ns.Options().SetExtendedOptions(newExtOpts)
		ns, err = namespace.NewMetadata(ns.ID(), opts)
		if err != nil {
			return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf(
				"error constructing new metadata: %w", err))
		}
	}
	if protoAggOpts := updateReq.Options.AggregationOptions; protoAggOpts != nil {
		newAggOpts, err := namespace.ToAggregationOptions(protoAggOpts)
		if err != nil {
			return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf(
				"error constructing construction aggregationOptions: %w", err))
		}
		if !ns.Options().AggregationOptions().Equal(newAggOpts) {
			opts := ns.Options().SetAggregationOptions(newAggOpts)
			ns, err = namespace.NewMetadata(ns.ID(), opts)
			if err != nil {
				return emptyReg, xerrors.NewInvalidParamsError(fmt.Errorf(
					"error constructing new metadata: %w", err))
			}
		}
	}

	// Update the namespace in case an update occurred.
	newMetadata[updateReq.Name] = ns

	// Set the new slice and update.
	newMDs := make([]namespace.Metadata, 0, len(newMetadata))
	for _, elem := range newMetadata {
		newMDs = append(newMDs, elem)
	}

	if err = validateNamespaceAggregationOptions(newMDs); err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(err)
	}

	nsMap, err := namespace.NewMap(newMDs)
	if err != nil {
		return emptyReg, xerrors.NewInvalidParamsError(err)
	}

	protoRegistry, err := namespace.ToProto(nsMap)
	if err != nil {
		return emptyReg, fmt.Errorf("error constructing namespace protobuf: %w", err)
	}

	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to update namespace: %w", err)
	}

	return *protoRegistry, nil
}
