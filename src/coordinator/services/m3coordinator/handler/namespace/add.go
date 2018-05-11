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
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"time"

	"github.com/m3db/m3db/src/coordinator/generated/proto/admin"
	"github.com/m3db/m3db/src/coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/m3db/m3cluster/kv"
	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3x/ident"

	"go.uber.org/zap"
)

const (
	// AddURL is the url for the namespace add handler (with the POST method).
	AddURL = "/namespace/add"

	defaultBlockDataExpiryPeriodStr = "5m"
)

var (
	errMissingRequiredField = errors.New("all attributes must be set")
)

type addHandler Handler

// NewAddHandler returns a new instance of a namespace add handler.
func NewAddHandler(store kv.Store) http.Handler {
	return &addHandler{store: store}
}

func (h *addHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		handler.Error(w, rErr.Error(), rErr.Code())
		return
	}

	nsRegistry, err := h.add(req)
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

func (h *addHandler) parseRequest(r *http.Request) (*admin.NamespaceAddRequest, *handler.ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)

	}
	defer r.Body.Close()

	addReq := new(admin.NamespaceAddRequest)
	if err := json.Unmarshal(body, addReq); err != nil {
		return nil, handler.NewParseError(err, http.StatusBadRequest)
	}

	if util.HasEmptyString(addReq.Name, addReq.RetentionPeriod, addReq.BlockSize, addReq.BufferFuture,
		addReq.BufferPast, addReq.BlockDataExpiryPeriod) {
		return nil, handler.NewParseError(errMissingRequiredField, http.StatusBadRequest)
	}

	return addReq, nil
}

func (h *addHandler) add(r *admin.NamespaceAddRequest) (nsproto.Registry, error) {
	var emptyReg = nsproto.Registry{}

	currentMetadata, version, err := Metadata(h.store)
	if err != nil {
		return emptyReg, err
	}

	inputMetadata, err := metadataFromRequest(r)
	if err != nil {
		return emptyReg, err
	}

	nsMap, err := namespace.NewMap(append(currentMetadata, inputMetadata))
	if err != nil {
		return emptyReg, err
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = h.store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return emptyReg, fmt.Errorf("failed to add namespace: %v", err)
	}

	return *protoRegistry, nil
}

func metadataFromRequest(r *admin.NamespaceAddRequest) (namespace.Metadata, error) {
	blockSize, err := time.ParseDuration(r.BlockSize)
	if err != nil {
		return nil, err
	}

	retentionPeriod, err := time.ParseDuration(r.RetentionPeriod)
	if err != nil {
		return nil, err
	}

	bufferFuture, err := time.ParseDuration(r.BufferFuture)
	if err != nil {
		return nil, err
	}

	bufferPast, err := time.ParseDuration(r.BufferPast)
	if err != nil {
		return nil, err
	}

	blockDataExpiryPeriodStr := r.BlockDataExpiryPeriod
	if blockDataExpiryPeriodStr == "" {
		blockDataExpiryPeriodStr = defaultBlockDataExpiryPeriodStr
	}

	blockDataExpiryPeriod, err := time.ParseDuration(blockDataExpiryPeriodStr)
	if err != nil {
		return nil, err
	}

	ropts := retention.NewOptions().
		SetBlockSize(blockSize).
		SetRetentionPeriod(retentionPeriod).
		SetBufferFuture(bufferFuture).
		SetBufferPast(bufferPast).
		SetBlockDataExpiry(r.BlockDataExpiry).
		SetBlockDataExpiryAfterNotAccessedPeriod(blockDataExpiryPeriod)

	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	opts := namespace.NewOptions().
		SetBootstrapEnabled(r.BootstrapEnabled).
		SetFlushEnabled(r.FlushEnabled).
		SetCleanupEnabled(r.CleanupEnabled).
		SetRepairEnabled(r.RepairEnabled).
		SetWritesToCommitLog(r.WritesToCommitlog).
		SetRetentionOptions(ropts)

	return namespace.NewMetadata(ident.StringID(r.Name), opts)
}
