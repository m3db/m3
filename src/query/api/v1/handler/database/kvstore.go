// Copyright (c) 2021 Uber Technologies, Inc.
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

package database

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"
	"google.golang.org/protobuf/runtime/protoiface"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/cluster/generated/proto/kvpb"
	"github.com/m3db/m3/src/cluster/kv"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	// KeyValueStoreURL is the url to edit key/value configuration values.
	KeyValueStoreURL = handler.RoutePrefixV1 + "/kvstore"
	// KeyValueStoreHTTPMethod is the HTTP method used with this resource.
	KeyValueStoreHTTPMethod = http.MethodPost
)

// KeyValueUpdate defines an update to a key's value.
type KeyValueUpdate struct {
	// Key to update.
	Key string `json:"key"`
	// Value to update the key to.
	Value json.RawMessage `json:"value"`
	// Commit, if false, will not persist the update. If true, the
	// update will be persisted. Used to test format of inputs.
	Commit bool `json:"commit"`
}

// KeyValueUpdateResult defines the result of an update to a key's value.
type KeyValueUpdateResult struct {
	// Key to update.
	Key string `json:"key"`
	// Old is the value before the update.
	Old string `json:"old"`
	// New is the value after the update.
	New string `json:"new"`
	// Version of the key.
	Version int `json:"version"`
}

// KeyValueStoreHandler represents a handler for the key/value store endpoint
type KeyValueStoreHandler struct {
	client         clusterclient.Client
	instrumentOpts instrument.Options
}

// NewKeyValueStoreHandler returns a new instance of handler
func NewKeyValueStoreHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) http.Handler {
	return &KeyValueStoreHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *KeyValueStoreHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	logger := logging.WithContext(r.Context(), h.instrumentOpts)

	update, err := h.parseBody(r)
	if err != nil {
		logger.Error("unable to parse request", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	kvStore, err := h.client.KV()
	if err != nil {
		logger.Error("unable to get kv store", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	results, err := h.update(logger, kvStore, update)
	if err != nil {
		logger.Error("kv store error",
			zap.Error(err),
			zap.Any("update", update))
		xhttp.WriteError(w, err)
		return
	}

	xhttp.WriteJSONResponse(w, results, logger)
}

func (h *KeyValueStoreHandler) parseBody(r *http.Request) (*KeyValueUpdate, error) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}
	defer r.Body.Close()

	var parsed KeyValueUpdate
	if err := json.Unmarshal(body, &parsed); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return &parsed, nil
}

func (h *KeyValueStoreHandler) update(
	logger *zap.Logger,
	kvStore kv.Store,
	update *KeyValueUpdate,
) (*KeyValueUpdateResult, error) {
	old, err := kvStore.Get(update.Key)
	if err != nil && !errors.Is(err, kv.ErrNotFound) {
		return nil, err
	}

	oldProto, err := newKVProtoMessage(update.Key)
	if err != nil {
		return nil, err
	}

	if old != nil {
		if err := old.Unmarshal(oldProto); err != nil {
			// Only log so we can overwrite corrupt existing entries.
			logger.Error("cannot unmarshal old kv proto", zap.Error(err), zap.String("key", update.Key))
		}
	}

	newProto, err := newKVProtoMessage(update.Key)
	if err != nil {
		return nil, err
	}

	if err := jsonpb.UnmarshalString(string([]byte(update.Value)), newProto); err != nil {
		return nil, err
	}

	var version int
	if update.Commit {
		version, err = kvStore.Set(update.Key, newProto)
		if err != nil {
			return nil, err
		}
	}

	result := KeyValueUpdateResult{
		Key:     update.Key,
		Old:     oldProto.String(),
		New:     newProto.String(),
		Version: version,
	}

	logger.Info("kv store", zap.Any("update", *update), zap.Any("result", result))

	return &result, nil
}

func newKVProtoMessage(key string) (protoiface.MessageV1, error) {
	switch key {
	case kvconfig.NamespacesKey:
		return &nsproto.Registry{}, nil
	case kvconfig.ClusterNewSeriesInsertLimitKey:
	case kvconfig.EncodersPerBlockLimitKey:
		return &commonpb.Int64Proto{}, nil
	case kvconfig.ClientBootstrapConsistencyLevel:
	case kvconfig.ClientReadConsistencyLevel:
	case kvconfig.ClientWriteConsistencyLevel:
		return &commonpb.StringProto{}, nil
	case kvconfig.QueryLimits:
		return &kvpb.QueryLimits{}, nil
	}
	return nil, fmt.Errorf("unsupported kvstore key %s", key)
}
