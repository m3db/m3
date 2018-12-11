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

package database

import (
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// ConfigGetBootstrappersURL is the url for the database create handler.
	ConfigGetBootstrappersURL = handler.RoutePrefixV1 + "/database/config/bootstrappers"

	// ConfigGetBootstrappersHTTPMethod is the HTTP method used with this resource.
	ConfigGetBootstrappersHTTPMethod = http.MethodGet
)

type configGetBootstrappersHandler struct {
	client clusterclient.Client
}

// NewConfigGetBootstrappersHandler returns a new instance of a database create handler.
func NewConfigGetBootstrappersHandler(
	client clusterclient.Client,
) http.Handler {
	return &configGetBootstrappersHandler{
		client: client,
	}
}

func (h *configGetBootstrappersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	store, err := h.client.KV()
	if err != nil {
		logger.Error("unable to get kv store", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	value, err := store.Get(kvconfig.BootstrapperKey)
	if err != nil {
		logger.Error("unable to get kv key", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	array := new(commonpb.StringArrayProto)
	if err := value.Unmarshal(array); err != nil {
		logger.Error("unable to unmarshal kv key", zap.Any("error", err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	xhttp.WriteProtoMsgJSONResponse(w, array, logger)
}
