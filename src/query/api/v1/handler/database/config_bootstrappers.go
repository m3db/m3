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
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/cluster/kv"

	"github.com/m3db/m3/src/cluster/generated/proto/commonpb"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/dbnode/kvconfig"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/golang/protobuf/jsonpb"
	"go.uber.org/zap"
)

const (
	// ConfigBootstrappersURL is the url for the database create handler.
	ConfigBootstrappersURL = handler.RoutePrefixV1 + "/database/config/bootstrappers"

	// ConfigBootstrappersHTTPMethod is the HTTP method used with this resource.
	ConfigBootstrappersHTTPMethod = http.MethodPost
)

type configBootstrappersHandler struct {
	store kv.Store
}

// NewConfigBootstrappersHandler returns a new instance of a database create handler.
func NewConfigBootstrappersHandler(
	store kv.Store,
) http.Handler {
	return &configBootstrappersHandler{
		store: store,
	}
}

func (h *configBootstrappersHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	value, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Any("error", rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	_, err := h.store.Set(kvconfig.BootstrapperKey, value)
	if err != nil {
		return
	}

	xhttp.WriteProtoMsgJSONResponse(w, value, logger)
}

func (h *configBootstrappersHandler) parseRequest(
	r *http.Request,
) (*commonpb.StringArrayProto, *xhttp.ParseError) {
	array := new(commonpb.StringArrayProto)

	defer r.Body.Close()

	if err := jsonpb.Unmarshal(r.Body, array); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	if len(array.Values) == 0 {
		return nil, xhttp.NewParseError(fmt.Errorf("no values"), http.StatusBadRequest)
	}

	if err := dbconfig.ValidateBootstrappersOrder(array.Values); err != nil {
		return nil, xhttp.NewParseError(err, http.StatusBadRequest)
	}

	return array, nil
}
