// Copyright (c) 2019 Uber Technologies, Inc.
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

package topic

import (
	"fmt"
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// DeleteURL is the url for the topic delete handler (with the DELETE method).
	DeleteURL = route.Prefix + "/topic"

	// DeleteHTTPMethod is the HTTP method used with this resource.
	DeleteHTTPMethod = http.MethodDelete
)

// DeleteHandler is the handler for topic adds.
type DeleteHandler Handler

// newDeleteHandler returns a new instance of DeleteHandler.
func newDeleteHandler(
	client clusterclient.Client,
	cfg config.Configuration,
	instrumentOpts instrument.Options,
) http.Handler {
	return &DeleteHandler{
		client:         client,
		cfg:            cfg,
		serviceFn:      Service,
		instrumentOpts: instrumentOpts,
	}
}

func (h *DeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOpts)
	)

	serviceCfg := handleroptions.ServiceNameAndDefaults{}
	svcOpts := handleroptions.NewServiceOptions(serviceCfg, r.Header, nil)
	service, err := h.serviceFn(h.client, svcOpts)
	if err != nil {
		logger.Error("unable to get service", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	name := topicName(r.Header)
	svcLogger := logger.With(zap.String("service", name))
	if err := service.Delete(name); err != nil {
		svcLogger.Error("unable to delete service", zap.Error(err))
		if err == kv.ErrNotFound {
			err = xerrors.NewInvalidParamsError(err)
		}
		err := xerrors.NewRenamedError(err,
			fmt.Errorf("error deleting service '%s': %v", name, err))
		xhttp.WriteError(w, err)
		return
	}

	svcLogger.Info("deleted service")
	// This is technically not necessary but we prefer to be verbose in handler
	// logic.
	w.WriteHeader(http.StatusOK)
}
