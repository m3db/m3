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

package topic

import (
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"go.uber.org/zap"
)

const (
	// InitURL is the url for the topic init handler (with the POST method).
	InitURL = handler.RoutePrefixV1 + "/topic/init"

	// InitHTTPMethod is the HTTP method used with this resource.
	InitHTTPMethod = http.MethodPost
)

// InitHandler is the handler for topic inits.
type InitHandler Handler

// newInitHandler returns a new instance of InitHandler.
func newInitHandler(
	client clusterclient.Client,
	cfg config.Configuration,
	instrumentOpts instrument.Options,
) http.Handler {
	return &InitHandler{
		client:         client,
		cfg:            cfg,
		serviceFn:      Service,
		instrumentOpts: instrumentOpts,
	}
}

func (h *InitHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOpts)
		req    admin.TopicInitRequest
	)
	rErr := parseRequest(r, &req)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.Error(w, rErr.Inner(), rErr.Code())
		return
	}

	serviceCfg := handleroptions.ServiceNameAndDefaults{}
	svcOpts := handleroptions.NewServiceOptions(serviceCfg, r.Header, nil)
	service, err := h.serviceFn(h.client, svcOpts)
	if err != nil {
		logger.Error("unable to get service", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	t := topic.NewTopic().
		SetName(topicName(r.Header)).
		SetNumberOfShards(req.NumberOfShards)
	t, err = service.CheckAndSet(t, 0)
	if err != nil {
		logger.Error("unable to init topic", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	topicProto, err := topic.ToProto(t)
	if err != nil {
		logger.Error("unable to get topic protobuf", zap.Error(err))
		xhttp.Error(w, err, http.StatusInternalServerError)
		return
	}

	resp := &admin.TopicGetResponse{
		Topic:   topicProto,
		Version: uint32(t.Version()),
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}
