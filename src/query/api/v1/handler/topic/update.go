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

package topic

import (
	"net/http"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	pkgerrors "github.com/pkg/errors"
	"go.uber.org/zap"
)

const (
	// UpdateURL is the url for the topic update handler (with the PUT method).
	UpdateURL = route.PrefixV1 + "/topic"

	// UpdateHTTPMethod is the HTTP method used with this resource.
	UpdateHTTPMethod = http.MethodPut
)

// UpdateHandler is the handler for topic updates.
type UpdateHandler Handler

// newUpdateHandler returns a new instance of UpdateHandler. This is used for
// updating a topic in-place, for example to add or remove consumers.
func newUpdateHandler(
	client clusterclient.Client,
	cfg config.Configuration,
	instrumentOpts instrument.Options,
) http.Handler {
	return &UpdateHandler{
		client:         client,
		cfg:            cfg,
		serviceFn:      Service,
		instrumentOpts: instrumentOpts,
	}
}

func (h *UpdateHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var (
		ctx    = r.Context()
		logger = logging.WithContext(ctx, h.instrumentOpts)
		req    admin.TopicUpdateRequest
	)

	if rErr := parseRequest(r, &req); rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}

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
	m3Topic, err := service.Get(name)
	if err != nil {
		logger.Error("unable to get topic", zap.Error(err))
		xhttp.WriteError(w, xhttp.NewError(err, http.StatusNotFound))
		return
	}

	oldConsumers := len(m3Topic.ConsumerServices())
	newConsumers := len(req.ConsumerServices)

	csvcs := make([]topic.ConsumerService, 0, newConsumers)
	for _, svc := range req.ConsumerServices {
		csvc, err := topic.NewConsumerServiceFromProto(svc)
		if err != nil {
			err := pkgerrors.WithMessagef(err, "error converting consumer service '%s'", svc.String())
			svcLogger.Error("convert consumer service error", zap.Error(err))
			xhttp.WriteError(w, xhttp.NewError(err, http.StatusBadRequest))
			return
		}

		csvcs = append(csvcs, csvc)
	}

	m3Topic = m3Topic.SetConsumerServices(csvcs)
	newTopic, err := service.CheckAndSet(m3Topic, int(req.Version))
	if err != nil {
		svcLogger.Error("unable to delete service", zap.Error(err))
		err := pkgerrors.WithMessagef(err, "error deleting service '%s'", name)
		xhttp.WriteError(w, err)
		return
	}

	svcLogger.Info("updated service in-place", zap.Int("oldConsumers", oldConsumers), zap.Int("newConsumers", newConsumers))

	pb, err := topic.ToProto(m3Topic)
	if err != nil {
		logger.Error("unable to convert topic to protobuf", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.TopicGetResponse{
		Topic:   pb,
		Version: uint32(newTopic.Version()),
	}
	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}
