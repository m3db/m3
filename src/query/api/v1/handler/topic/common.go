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
	"strings"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
	"github.com/gorilla/mux"
)

const (
	// DefaultTopicName is the default topic name
	DefaultTopicName = "aggregated_metrics"
	// HeaderTopicName is the header used to specify the topic name.
	HeaderTopicName = "topic-name"
)

type serviceFn func(clusterClient clusterclient.Client, opts handleroptions.ServiceOptions) (topic.Service, error)

// Handler represents a generic handler for topic endpoints.
// nolint: structcheck
type Handler struct {
	// This is used by other topic Handlers
	client clusterclient.Client
	cfg    config.Configuration

	serviceFn      serviceFn
	instrumentOpts instrument.Options
}

// Service gets a topic service from m3cluster client
func Service(clusterClient clusterclient.Client, opts handleroptions.ServiceOptions) (topic.Service, error) {
	kvOverride := kv.NewOverrideOptions().
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)
	topicOpts := topic.NewServiceOptions().
		SetConfigService(clusterClient).
		SetKVOverrideOptions(kvOverride)
	return topic.NewService(topicOpts)
}

// RegisterRoutes registers the topic routes
func RegisterRoutes(
	r *mux.Router,
	client clusterclient.Client,
	cfg config.Configuration,
	instrumentOpts instrument.Options,
) {
	wrapped := func(n http.Handler) http.Handler {
		return logging.WithResponseTimeAndPanicErrorLogging(n, instrumentOpts)
	}

	r.HandleFunc(InitURL,
		wrapped(newInitHandler(client, cfg, instrumentOpts)).ServeHTTP).
		Methods(InitHTTPMethod)
	r.HandleFunc(GetURL,
		wrapped(newGetHandler(client, cfg, instrumentOpts)).ServeHTTP).
		Methods(GetHTTPMethod)
	r.HandleFunc(AddURL,
		wrapped(newAddHandler(client, cfg, instrumentOpts)).ServeHTTP).
		Methods(AddHTTPMethod)
	r.HandleFunc(UpdateURL,
		wrapped(newUpdateHandler(client, cfg, instrumentOpts)).ServeHTTP).
		Methods(UpdateHTTPMethod)
	r.HandleFunc(DeleteURL,
		wrapped(newDeleteHandler(client, cfg, instrumentOpts)).ServeHTTP).
		Methods(DeleteHTTPMethod)
}

func topicName(headers http.Header) string {
	if v := strings.TrimSpace(headers.Get(HeaderTopicName)); v != "" {
		return v
	}
	return DefaultTopicName
}

func parseRequest(r *http.Request, m proto.Message) *xhttp.ParseError {
	defer r.Body.Close()

	if err := jsonpb.Unmarshal(r.Body, m); err != nil {
		return xhttp.NewParseError(err, http.StatusBadRequest)
	}
	return nil
}
