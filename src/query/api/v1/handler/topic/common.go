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
	"strings"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3msg/topic"

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

type serviceFn func(clusterClient clusterclient.Client) (topic.Service, error)

// Handler represents a generic handler for topic endpoints.
// nolint: structcheck
type Handler struct {
	// This is used by other topic Handlers
	client clusterclient.Client
	cfg    config.Configuration

	serviceFn serviceFn
}

// Service gets a topic service from m3cluster client
func Service(clusterClient clusterclient.Client) (topic.Service, error) {
	return topic.NewService(
		topic.NewServiceOptions().
			SetConfigService(clusterClient),
	)
}

// RegisterRoutes registers the topic routes
func RegisterRoutes(r *mux.Router, client clusterclient.Client, cfg config.Configuration) {
	logged := logging.WithResponseTimeLogging

	r.HandleFunc(InitURL, logged(NewInitHandler(client, cfg)).ServeHTTP).Methods(InitHTTPMethod)
	r.HandleFunc(GetURL, logged(NewGetHandler(client, cfg)).ServeHTTP).Methods(GetHTTPMethod)
	r.HandleFunc(AddURL, logged(NewAddHandler(client, cfg)).ServeHTTP).Methods(AddHTTPMethod)
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
