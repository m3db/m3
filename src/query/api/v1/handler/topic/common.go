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
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/msg/topic"
	"github.com/m3db/m3/src/query/util/queryhttp"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/gogo/protobuf/jsonpb"
	"github.com/gogo/protobuf/proto"
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
	r *queryhttp.EndpointRegistry,
	client clusterclient.Client,
	cfg config.Configuration,
	instrumentOpts instrument.Options,
) error {
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    InitURL,
		Handler: newInitHandler(client, cfg, instrumentOpts),
		Methods: []string{InitHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    GetURL,
		Handler: newGetHandler(client, cfg, instrumentOpts),
		Methods: []string{GetHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    AddURL,
		Handler: newAddHandler(client, cfg, instrumentOpts),
		Methods: []string{AddHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    UpdateURL,
		Handler: newUpdateHandler(client, cfg, instrumentOpts),
		Methods: []string{UpdateHTTPMethod},
	}); err != nil {
		return err
	}
	if err := r.Register(queryhttp.RegisterOptions{
		Path:    DeleteURL,
		Handler: newDeleteHandler(client, cfg, instrumentOpts),
		Methods: []string{DeleteHTTPMethod},
	}); err != nil {
		return err
	}
	return nil
}

func topicName(headers http.Header) string {
	if v := strings.TrimSpace(headers.Get(HeaderTopicName)); v != "" {
		return v
	}
	return DefaultTopicName
}

func parseRequest(r *http.Request, m proto.Message) error {
	defer r.Body.Close()

	if err := jsonpb.Unmarshal(r.Body, m); err != nil {
		return xerrors.NewInvalidParamsError(err)
	}

	return nil
}
