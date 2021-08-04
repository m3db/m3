// Copyright (c) 2020  Uber Technologies, Inc.
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
	"context"
	"fmt"
	"net/http"
	"path"
	"time"

	"github.com/gogo/protobuf/jsonpb"
	"go.uber.org/zap"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/dbnode/client"
	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/dbnode/storage/index"
	"github.com/m3db/m3/src/m3ninx/idx"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/generated/proto/admin"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/ident"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"
)

const (
	defaultReadyContextTimeout = 10 * time.Second
)

var (
	// M3DBReadyURL is the url for the M3DB namespace mark_ready handler.
	M3DBReadyURL = path.Join(route.Prefix, M3DBServiceNamespacePathName, "ready")

	// ReadyHTTPMethod is the HTTP method used with this resource.
	ReadyHTTPMethod = http.MethodPost
)

// ReadyHandler is the handler for marking namespaces ready.
type ReadyHandler Handler

// NewReadyHandler returns a new instance of ReadyHandler.
func NewReadyHandler(
	client clusterclient.Client,
	clusters m3.Clusters,
	instrumentOpts instrument.Options,
) *ReadyHandler {
	return &ReadyHandler{
		client:         client,
		clusters:       clusters,
		instrumentOpts: instrumentOpts,
	}
}

func (h *ReadyHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	if _, ok := ctx.Deadline(); !ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, defaultReadyContextTimeout)
		defer cancel()
	}

	logger := logging.WithContext(ctx, h.instrumentOpts)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		logger.Error("unable to parse request", zap.Error(rErr))
		xhttp.WriteError(w, rErr)
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	ready, err := h.ready(ctx, req, opts)
	if err != nil {
		logger.Error("unable to mark namespace as ready", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	resp := &admin.NamespaceReadyResponse{
		Ready: ready,
	}

	xhttp.WriteProtoMsgJSONResponse(w, resp, logger)
}

func (h *ReadyHandler) parseRequest(r *http.Request) (*admin.NamespaceReadyRequest, error) {
	defer r.Body.Close()

	req := new(admin.NamespaceReadyRequest)
	if err := jsonpb.Unmarshal(r.Body, req); err != nil {
		return nil, xerrors.NewInvalidParamsError(err)
	}

	return req, nil
}

func (h *ReadyHandler) ready(
	ctx context.Context,
	req *admin.NamespaceReadyRequest,
	opts handleroptions.ServiceOptions,
) (bool, error) {
	// NB(nate): Readying a namespace only applies to namespaces created dynamically. As such,
	// ensure that any calls to the ready endpoint simply return true when using static configuration
	// as namespaces are ready by default in this case.
	if h.clusters != nil && h.clusters.ConfigType() == m3.ClusterConfigTypeStatic {
		h.instrumentOpts.Logger().Debug(
			"/namespace/ready endpoint not supported for statically configured namespaces.",
		)
		return true, nil
	}

	// Fetch existing namespace metadata.
	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return false, err
	}

	metadata, version, err := Metadata(store)
	if err != nil {
		return false, err
	}

	// Find the desired namespace.
	newMetadata := make(map[string]namespace.Metadata)
	for _, ns := range metadata {
		newMetadata[ns.ID().String()] = ns
	}

	ns, ok := newMetadata[req.Name]
	if !ok {
		return false, xerrors.NewInvalidParamsError(fmt.Errorf("namespace %v not found", req.Name))
	}

	// Just return if namespace is already ready.
	currentState := ns.Options().StagingState()
	if currentState.Status() == namespace.ReadyStagingStatus {
		return true, nil
	}

	// If we're not forcing the staging state, check db nodes to see if namespace is ready.
	if !req.Force {
		if err := h.checkDBNodes(ctx, req.Name); err != nil {
			return false, err
		}
	}

	// Update staging state status to ready.
	state, err := namespace.NewStagingState(nsproto.StagingStatus_READY)
	if err != nil {
		return false, err
	}
	newOpts := ns.Options().SetStagingState(state)
	newNs, err := namespace.NewMetadata(ns.ID(), newOpts)
	if err != nil {
		return false, err
	}

	newMetadata[req.Name] = newNs

	newMds := make([]namespace.Metadata, 0, len(newMetadata))
	for _, elem := range newMetadata {
		newMds = append(newMds, elem)
	}

	nsMap, err := namespace.NewMap(newMds)
	if err != nil {
		return false, err
	}

	protoRegistry, err := namespace.ToProto(nsMap)
	if err != nil {
		return false, err
	}

	if _, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry); err != nil {
		return false, err
	}

	return true, nil
}

func (h *ReadyHandler) checkDBNodes(ctx context.Context, namespace string) error {
	if h.clusters == nil {
		err := fmt.Errorf("coordinator is not connected to dbnodes. cannot check namespace %v"+
			" for readiness. set force = true to make namespaces ready without checking dbnodes", namespace)
		return xerrors.NewInvalidParamsError(err)
	}

	var (
		session client.Session
		id      ident.ID
	)
	for _, clusterNamespace := range h.clusters.NonReadyClusterNamespaces() {
		if clusterNamespace.NamespaceID().String() == namespace {
			session = clusterNamespace.Session()
			id = clusterNamespace.NamespaceID()
			break
		}
	}
	if session == nil {
		err := fmt.Errorf("could not find db session for namespace: %v", namespace)
		return xerrors.NewInvalidParamsError(err)
	}

	// Do a simple quorum read. A non-error indicates most dbnodes have the namespace.
	_, _, err := session.FetchTaggedIDs(ctx, id,
		index.Query{Query: idx.NewAllQuery()},
		index.QueryOptions{SeriesLimit: 1, DocsLimit: 1})
	// We treat any error here as a proxy for namespace readiness.
	if err != nil {
		return fmt.Errorf("namepace %v not yet ready, err: %w", namespace, err)
	}

	return nil
}
