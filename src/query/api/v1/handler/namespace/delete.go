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

package namespace

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/route"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	namespaceIDVar = "id"

	// DeleteHTTPMethod is the HTTP method used with this resource.
	DeleteHTTPMethod = http.MethodDelete
)

var (
	// M3DBDeleteURL is the url for the M3DB namespace delete handler.
	M3DBDeleteURL = path.Join(
		route.Prefix,
		M3DBServiceNamespacePathName,
		fmt.Sprintf("{%s}", namespaceIDVar),
	)
)

var (
	errEmptyID = xerrors.NewInvalidParamsError(errors.New("must specify namespace ID to delete"))
)

// DeleteHandler is the handler for namespace deletes.
type DeleteHandler Handler

// NewDeleteHandler returns a new instance of DeleteHandler.
func NewDeleteHandler(
	client clusterclient.Client,
	instrumentOpts instrument.Options,
) *DeleteHandler {
	return &DeleteHandler{
		client:         client,
		instrumentOpts: instrumentOpts,
	}
}

func (h *DeleteHandler) ServeHTTP(
	svc handleroptions.ServiceNameAndDefaults,
	w http.ResponseWriter,
	r *http.Request,
) {
	ctx := r.Context()
	logger := logging.WithContext(ctx, h.instrumentOpts)
	id := strings.TrimSpace(mux.Vars(r)[namespaceIDVar])
	if id == "" {
		logger.Error("no namespace ID to delete", zap.Error(errEmptyID))
		xhttp.WriteError(w, errEmptyID)
		return
	}

	opts := handleroptions.NewServiceOptions(svc, r.Header, nil)
	err := h.Delete(id, opts)
	if err != nil {
		logger.Error("unable to delete namespace", zap.Error(err))
		xhttp.WriteError(w, err)
		return
	}

	json.NewEncoder(w).Encode(struct {
		Deleted bool `json:"deleted"`
	}{
		Deleted: true,
	})
}

// Delete deletes a namespace.
func (h *DeleteHandler) Delete(id string, opts handleroptions.ServiceOptions) error {
	store, err := h.client.Store(opts.KVOverrideOptions())
	if err != nil {
		return err
	}

	metadatas, version, err := Metadata(store)
	if err != nil {
		return err
	}

	mdIdx := -1
	for idx, md := range metadatas {
		if md.ID().String() == id {
			mdIdx = idx
			break
		}
	}

	if mdIdx == -1 {
		return errNamespaceNotFound
	}

	// If metadatas are empty, remove the key
	if len(metadatas) == 1 {
		if _, err = store.Delete(M3DBNodeNamespacesKey); err != nil {
			return fmt.Errorf("unable to delete kv key: %v", err)
		}

		return nil
	}

	// Replace the index where we found the metadata with the last element, then truncate
	metadatas[mdIdx] = metadatas[len(metadatas)-1]
	metadatas = metadatas[:len(metadatas)-1]

	// Update namespace map and set kv
	nsMap, err := namespace.NewMap(metadatas)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	protoRegistry, err := namespace.ToProto(nsMap)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	return nil
}
