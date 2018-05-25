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
	"strings"

	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/src/cmd/services/m3coordinator/handler"
	"github.com/m3db/m3db/src/coordinator/util/logging"

	"github.com/m3db/m3db/src/dbnode/storage/namespace"

	"github.com/gorilla/mux"
	"go.uber.org/zap"
)

const (
	namespaceIDVar = "id"
)

var (
	// DeleteURL is the url for the namespace delete handler (with the DELETE method).
	DeleteURL = fmt.Sprintf("%s/namespace/{%s}", handler.RoutePrefix, namespaceIDVar)
)

var (
	errNamespaceNotFound = errors.New("unable to find a namespace with specified name")

	errEmptyID = errors.New("must specify namespace ID to delete")
)

type deleteHandler Handler

// NewDeleteHandler returns a new instance of a namespace delete handler.
func NewDeleteHandler(client clusterclient.Client) http.Handler {
	return &deleteHandler{client: client}
}

func (h *deleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)
	id := strings.TrimSpace(mux.Vars(r)[namespaceIDVar])
	if id == "" {
		logger.Error("no namespace ID to delete", zap.Any("error", errEmptyID))
		handler.Error(w, errEmptyID, http.StatusBadRequest)
		return
	}

	err := h.delete(id)
	if err != nil {
		logger.Error("unable to delete namespace", zap.Any("error", err))
		if err == errNamespaceNotFound {
			handler.Error(w, err, http.StatusNotFound)
		} else {
			handler.Error(w, err, http.StatusInternalServerError)
		}
		return
	}

	json.NewEncoder(w).Encode(struct {
		Deleted bool `json:"deleted"`
	}{
		Deleted: true,
	})
}

func (h *deleteHandler) delete(id string) error {
	store, err := h.client.KV()
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

	protoRegistry := namespace.ToProto(nsMap)
	_, err = store.CheckAndSet(M3DBNodeNamespacesKey, version, protoRegistry)
	if err != nil {
		return fmt.Errorf("failed to delete namespace: %v", err)
	}

	return nil
}
