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

package handler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"

	"github.com/m3db/m3coordinator/generated/proto/admin"
	"github.com/m3db/m3coordinator/util/logging"

	m3clusterClient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3db/storage/namespace"

	"go.uber.org/zap"
)

const (
	// NamespaceDeleteURL is the url for the placement delete handler (with the POST method).
	NamespaceDeleteURL = "/namespace/delete"
)

var (
	errNamespaceNotFound = errors.New("unable to find a namespace with specified name")
)

// namespaceDeleteHandler represents a handler for placement delete endpoint.
type namespaceDeleteHandler AdminHandler

// NewNamespaceDeleteHandler returns a new instance of handler.
func NewNamespaceDeleteHandler(clusterClient m3clusterClient.Client) http.Handler {
	return &namespaceDeleteHandler{
		clusterClient: clusterClient,
	}
}

func (h *namespaceDeleteHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	logger := logging.WithContext(ctx)

	req, rErr := h.parseRequest(r)
	if rErr != nil {
		Error(w, rErr.Error(), rErr.Code())
		return
	}

	err := h.namespaceDelete(ctx, req)
	if err != nil {
		logger.Error("unable to delete namespace", zap.Any("error", err))

		if err == errNamespaceNotFound {
			Error(w, err, http.StatusBadRequest)
		} else {
			Error(w, err, http.StatusInternalServerError)
		}
	}
}

func (h *namespaceDeleteHandler) parseRequest(r *http.Request) (*admin.NamespaceDeleteRequest, *ParseError) {
	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}
	defer r.Body.Close()

	deleteReq := new(admin.NamespaceDeleteRequest)
	if err := json.Unmarshal(body, deleteReq); err != nil {
		return nil, NewParseError(err, http.StatusBadRequest)
	}

	return deleteReq, nil
}

func (h *namespaceDeleteHandler) namespaceDelete(ctx context.Context, r *admin.NamespaceDeleteRequest) error {
	kv, err := h.clusterClient.KV()
	if err != nil {
		return err
	}

	currentMetadata, err := currentNamespaceMetadata(kv)
	if err != nil {
		return err
	}

	newMds := []namespace.Metadata{}
	found := false
	for _, md := range currentMetadata {
		if md.ID().String() == r.Name {
			found = true
			continue
		}
		newMds = append(newMds, md)
	}

	if !found {
		return errNamespaceNotFound
	}

	// If metadatas are empty, remove the key
	if len(newMds) == 0 {
		if _, err = kv.Delete(M3DBNodeNamespacesKey); err != nil {
			return fmt.Errorf("unable to delete kv key: %v", err)
		}

		return nil
	}

	// Update namespace map and set kv
	nsMap, err := namespace.NewMap(newMds)
	if err != nil {
		return fmt.Errorf("unable to delete kv key: %v", err)
	}

	protoRegistry := namespace.ToProto(nsMap)
	_, err = kv.Set(M3DBNodeNamespacesKey, protoRegistry)
	if err != nil {
		return fmt.Errorf("unable to update kv: %v", err)
	}

	return nil
}
