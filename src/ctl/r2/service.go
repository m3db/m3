// Copyright (c) 2017 Uber Technologies, Inc.
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

package r2

import (
	"fmt"
	"net/http"

	"github.com/m3db/m3ctl/services/r2ctl/server"
	"github.com/m3db/m3x/instrument"

	"github.com/gorilla/mux"
)

const (
	namespacePrefix   = "namespace/{namespaceId}"
	mappingRulePrefix = "mapping"
	rollupRulePrefix  = "rollup"
	namespacePath     = "namespaces"
)

var (
	mappingRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, mappingRulePrefix)
	mappingRuleWithIDPath  = fmt.Sprintf("%s/{ruleId}", mappingRuleRoot)
	mappingRuleHistoryPath = fmt.Sprintf("%s/history", mappingRuleWithIDPath)

	rollupRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, rollupRulePrefix)
	rollupRuleWithIDPath  = fmt.Sprintf("%s/{ruleId}", rollupRuleRoot)
	rollupRuleHistoryPath = fmt.Sprintf("%s/history", rollupRuleWithIDPath)
)

// service handles all of the endpoints for r2.
type service struct {
	iOpts      instrument.Options
	rootPrefix string
	store      Store
}

// NewService creates a new r2 service using a given store.
func NewService(iOpts instrument.Options, rootPrefix string, store Store) server.Service {
	return &service{iOpts: iOpts, store: store, rootPrefix: rootPrefix}
}

func (s *service) readNamespaces(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readNamespace")
}

func (s *service) readNamespace(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readNamespace")
}

func (s *service) createNamespace(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for createNamespace")
}

func (s *service) deleteNamespace(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for deleteNamespace")
}

func (s *service) readMappingRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readMappingRule")
}

func (s *service) createMappingRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for createMappingRule")
}

func (s *service) updateMappingRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for updateMappingRule")
}

func (s *service) deleteMappingRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for deleteMappingRule")
}

func (s *service) readMappingRuleHistory(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readMappingRuleHistory")
}

func (s *service) readRollupRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readRollupRule")
}

func (s *service) createRollupRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for createRollupRule")
}

func (s *service) updateRollupRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for updateRollupRule")
}

func (s *service) deleteRollupRule(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for deleteRollupRule")
}

func (s *service) readRollupRuleHistory(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusNotImplemented)
	fmt.Fprintf(w, "Placeholder for readRollupRuleHistory")
}

// RegisterHandlers registers rule handler.
func (s *service) RegisterHandlers(router *mux.Router) {
	log := s.iOpts.Logger()
	// Get all namespaces
	router.HandleFunc(s.fullPath(namespacePrefix), s.readNamespaces).Methods(http.MethodGet)

	fullNamespacePath := s.fullPath(namespacePath)
	// Ruleset actions
	router.HandleFunc(fullNamespacePath, s.readNamespace).Methods(http.MethodGet)
	router.HandleFunc(fullNamespacePath, s.createNamespace).Methods(http.MethodPost)
	router.HandleFunc(fullNamespacePath, s.deleteNamespace).Methods(http.MethodDelete)

	// Mapping Rule actions
	router.HandleFunc(s.fullPath(mappingRuleRoot), s.createMappingRule).Methods(http.MethodPost)

	fullMappingRuleWithIDPath := s.fullPath(mappingRuleWithIDPath)
	router.HandleFunc(fullMappingRuleWithIDPath, s.readMappingRule).Methods(http.MethodGet)
	router.HandleFunc(fullMappingRuleWithIDPath, s.updateMappingRule).Methods(http.MethodPut, http.MethodPatch)
	router.HandleFunc(fullMappingRuleWithIDPath, s.deleteMappingRule).Methods(http.MethodDelete)

	// Mapping Rule history
	router.HandleFunc(s.fullPath(mappingRuleHistoryPath), s.readMappingRuleHistory).Methods(http.MethodGet)

	// Rollup Rule actions
	router.HandleFunc(s.fullPath(rollupRuleRoot), s.createRollupRule).Methods(http.MethodPost)

	fullRollupRuleWithIDPath := s.fullPath(rollupRuleWithIDPath)
	router.HandleFunc(fullRollupRuleWithIDPath, s.readRollupRule).Methods(http.MethodGet)
	router.HandleFunc(fullRollupRuleWithIDPath, s.updateRollupRule).Methods(http.MethodPut, http.MethodPatch)
	router.HandleFunc(fullRollupRuleWithIDPath, s.deleteRollupRule).Methods(http.MethodDelete)

	// Rollup Rule history
	router.HandleFunc(s.fullPath(rollupRuleHistoryPath), s.readRollupRuleHistory).Methods(http.MethodGet)

	log.Infof("Registered rules endpoints")
}

func (s *service) fullPath(path string) string {
	return fmt.Sprintf("/%s/%s", s.rootPrefix, path)
}
