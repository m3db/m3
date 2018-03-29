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
	"encoding/json"
	"errors"
	"fmt"
	"net/http"

	"github.com/m3db/m3ctl/auth"
	mservice "github.com/m3db/m3ctl/service"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"
	"github.com/m3db/m3x/log"

	"github.com/gorilla/mux"
	"github.com/uber-go/tally"
)

const (
	namespacePath     = "/namespaces"
	mappingRulePrefix = "mapping-rules"
	rollupRulePrefix  = "rollup-rules"
	namespaceIDVar    = "namespaceID"
	ruleIDVar         = "ruleID"
)

var (
	namespacePrefix     = fmt.Sprintf("%s/{%s}", namespacePath, namespaceIDVar)
	validateRuleSetPath = fmt.Sprintf("%s/{%s}/ruleset/validate", namespacePath, namespaceIDVar)

	mappingRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, mappingRulePrefix)
	mappingRuleWithIDPath  = fmt.Sprintf("%s/{%s}", mappingRuleRoot, ruleIDVar)
	mappingRuleHistoryPath = fmt.Sprintf("%s/history", mappingRuleWithIDPath)

	rollupRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, rollupRulePrefix)
	rollupRuleWithIDPath  = fmt.Sprintf("%s/{%s}", rollupRuleRoot, ruleIDVar)
	rollupRuleHistoryPath = fmt.Sprintf("%s/history", rollupRuleWithIDPath)

	errNilRequest = errors.New("Nil request")
)

type serviceMetrics struct {
	fetchNamespaces         instrument.MethodMetrics
	fetchNamespace          instrument.MethodMetrics
	createNamespace         instrument.MethodMetrics
	deleteNamespace         instrument.MethodMetrics
	validateRuleSet         instrument.MethodMetrics
	fetchMappingRule        instrument.MethodMetrics
	createMappingRule       instrument.MethodMetrics
	updateMappingRule       instrument.MethodMetrics
	deleteMappingRule       instrument.MethodMetrics
	fetchMappingRuleHistory instrument.MethodMetrics
	fetchRollupRule         instrument.MethodMetrics
	createRollupRule        instrument.MethodMetrics
	updateRollupRule        instrument.MethodMetrics
	deleteRollupRule        instrument.MethodMetrics
	fetchRollupRuleHistory  instrument.MethodMetrics
}

func newServiceMetrics(scope tally.Scope, samplingRate float64) serviceMetrics {
	return serviceMetrics{
		fetchNamespaces:         instrument.NewMethodMetrics(scope, "fetchNamespaces", samplingRate),
		fetchNamespace:          instrument.NewMethodMetrics(scope, "fetchNamespace", samplingRate),
		createNamespace:         instrument.NewMethodMetrics(scope, "createNamespace", samplingRate),
		deleteNamespace:         instrument.NewMethodMetrics(scope, "deleteNamespace", samplingRate),
		validateRuleSet:         instrument.NewMethodMetrics(scope, "validateRuleSet", samplingRate),
		fetchMappingRule:        instrument.NewMethodMetrics(scope, "fetchMappingRule", samplingRate),
		createMappingRule:       instrument.NewMethodMetrics(scope, "createMappingRule", samplingRate),
		updateMappingRule:       instrument.NewMethodMetrics(scope, "updateMappingRule", samplingRate),
		deleteMappingRule:       instrument.NewMethodMetrics(scope, "deleteMappingRule", samplingRate),
		fetchMappingRuleHistory: instrument.NewMethodMetrics(scope, "fetchMappingRuleHistory", samplingRate),
		fetchRollupRule:         instrument.NewMethodMetrics(scope, "fetchRollupRule", samplingRate),
		createRollupRule:        instrument.NewMethodMetrics(scope, "createRollupRule", samplingRate),
		updateRollupRule:        instrument.NewMethodMetrics(scope, "updateRollupRule", samplingRate),
		deleteRollupRule:        instrument.NewMethodMetrics(scope, "deleteRollupRule", samplingRate),
		fetchRollupRuleHistory:  instrument.NewMethodMetrics(scope, "fetchRollupRuleHistory", samplingRate),
	}
}

var authorizationRegistry = map[route]auth.AuthorizationType{
	// This validation route should only require read access.
	{path: validateRuleSetPath, method: http.MethodPost}: auth.ReadOnlyAuthorization,
}

func defaultAuthorizationTypeForHTTPMethod(method string) (auth.AuthorizationType, error) {
	switch method {
	case http.MethodGet:
		return auth.ReadOnlyAuthorization, nil
	case http.MethodPost, http.MethodPut, http.MethodDelete, http.MethodPatch:
		return auth.ReadWriteAuthorization, nil
	default:
		return auth.UnknownAuthorization, fmt.Errorf("unknown authorization type for method %s", method)
	}
}

func registerRoute(router *mux.Router, path, method string, h r2Handler, hf r2HandlerFunc) error {
	authType, exists := authorizationRegistry[route{path: path, method: method}]
	if !exists {
		var err error
		if authType, err = defaultAuthorizationTypeForHTTPMethod(method); err != nil {
			return fmt.Errorf("could not register route for method %s and path %s, error: %v", method, path, err)
		}
	}
	fn := h.wrap(authType, hf)
	router.Handle(path, fn).Methods(method)
	return nil
}

// service handles all of the endpoints for r2.
type service struct {
	rootPrefix  string
	store       Store
	authService auth.HTTPAuthService
	logger      log.Logger
	nowFn       clock.NowFn
	metrics     serviceMetrics
}

// NewService creates a new r2 service using a given store.
func NewService(
	rootPrefix string,
	authService auth.HTTPAuthService,
	store Store,
	iOpts instrument.Options,
	clockOpts clock.Options,
) mservice.Service {
	return &service{
		rootPrefix:  rootPrefix,
		store:       store,
		authService: authService,
		logger:      iOpts.Logger(),
		nowFn:       clockOpts.NowFn(),
		metrics:     newServiceMetrics(iOpts.MetricsScope(), iOpts.MetricsSamplingRate()),
	}
}

func (s *service) URLPrefix() string { return s.rootPrefix }

func (s *service) RegisterHandlers(router *mux.Router) error {
	routeWithHandlers := []struct {
		route   route
		handler r2HandlerFunc
	}{
		// Namespaces actions.
		{route: route{path: namespacePath, method: http.MethodGet}, handler: s.fetchNamespaces},
		{route: route{path: namespacePath, method: http.MethodPost}, handler: s.createNamespace},

		// Ruleset actions.
		{route: route{path: namespacePrefix, method: http.MethodGet}, handler: s.fetchNamespace},
		{route: route{path: namespacePrefix, method: http.MethodDelete}, handler: s.deleteNamespace},
		{route: route{path: validateRuleSetPath, method: http.MethodPost}, handler: s.validateNamespace},

		// Mapping Rule actions.
		{route: route{path: mappingRuleRoot, method: http.MethodPost}, handler: s.createMappingRule},

		{route: route{path: mappingRuleWithIDPath, method: http.MethodGet}, handler: s.fetchMappingRule},
		{route: route{path: mappingRuleWithIDPath, method: http.MethodPut}, handler: s.updateMappingRule},
		{route: route{path: mappingRuleWithIDPath, method: http.MethodDelete}, handler: s.deleteMappingRule},

		// Mapping Rule history.
		{route: route{path: mappingRuleHistoryPath, method: http.MethodGet}, handler: s.fetchMappingRuleHistory},

		// Rollup Rule actions.
		{route: route{path: rollupRuleRoot, method: http.MethodPost}, handler: s.createRollupRule},

		{route: route{path: rollupRuleWithIDPath, method: http.MethodGet}, handler: s.fetchRollupRule},
		{route: route{path: rollupRuleWithIDPath, method: http.MethodPut}, handler: s.updateRollupRule},
		{route: route{path: rollupRuleWithIDPath, method: http.MethodDelete}, handler: s.deleteRollupRule},

		// Rollup Rule history.
		{route: route{path: rollupRuleHistoryPath, method: http.MethodGet}, handler: s.fetchRollupRuleHistory},
	}

	h := r2Handler{s.logger, s.authService}
	for _, rh := range routeWithHandlers {
		if err := registerRoute(router, rh.route.path, rh.route.method, h, rh.handler); err != nil {
			return err
		}
	}
	s.logger.Info("Registered rules endpoints")
	return nil
}

func (s *service) Close() { s.store.Close() }

type routeFunc func(s *service, r *http.Request) (data interface{}, err error)

func (s *service) handleRoute(rf routeFunc, r *http.Request, m instrument.MethodMetrics) (interface{}, error) {
	if r == nil {
		return nil, errNilRequest
	}
	start := s.nowFn()
	data, err := rf(s, r)
	dur := s.nowFn().Sub(start)
	m.ReportSuccessOrError(err, dur)
	s.logRequest(r, err)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (s *service) logRequest(r *http.Request, err error) {
	logger := s.logger.WithFields(
		log.NewField("http-method", r.Method),
		log.NewField("route-path", r.RequestURI),
	)
	if err != nil {
		logger.WithFields(log.NewErrField(err)).Error("request error")
		return
	}
	logger.Info("request success")
}

func (s *service) sendResponse(w http.ResponseWriter, statusCode int, data interface{}) error {
	if j, err := json.Marshal(data); err == nil {
		return sendResponse(w, j, statusCode)
	}
	return writeAPIResponse(w, http.StatusInternalServerError, "could not create response object")
}

func (s *service) fetchNamespaces(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchNamespaces, r, s.metrics.fetchNamespaces)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) fetchNamespace(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchNamespace, r, s.metrics.fetchNamespace)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) createNamespace(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(createNamespace, r, s.metrics.createNamespace)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusCreated, data)
}

func (s *service) validateNamespace(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(validateRuleSet, r, s.metrics.validateRuleSet)
	if err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK, data.(string))
}

func (s *service) deleteNamespace(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(deleteNamespace, r, s.metrics.deleteNamespace)
	if err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK, data.(string))
}

func (s *service) fetchMappingRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchMappingRule, r, s.metrics.fetchMappingRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) createMappingRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(createMappingRule, r, s.metrics.createMappingRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusCreated, data)
}

func (s *service) updateMappingRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(updateMappingRule, r, s.metrics.updateMappingRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) deleteMappingRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(deleteMappingRule, r, s.metrics.deleteMappingRule)
	if err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK, data.(string))
}

func (s *service) fetchMappingRuleHistory(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchMappingRuleHistory, r, s.metrics.fetchMappingRuleHistory)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) fetchRollupRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchRollupRule, r, s.metrics.fetchRollupRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) createRollupRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(createRollupRule, r, s.metrics.createRollupRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusCreated, data)
}

func (s *service) updateRollupRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(updateRollupRule, r, s.metrics.updateRollupRule)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

func (s *service) deleteRollupRule(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(deleteRollupRule, r, s.metrics.deleteRollupRule)
	if err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK, data.(string))
}

func (s *service) fetchRollupRuleHistory(w http.ResponseWriter, r *http.Request) error {
	data, err := s.handleRoute(fetchRollupRuleHistory, r, s.metrics.fetchRollupRuleHistory)
	if err != nil {
		return err
	}
	return s.sendResponse(w, http.StatusOK, data)
}

type route struct {
	path   string
	method string
}

func (s *service) newUpdateOptions(r *http.Request) (UpdateOptions, error) {
	uOpts := NewUpdateOptions()
	author, err := s.authService.GetUser(r.Context())
	if err != nil {
		return uOpts, nil
	}
	return uOpts.SetAuthor(author), nil
}
