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
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strconv"
	"strings"

	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3ctl/services/r2ctl/server"
	"github.com/m3db/m3metrics/policy"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/instrument"
	xtime "github.com/m3db/m3x/time"

	"github.com/gorilla/mux"
	"gopkg.in/go-playground/validator.v9"
)

const (
	// TagFilterListSeparator splits key:value pairs in a tag filter list.
	tagFilterListSeparator = " "

	// TagFilterSeparator splits keys and values inside of a tag filter.
	tagFilterSeparator = ":"

	namespacePath     = "/namespaces"
	mappingRulePrefix = "mapping-rules"
	rollupRulePrefix  = "rollup-rules"

	namespaceIDVar = "namespaceID"
	ruleIDVar      = "ruleID"
)

var (
	namespacePrefix = fmt.Sprintf("%s/{%s}", namespacePath, namespaceIDVar)

	mappingRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, mappingRulePrefix)
	mappingRuleWithIDPath  = fmt.Sprintf("%s/{%s}", mappingRuleRoot, ruleIDVar)
	mappingRuleHistoryPath = fmt.Sprintf("%s/history", mappingRuleWithIDPath)

	rollupRuleRoot        = fmt.Sprintf("%s/%s", namespacePrefix, rollupRulePrefix)
	rollupRuleWithIDPath  = fmt.Sprintf("%s/{%s}", rollupRuleRoot, ruleIDVar)
	rollupRuleHistoryPath = fmt.Sprintf("%s/history", rollupRuleWithIDPath)
)

func write(w http.ResponseWriter, data []byte, status int) error {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, err := w.Write(data)
	return err
}

// TODO(dgromov): Make this return a list of validation errors
func parseRequest(s interface{}, body io.ReadCloser) error {
	if err := json.NewDecoder(body).Decode(s); err != nil {
		return NewBadInputError(fmt.Sprintf("Malformed Json: %s", err.Error()))
	}

	// Invoking the validation explictely to have control over the format of the error output.
	validate := validator.New()
	validate.RegisterTagNameFunc(func(fld reflect.StructField) string {
		parts := strings.SplitN(fld.Tag.Get("json"), ",", 2)
		if len(parts) > 0 {
			return parts[0]
		}
		return fld.Name
	})

	var required []string
	if err := validate.Struct(s); err != nil {
		for _, e := range err.(validator.ValidationErrors) {
			if e.ActualTag() == "required" {
				required = append(required, e.Namespace())
			}
		}
	}

	if len(required) > 0 {
		return NewBadInputError(fmt.Sprintf("Required: [%v]", strings.Join(required, ", ")))
	}
	return nil
}

func writeAPIResponse(w http.ResponseWriter, code int, msg string) error {
	j, err := json.Marshal(apiResponse{Code: code, Message: msg})
	if err != nil {
		return err
	}
	return write(w, j, code)
}

type r2HandlerFunc func(http.ResponseWriter, *http.Request) error

type r2Handler struct {
	iOpts instrument.Options
	auth  auth.HTTPAuthService
}

func (h r2Handler) wrap(fn r2HandlerFunc) http.Handler {
	f := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := fn(w, r); err != nil {
			h.handleError(w, err)
		}
	})

	return h.auth.NewAuthHandler(f)
}

func (h r2Handler) handleError(w http.ResponseWriter, opError error) {
	h.iOpts.Logger().Errorf(opError.Error())

	var err error
	switch opError.(type) {
	case conflictError:
		err = writeAPIResponse(w, http.StatusConflict, opError.Error())
	case badInputError:
		err = writeAPIResponse(w, http.StatusBadRequest, opError.Error())
	case versionError:
		err = writeAPIResponse(w, http.StatusConflict, opError.Error())
	case notFoundError:
		err = writeAPIResponse(w, http.StatusNotFound, opError.Error())
	case authError:
		err = writeAPIResponse(w, http.StatusUnauthorized, opError.Error())
	default:
		err = writeAPIResponse(w, http.StatusInternalServerError, opError.Error())
	}

	// Getting here means that the error handling failed. Trying to convey what was supposed to happen.
	if err != nil {
		msg := fmt.Sprintf("Could not generate error response for: %s", opError.Error())
		h.iOpts.Logger().Errorf(msg)
		http.Error(w, msg, http.StatusInternalServerError)
	}
}

// service handles all of the endpoints for r2.
type service struct {
	iOpts       instrument.Options
	rootPrefix  string
	store       Store
	authService auth.HTTPAuthService
}

// NewService creates a new r2 service using a given store.
func NewService(
	iOpts instrument.Options,
	rootPrefix string,
	authService auth.HTTPAuthService,
	store Store,
) server.Service {
	return &service{iOpts: iOpts, store: store, authService: authService, rootPrefix: rootPrefix}
}

func (s *service) write(w http.ResponseWriter, data interface{}) error {
	if j, err := json.Marshal(data); err == nil {
		return write(w, j, http.StatusOK)
	}
	return writeAPIResponse(w, http.StatusInternalServerError, "Could not create response object")
}

func (s *service) fetchNamespaces(w http.ResponseWriter, _ *http.Request) error {
	names, v, err := s.store.FetchNamespaces()
	if err != nil {
		return writeAPIResponse(w, http.StatusInternalServerError, "Could not read namespaces")
	}

	nss := make([]namespaceJSON, len(names))
	for i, n := range names {
		nss[i] = namespaceJSON{ID: n}
	}
	return s.write(w, namespacesJSON{Version: v, Namespaces: nss})
}

func (s *service) fetchNamespace(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	rs, err := s.store.FetchRuleSet(vars[namespaceIDVar])
	if err != nil {
		return err
	}
	return s.write(w, rs.ruleSetJSON())
}

func (s *service) createNamespace(w http.ResponseWriter, r *http.Request) error {
	var n namespaceJSON
	if err := parseRequest(&n, r.Body); err != nil {
		return err
	}

	id, err := s.store.CreateNamespace(n.ID)
	if err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusCreated, fmt.Sprintf("Created namespace %s", id))
}

func (s *service) deleteNamespace(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	namespaceID := vars["namespaceIDVar"]
	if err := s.store.DeleteNamespace(namespaceID); err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK, fmt.Sprintf("Deleted namespace %s", namespaceID))
}

func (s *service) fetchMappingRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	mr, err := s.store.FetchMappingRule(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return err
	}
	return s.write(w, newMappingRuleJSON(mr))
}

func (s *service) createMappingRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	var mrj mappingRuleJSON
	if err := parseRequest(&mrj, r.Body); err != nil {
		return err
	}

	mr, err := s.store.CreateMappingRule(
		vars[namespaceIDVar],
		mrj.mappingRuleView(),
	)

	if err != nil {
		return err
	}
	return s.write(w, newMappingRuleJSON(mr))
}

func (s *service) updateMappingRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)

	var mrj mappingRuleJSON
	if err := parseRequest(&mrj, r.Body); err != nil {
		return err
	}
	mr, err := s.store.UpdateMappingRule(
		vars[namespaceIDVar],
		vars[ruleIDVar],
		mrj.mappingRuleView(),
	)

	if err != nil {
		return err
	}
	return s.write(w, newMappingRuleJSON(mr))
}

func (s *service) deleteMappingRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]
	mappingRuleID := vars[ruleIDVar]
	if err := s.store.DeleteMappingRule(namespaceID, mappingRuleID); err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK,
		fmt.Sprintf("Deleted mapping rule: %s in namespace %s", mappingRuleID, namespaceID))
}

func (s *service) fetchMappingRuleHistory(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	hist, err := s.store.FetchMappingRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return err
	}
	return s.write(w, newMappingRuleHistoryJSON(hist))
}

func (s *service) fetchRollupRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	rr, err := s.store.FetchRollupRule(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return err
	}
	return s.write(w, newRollupRuleJSON(rr))
}

func (s *service) createRollupRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]

	var rrj rollupRuleJSON
	if err := parseRequest(&rrj, r.Body); err != nil {
		return err
	}

	rr, err := s.store.CreateRollupRule(namespaceID, rrj.rollupRuleView())
	if err != nil {
		return err
	}
	return s.write(w, newRollupRuleJSON(rr))
}

func (s *service) updateRollupRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)

	var rrj rollupRuleJSON
	if err := parseRequest(&rrj, r.Body); err != nil {
		return err
	}

	rr, err := s.store.UpdateRollupRule(
		vars[namespaceIDVar],
		vars[ruleIDVar],
		rrj.rollupRuleView(),
	)

	if err != nil {
		return err
	}
	return s.write(w, newRollupRuleJSON(rr))
}

func (s *service) deleteRollupRule(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	namespaceID := vars[namespaceIDVar]
	rollupRuleID := vars[ruleIDVar]
	if err := s.store.DeleteRollupRule(namespaceID, rollupRuleID); err != nil {
		return err
	}
	return writeAPIResponse(w, http.StatusOK,
		fmt.Sprintf("Deleted rollup rule: %s in namespace %s", rollupRuleID, namespaceID))
}

func (s *service) fetchRollupRuleHistory(w http.ResponseWriter, r *http.Request) error {
	vars := mux.Vars(r)
	hist, err := s.store.FetchRollupRuleHistory(vars[namespaceIDVar], vars[ruleIDVar])
	if err != nil {
		return err
	}
	return s.write(w, newRollupRuleHistoryJSON(hist))
}

// RegisterHandlers registers rule handler.
func (s *service) RegisterHandlers(router *mux.Router) {
	log := s.iOpts.Logger()
	// Namespaces action
	h := r2Handler{s.iOpts, s.authService}

	router.Handle(namespacePath, h.wrap(s.fetchNamespaces)).Methods(http.MethodGet)
	router.Handle(namespacePath, h.wrap(s.createNamespace)).Methods(http.MethodPost)

	// Ruleset actions
	router.Handle(namespacePrefix, h.wrap(s.fetchNamespace)).Methods(http.MethodGet)
	router.Handle(namespacePrefix, h.wrap(s.deleteNamespace)).Methods(http.MethodDelete)

	// Mapping Rule actions
	router.Handle(mappingRuleRoot, h.wrap(s.createMappingRule)).Methods(http.MethodPost)

	router.Handle(mappingRuleWithIDPath, h.wrap(s.fetchMappingRule)).Methods(http.MethodGet)
	router.Handle(mappingRuleWithIDPath, h.wrap(s.updateMappingRule)).Methods(http.MethodPut, http.MethodPatch)
	router.Handle(mappingRuleWithIDPath, h.wrap(s.deleteMappingRule)).Methods(http.MethodDelete)

	// Mapping Rule history
	router.Handle(mappingRuleHistoryPath, h.wrap(s.fetchMappingRuleHistory)).Methods(http.MethodGet)

	// Rollup Rule actions
	router.Handle(rollupRuleRoot, h.wrap(s.createRollupRule)).Methods(http.MethodPost)

	router.Handle(rollupRuleWithIDPath, h.wrap(s.fetchRollupRule)).Methods(http.MethodGet)
	router.Handle(rollupRuleWithIDPath, h.wrap(s.updateRollupRule)).Methods(http.MethodPut, http.MethodPatch)
	router.Handle(rollupRuleWithIDPath, h.wrap(s.deleteRollupRule)).Methods(http.MethodDelete)

	// Rollup Rule history
	router.Handle(rollupRuleHistoryPath, h.wrap(s.fetchRollupRuleHistory)).Methods(http.MethodGet)

	log.Infof("Registered rules endpoints")
}

func (s *service) URLPrefix() string {
	return s.rootPrefix
}

// CurrentRuleSet represents the current state of a rule set. A rule with all the latest snapshots
// of its ruleset.
type CurrentRuleSet struct {
	Namespace    string
	Version      int
	CutoverNanos int64
	MappingRules []*rules.MappingRuleView
	RollupRules  []*rules.RollupRuleView
}

// nolint
func newCurrentRuleSetView(r rules.RuleSet) (*CurrentRuleSet, error) {
	mrs, err := r.MappingRules()
	if err != nil {
		return nil, err
	}

	rrs, err := r.RollupRules()
	if err != nil {
		return nil, err
	}

	return &CurrentRuleSet{
		Namespace:    string(r.Namespace()),
		Version:      r.Version(),
		CutoverNanos: r.CutoverNanos(),
		MappingRules: newMappingRules(mrs),
		RollupRules:  newRollupRules(rrs),
	}, nil
}

func (c CurrentRuleSet) ruleSetJSON() ruleSetJSON {
	mrJSON := make([]mappingRuleJSON, len(c.MappingRules))
	for i, m := range c.MappingRules {
		mrJSON[i] = newMappingRuleJSON(m)
	}
	rrJSON := make([]rollupRuleJSON, len(c.RollupRules))
	for i, r := range c.RollupRules {
		rrJSON[i] = newRollupRuleJSON(r)
	}
	return ruleSetJSON{
		Namespace:     c.Namespace,
		Version:       c.Version,
		CutoverMillis: xtime.ToUnixMillis(xtime.FromNanoseconds(c.CutoverNanos)),
		MappingRules:  mrJSON,
		RollupRules:   rrJSON,
	}
}

// nolint
func newMappingRules(mrs rules.MappingRules) []*rules.MappingRuleView {
	var result []*rules.MappingRuleView
	for _, m := range mrs {
		if len(m) > 0 {
			// views included in m are sorted latest first.
			result = append(result, m[0])
		}
	}
	return result
}

// nolint
func newRollupRules(rrs rules.RollupRules) []*rules.RollupRuleView {
	var result []*rules.RollupRuleView
	for _, r := range rrs {
		if len(r) > 0 {
			// views included in m are sorted latest first.
			result = append(result, r[0])
		}
	}
	return result
}

// filters is an object that represents the key value tag filters for a rule.
// It is used for parsing a key:value query string into a map useful for storage.
type filters map[string]string

func (f filters) MarshalJSON() ([]byte, error) {
	tmp := make([]string, 0, len(f))
	for k, v := range f {
		tmp = append(tmp, fmt.Sprintf("%s%s%s", k, tagFilterSeparator, v))
	}
	marshalled := strconv.Quote(strings.Join(tmp, tagFilterListSeparator))
	return []byte(marshalled), nil
}

func (f *filters) UnmarshalJSON(data []byte) error {
	str := string(data)
	unquoted, err := strconv.Unquote(str)
	if err != nil {
		return err
	}

	tmp := make(map[string]string)
	tagPairs := strings.Split(unquoted, tagFilterListSeparator)
	for _, p := range tagPairs {
		parts := strings.Split(p, tagFilterSeparator)
		if len(parts) != 2 {
			return NewBadInputError(fmt.Sprintf("invalid filter: Expecting tag:value pairs. Got: %s", unquoted))
		}
		tmp[parts[0]] = parts[1]
	}
	*f = tmp
	return nil
}

type apiResponse struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
}

type namespaceJSON struct {
	ID string `json:"id" validate:"required"`
}

type namespacesJSON struct {
	Version    int             `json:"version"`
	Namespaces []namespaceJSON `json:"namespaces"`
}

type mappingRuleJSON struct {
	ID            string          `json:"id,omitempty"`
	Name          string          `json:"name" validate:"required"`
	CutoverMillis int64           `json:"cutoverMillis,omitempty"`
	Filters       filters         `json:"filter" validate:"required"`
	Policies      []policy.Policy `json:"policies" validate:"required"`
}

func (m mappingRuleJSON) mappingRuleView() *rules.MappingRuleView {
	return &rules.MappingRuleView{
		ID:       m.ID,
		Name:     m.Name,
		Filters:  m.Filters,
		Policies: m.Policies,
	}
}

func newMappingRuleJSON(mrv *rules.MappingRuleView) mappingRuleJSON {
	return mappingRuleJSON{
		ID:            mrv.ID,
		Name:          mrv.Name,
		Filters:       mrv.Filters,
		Policies:      mrv.Policies,
		CutoverMillis: xtime.ToUnixMillis(xtime.FromNanoseconds(mrv.CutoverNanos)),
	}
}

type mappingRuleHistoryJSON struct {
	MappingRules []mappingRuleJSON `json:"mappingRules"`
}

func newMappingRuleHistoryJSON(hist []*rules.MappingRuleView) mappingRuleHistoryJSON {
	views := make([]mappingRuleJSON, len(hist))
	for i, mappingRule := range hist {
		views[i] = newMappingRuleJSON(mappingRule)
	}
	return mappingRuleHistoryJSON{MappingRules: views}
}

type rollupTargetJSON struct {
	Name     string          `json:"name" validate:"required"`
	Tags     []string        `json:"tags" validate:"required"`
	Policies []policy.Policy `json:"policies" validate:"required"`
}

func (t rollupTargetJSON) rollupTargetView() rules.RollupTargetView {
	return rules.RollupTargetView{
		Name:     t.Name,
		Tags:     t.Tags,
		Policies: t.Policies,
	}
}

func newRollupTargetJSON(t rules.RollupTargetView) rollupTargetJSON {
	return rollupTargetJSON{
		Name:     t.Name,
		Tags:     t.Tags,
		Policies: t.Policies,
	}
}

type rollupRuleJSON struct {
	ID            string             `json:"id,omitempty"`
	Name          string             `json:"name" validate:"required"`
	Filters       filters            `json:"filter" validate:"required"`
	Targets       []rollupTargetJSON `json:"targets" validate:"required,dive,required"`
	CutoverMillis int64              `json:"cutoverMillis,omitempty"`
}

func newRollupRuleJSON(rrv *rules.RollupRuleView) rollupRuleJSON {
	targets := make([]rollupTargetJSON, len(rrv.Targets))
	for i, t := range rrv.Targets {
		targets[i] = newRollupTargetJSON(t)
	}
	return rollupRuleJSON{
		ID:            rrv.ID,
		Name:          rrv.Name,
		Filters:       rrv.Filters,
		Targets:       targets,
		CutoverMillis: xtime.ToUnixMillis(xtime.FromNanoseconds(rrv.CutoverNanos)),
	}
}

func (r rollupRuleJSON) rollupRuleView() *rules.RollupRuleView {
	targets := make([]rules.RollupTargetView, len(r.Targets))
	for i, t := range r.Targets {
		targets[i] = t.rollupTargetView()
	}

	return &rules.RollupRuleView{
		ID:      r.ID,
		Name:    r.Name,
		Filters: r.Filters,
		Targets: targets,
	}
}

type rollupRuleHistoryJSON struct {
	RollupRules []rollupRuleJSON `json:"rollupRules"`
}

func newRollupRuleHistoryJSON(hist []*rules.RollupRuleView) rollupRuleHistoryJSON {
	views := make([]rollupRuleJSON, len(hist))
	for i, rollupRule := range hist {
		views[i] = newRollupRuleJSON(rollupRule)
	}
	return rollupRuleHistoryJSON{RollupRules: views}
}

type ruleSetJSON struct {
	Namespace     string            `json:"id"`
	Version       int               `json:"version"`
	CutoverMillis int64             `json:"cutoverMillis"`
	MappingRules  []mappingRuleJSON `json:"mappingRules"`
	RollupRules   []rollupRuleJSON  `json:"rollupRules"`
}
