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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3ctl/service/r2/store"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3metrics/rules/view"
	"github.com/m3db/m3metrics/rules/view/changes"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/golang/mock/gomock"
	"github.com/gorilla/mux"
	"github.com/stretchr/testify/require"
	"github.com/uber-go/tally"
)

func TestHandleRoute(t *testing.T) {
	s := newTestService(nil)
	r := newTestGetRequest()
	expected := view.Namespaces{}
	actual, err := s.handleRoute(fetchNamespaces, r, newTestInstrumentMethodMetrics())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestHandleRouteNilRequest(t *testing.T) {
	s := newTestService(nil)
	_, err := s.handleRoute(fetchNamespaces, nil, newTestInstrumentMethodMetrics())
	require.EqualError(t, err, errNilRequest.Error())
}
func TestFetchNamespacesSuccess(t *testing.T) {
	expected := view.Namespaces{}
	actual, err := fetchNamespaces(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchNamespaceSuccess(t *testing.T) {
	expected := view.RuleSet{}
	actual, err := fetchNamespace(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestValidateRuleSetSuccess(t *testing.T) {
	expected := "Ruleset is valid"
	actual, err := validateRuleSet(newTestService(nil), newTestPostRequest([]byte(`{}`)))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateNamespaceSuccess(t *testing.T) {
	expected := view.Namespace{}
	actual, err := createNamespace(newTestService(nil), newTestPostRequest([]byte(`{"id": "id"}`)))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteNamespaceSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted namespace %s", "")
	actual, err := deleteNamespace(newTestService(nil), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchMappingRuleSuccess(t *testing.T) {
	expected := view.MappingRule{}
	actual, err := fetchMappingRule(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateMappingRuleSuccess(t *testing.T) {
	expected := view.MappingRule{}
	actual, err := createMappingRule(newTestService(nil), newTestPostRequest(
		[]byte(`{"filter": "key:val", "name": "name", "policies": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestUpdateMappingRuleSuccess(t *testing.T) {
	expected := view.MappingRule{}
	actual, err := updateMappingRule(newTestService(nil), newTestPutRequest(
		[]byte(`{"filter": "key:val", "name": "name", "policies": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteMappingRuleSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted mapping rule: %s in namespace %s", "", "")
	actual, err := deleteMappingRule(newTestService(nil), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchMappingRuleHistorySuccess(t *testing.T) {
	expected := view.MappingRuleSnapshots{MappingRules: make([]view.MappingRule, 0)}
	actual, err := fetchMappingRuleHistory(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchRollupRuleSuccess(t *testing.T) {
	expected := view.RollupRule{}
	actual, err := fetchRollupRule(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateRollupRuleSuccess(t *testing.T) {
	expected := view.RollupRule{}
	actual, err := createRollupRule(newTestService(nil), newTestPostRequest(
		[]byte(`{"filter": "key:val", "name": "name", "targets": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestUpdateRollupRuleSuccess(t *testing.T) {
	expected := view.RollupRule{}
	actual, err := updateRollupRule(newTestService(nil), newTestPutRequest(
		[]byte(`{"filter": "key:val", "name": "name", "targets": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteRollupRuleSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted rollup rule: %s in namespace %s", "", "")
	actual, err := deleteRollupRule(newTestService(nil), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchRollupRuleHistorySuccess(t *testing.T) {
	expected := view.RollupRuleSnapshots{RollupRules: []view.RollupRule{}}
	actual, err := fetchRollupRuleHistory(newTestService(nil), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestRulesetUpdateRuleSet(t *testing.T) {
	namespaceID := "testNamespace"
	bulkReqBody := newTestBulkReqBody()
	bodyBytes, err := json.Marshal(bulkReqBody)
	require.NoError(t, err)
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/namespaces/%s/ruleset/update", namespaceID),
		bytes.NewBuffer(bodyBytes),
	)
	require.NoError(t, err)
	req = mux.SetURLVars(
		req,
		map[string]string{
			"namespaceID": namespaceID,
		},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().UpdateRuleSet(gomock.Any(), 1, gomock.Any()).Return(
		view.RuleSet{
			Version: 2,
		},
		nil,
	)

	service := newTestService(storeMock)
	resp, err := updateRuleSet(service, req)
	require.NoError(t, err)
	typedResp := resp.(view.RuleSet)
	require.Equal(t, typedResp.Version, 2)
}

func TestUpdateRuleSetStoreUpdateFailure(t *testing.T) {
	namespaceID := "testNamespace"
	bulkReqBody := newTestBulkReqBody()
	bodyBytes, err := json.Marshal(bulkReqBody)
	require.NoError(t, err)
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/namespaces/%s/ruleset/bulk", namespaceID),
		bytes.NewBuffer(bodyBytes),
	)
	require.NoError(t, err)
	req = mux.SetURLVars(
		req,
		map[string]string{
			"namespaceID": namespaceID,
		},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeMock := store.NewMockStore(ctrl)
	storeMock.EXPECT().UpdateRuleSet(gomock.Any(), 1, gomock.Any()).Return(
		view.RuleSet{},
		NewConflictError("something horrible has happened"),
	)

	service := newTestService(storeMock)
	resp, err := updateRuleSet(service, req)
	require.Equal(t, view.RuleSet{}, resp)
	require.Error(t, err)
	require.IsType(t, NewConflictError(""), err)
}

func TestUpdateRuleSetInvalidJSON(t *testing.T) {
	namespaceID := "testNamespace"
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/namespaces/%s/ruleset/bulk", namespaceID),
		bytes.NewBuffer([]byte("invalid josn")),
	)
	require.NoError(t, err)
	req = mux.SetURLVars(
		req,
		map[string]string{
			"namespaceID": namespaceID,
		},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeMock := store.NewMockStore(ctrl)
	service := newTestService(storeMock)
	resp, err := updateRuleSet(service, req)
	require.Nil(t, resp)
	require.Error(t, err)
	require.IsType(t, NewBadInputError(""), err)
}

func TestUpdateRuleSetEmptyRequest(t *testing.T) {
	namespaceID := "testNamespace"
	body := &updateRuleSetRequest{
		RuleSetChanges: changes.RuleSetChanges{},
	}
	bodyBytes, err := json.Marshal(body)
	require.NoError(t, err)
	req, err := http.NewRequest(
		http.MethodPost,
		fmt.Sprintf("/namespaces/%s/ruleset/bulk", namespaceID),
		bytes.NewBuffer(bodyBytes),
	)
	require.NoError(t, err)
	req = mux.SetURLVars(
		req,
		map[string]string{
			"namespaceID": namespaceID,
		},
	)

	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	storeMock := store.NewMockStore(ctrl)
	service := newTestService(storeMock)
	resp, err := updateRuleSet(service, req)
	require.Nil(t, resp)
	require.Error(t, err)
	require.IsType(t, NewBadInputError(""), err)
	println(err.Error())
}

func newTestService(store store.Store) *service {
	if store == nil {
		store = newMockStore()
	}
	iOpts := instrument.NewOptions()
	return &service{
		metrics:     newServiceMetrics(iOpts.MetricsScope(), iOpts.MetricsSamplingRate()),
		nowFn:       clock.NewOptions().NowFn(),
		store:       store,
		authService: auth.NewNoopAuth(),
		logger:      iOpts.Logger(),
	}
}

func newTestGetRequest() *http.Request {
	req, _ := http.NewRequest("GET", "/route", nil)
	return req.WithContext(context.Background())
}

func newTestPostRequest(bodyBuff []byte) *http.Request {
	req, _ := http.NewRequest("POST", "/route", bytes.NewReader(bodyBuff))
	return req.WithContext(context.Background())
}

func newTestPutRequest(bodyBuff []byte) *http.Request {
	req, _ := http.NewRequest("PUT", "/route", bytes.NewReader(bodyBuff))
	return req.WithContext(context.Background())
}

func newTestDeleteRequest() *http.Request {
	req, _ := http.NewRequest("DELETE", "/route", nil)
	return req.WithContext(context.Background())
}

func newTestInstrumentMethodMetrics() instrument.MethodMetrics {
	return instrument.NewMethodMetrics(tally.NoopScope, "testRoute", 1.0)
}

func newTestBulkReqBody() updateRuleSetRequest {
	return updateRuleSetRequest{
		RuleSetVersion: 1,
		RuleSetChanges: changes.RuleSetChanges{
			Namespace: "testNamespace",
			RollupRuleChanges: []changes.RollupRuleChange{
				changes.RollupRuleChange{
					Op: changes.AddOp,
					RuleData: &view.RollupRule{
						Name: "rollupRule3",
					},
				},
			},
			MappingRuleChanges: []changes.MappingRuleChange{
				changes.MappingRuleChange{
					Op: changes.AddOp,
					RuleData: &view.MappingRule{
						Name: "mappingRule3",
					},
				},
			},
		},
	}
}

// nolint: unparam
func newTestRuleSet(version int) rules.RuleSet {
	helper := rules.NewRuleSetUpdateHelper(time.Minute)
	// For testing all updates happen at the 0 epoch
	meta := helper.NewUpdateMetadata(0, "originalAuthor")

	mrs := rules.NewEmptyRuleSet("testNamespace", rules.UpdateMetadata{})
	mrs.AddRollupRule(
		view.RollupRule{
			Name: "rollupRule1",
		},
		meta,
	)
	mrs.AddRollupRule(
		view.RollupRule{
			Name: "rollupRule2",
		},
		meta,
	)
	mrs.AddMappingRule(
		view.MappingRule{
			Name: "mappingRule1",
		},
		meta,
	)
	mrs.AddMappingRule(
		view.MappingRule{
			Name: "mappingRule2",
		},
		meta,
	)
	proto, _ := mrs.Proto()
	rs, _ := rules.NewRuleSetFromProto(version, proto, rules.NewOptions())
	return rs
}

// TODO(jskelcy): Migrate to use mockgen mock for testing.
type mockStore struct{}

func newMockStore() store.Store {
	return mockStore{}
}

func (s mockStore) FetchNamespaces() (view.Namespaces, error) {
	return view.Namespaces{}, nil
}

func (s mockStore) ValidateRuleSet(rs view.RuleSet) error {
	return nil
}

func (s mockStore) UpdateRuleSet(rsChanges changes.RuleSetChanges, version int, uOpts store.UpdateOptions) (view.RuleSet, error) {
	return view.RuleSet{}, nil
}

func (s mockStore) CreateNamespace(namespaceID string, uOpts store.UpdateOptions) (view.Namespace, error) {
	return view.Namespace{}, nil
}

func (s mockStore) DeleteNamespace(namespaceID string, uOpts store.UpdateOptions) error {
	return nil
}

//nolint: unparam
func (s mockStore) FetchRuleSet(namespaceID string) (rules.RuleSet, error) {
	return newTestRuleSet(1), nil
}

func (s mockStore) FetchRuleSetSnapshot(namespaceID string) (view.RuleSet, error) {
	return view.RuleSet{}, nil
}

func (s mockStore) FetchMappingRule(namespaceID, mappingRuleID string) (view.MappingRule, error) {
	return view.MappingRule{}, nil
}

func (s mockStore) CreateMappingRule(namespaceID string, mrv view.MappingRule, uOpts store.UpdateOptions) (view.MappingRule, error) {
	return view.MappingRule{}, nil
}

func (s mockStore) UpdateMappingRule(namespaceID, mappingRuleID string, mrv view.MappingRule, uOpts store.UpdateOptions) (view.MappingRule, error) {
	return view.MappingRule{}, nil
}

func (s mockStore) DeleteMappingRule(namespaceID, mappingRuleID string, uOpts store.UpdateOptions) error {
	return nil
}

func (s mockStore) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]view.MappingRule, error) {
	return make([]view.MappingRule, 0), nil
}

func (s mockStore) FetchRollupRule(namespaceID, rollupRuleID string) (view.RollupRule, error) {
	return view.RollupRule{}, nil
}

func (s mockStore) CreateRollupRule(namespaceID string, rrv view.RollupRule, uOpts store.UpdateOptions) (view.RollupRule, error) {
	return view.RollupRule{}, nil
}

func (s mockStore) UpdateRollupRule(namespaceID, rollupRuleID string, rrv view.RollupRule, uOpts store.UpdateOptions) (view.RollupRule, error) {
	return view.RollupRule{}, nil
}

func (s mockStore) DeleteRollupRule(namespaceID, rollupRuleID string, uOpts store.UpdateOptions) error {
	return nil
}

func (s mockStore) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]view.RollupRule, error) {
	return make([]view.RollupRule, 0), nil
}

func (s mockStore) Close() {}
