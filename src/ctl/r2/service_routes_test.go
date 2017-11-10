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
	"fmt"
	"net/http"
	"testing"

	"github.com/m3db/m3ctl/auth"
	"github.com/m3db/m3metrics/rules"
	"github.com/m3db/m3x/clock"
	"github.com/m3db/m3x/instrument"

	"github.com/stretchr/testify/require"
)

func TestHandleRoute(t *testing.T) {
	s := newTestService()
	r := newTestGetRequest()
	expected := newNamespacesJSON(&rules.NamespacesView{})
	actual, err := s.handleRoute(fetchNamespaces, r, "ns")
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestHandleRouteNilRequest(t *testing.T) {
	s := newTestService()
	_, err := s.handleRoute(fetchNamespaces, nil, "ns")
	require.EqualError(t, err, errNilRequest.Error())
}
func TestFetchNamespacesSuccess(t *testing.T) {
	expected := newNamespacesJSON(&rules.NamespacesView{})
	actual, err := fetchNamespaces(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchNamespaceSuccess(t *testing.T) {
	expected := newRuleSetJSON(&rules.RuleSetSnapshot{})
	actual, err := fetchNamespace(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateNamespaceSuccess(t *testing.T) {
	expected := newNamespaceJSON(&rules.NamespaceView{})
	actual, err := createNamespace(newTestService(), newTestPostRequest([]byte(`{"id": "id"}`)))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteNamespaceSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted namespace %s", "")
	actual, err := deleteNamespace(newTestService(), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchMappingRuleSuccess(t *testing.T) {
	expected := newMappingRuleJSON(&rules.MappingRuleView{})
	actual, err := fetchMappingRule(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateMappingRuleSuccess(t *testing.T) {
	expected := newMappingRuleJSON(&rules.MappingRuleView{})
	actual, err := createMappingRule(newTestService(), newTestPostRequest(
		[]byte(`{"filter": "key:val", "name": "name", "policies": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestUpdateMappingRuleSuccess(t *testing.T) {
	expected := newMappingRuleJSON(&rules.MappingRuleView{})
	actual, err := updateMappingRule(newTestService(), newTestPutRequest(
		[]byte(`{"filter": "key:val", "name": "name", "policies": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteMappingRuleSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted mapping rule: %s in namespace %s", "", "")
	actual, err := deleteMappingRule(newTestService(), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchMappingRuleHistorySuccess(t *testing.T) {
	expected := newMappingRuleHistoryJSON(make([]*rules.MappingRuleView, 0))
	actual, err := fetchMappingRuleHistory(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchRollupRuleSuccess(t *testing.T) {
	expected := newRollupRuleJSON(&rules.RollupRuleView{})
	actual, err := fetchRollupRule(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestCreateRollupRuleSuccess(t *testing.T) {
	expected := newRollupRuleJSON(&rules.RollupRuleView{})
	actual, err := createRollupRule(newTestService(), newTestPostRequest(
		[]byte(`{"filter": "key:val", "name": "name", "targets": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestUpdateRollupRuleSuccess(t *testing.T) {
	expected := newRollupRuleJSON(&rules.RollupRuleView{})
	actual, err := updateRollupRule(newTestService(), newTestPutRequest(
		[]byte(`{"filter": "key:val", "name": "name", "targets": []}`),
	))
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestDeleteRollupRuleSuccess(t *testing.T) {
	expected := fmt.Sprintf("Deleted rollup rule: %s in namespace %s", "", "")
	actual, err := deleteRollupRule(newTestService(), newTestDeleteRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func TestFetchRollupRuleHistorySuccess(t *testing.T) {
	expected := newRollupRuleHistoryJSON([]*rules.RollupRuleView{})
	actual, err := fetchRollupRuleHistory(newTestService(), newTestGetRequest())
	require.NoError(t, err)
	require.Equal(t, expected, actual)
}

func newTestService() *service {
	iOpts := instrument.NewOptions()
	return &service{
		metrics:     newServiceMetrics(iOpts.MetricsScope()),
		nowFn:       clock.NewOptions().NowFn(),
		store:       newMockStore(),
		authService: auth.NewNoopAuth(),
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

type mockStore struct{}

func newMockStore() Store {
	return mockStore{}
}

func (s mockStore) FetchNamespaces() (*rules.NamespacesView, error) {
	return &rules.NamespacesView{}, nil
}

func (s mockStore) CreateNamespace(namespaceID string, uOpts UpdateOptions) (*rules.NamespaceView, error) {
	return &rules.NamespaceView{}, nil
}

func (s mockStore) DeleteNamespace(namespaceID string, uOpts UpdateOptions) error {
	return nil
}

func (s mockStore) FetchRuleSet(namespaceID string) (*rules.RuleSetSnapshot, error) {
	return &rules.RuleSetSnapshot{}, nil
}

func (s mockStore) FetchMappingRule(namespaceID, mappingRuleID string) (*rules.MappingRuleView, error) {
	return &rules.MappingRuleView{}, nil
}

func (s mockStore) CreateMappingRule(namespaceID string, mrv *rules.MappingRuleView, uOpts UpdateOptions) (*rules.MappingRuleView, error) {
	return &rules.MappingRuleView{}, nil
}

func (s mockStore) UpdateMappingRule(namespaceID, mappingRuleID string, mrv *rules.MappingRuleView, uOpts UpdateOptions) (*rules.MappingRuleView, error) {
	return &rules.MappingRuleView{}, nil
}

func (s mockStore) DeleteMappingRule(namespaceID, mappingRuleID string, uOpts UpdateOptions) error {
	return nil
}

func (s mockStore) FetchMappingRuleHistory(namespaceID, mappingRuleID string) ([]*rules.MappingRuleView, error) {
	return make([]*rules.MappingRuleView, 0), nil
}

func (s mockStore) FetchRollupRule(namespaceID, rollupRuleID string) (*rules.RollupRuleView, error) {
	return &rules.RollupRuleView{}, nil
}

func (s mockStore) CreateRollupRule(namespaceID string, rrv *rules.RollupRuleView, uOpts UpdateOptions) (*rules.RollupRuleView, error) {
	return &rules.RollupRuleView{}, nil
}

func (s mockStore) UpdateRollupRule(namespaceID, rollupRuleID string, rrv *rules.RollupRuleView, uOpts UpdateOptions) (*rules.RollupRuleView, error) {
	return &rules.RollupRuleView{}, nil
}

func (s mockStore) DeleteRollupRule(namespaceID, rollupRuleID string, uOpts UpdateOptions) error {
	return nil
}

func (s mockStore) FetchRollupRuleHistory(namespaceID, rollupRuleID string) ([]*rules.RollupRuleView, error) {
	return make([]*rules.RollupRuleView, 0), nil
}
