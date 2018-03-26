package handler

import (
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/services"

	"github.com/golang/mock/gomock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPlacementGetHandler(t *testing.T) {
	ctrl := gomock.NewController(t)
	defer ctrl.Finish()
	mockClient := client.NewMockClient(ctrl)
	require.NotNil(t, mockClient)
	mockServices := services.NewMockServices(ctrl)
	require.NotNil(t, mockServices)
	mockPlacementService := placement.NewMockService(ctrl)
	require.NotNil(t, mockPlacementService)

	handler := NewPlacementGetHandler(mockClient)
	w := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/placement/get", nil)
	require.NoError(t, err)

	mockClient.EXPECT().Services(gomock.Not(nil)).Return(mockServices, nil)
	mockServices.EXPECT().PlacementService(gomock.Not(nil), gomock.Not(nil)).Return(mockPlacementService, nil)
	mockPlacementService.EXPECT().Placement().Return(placement.NewPlacement(), 0, nil)
	handler.ServeHTTP(w, req)

	resp := w.Result()
	body, _ := ioutil.ReadAll(resp.Body)
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "{\"placement\":{}}", string(body))
}
