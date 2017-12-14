package httpd

import (
	"net/http/httptest"
	"testing"
	"net/http"

	"github.com/m3db/m3coordinator/util/logging"

	"github.com/stretchr/testify/require"
)

func TestPromReadGet(t *testing.T) {
	logging.InitWithCores(nil)
	req, _ := http.NewRequest("GET", promReadURL, nil)
	res := httptest.NewRecorder()
	handler, err := NewHandler()
	require.Nil(t, err, "unable to setup handler")
	handler.RegisterRoutes()
	handler.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "GET method not defined")
}

func TestPromReadPost(t *testing.T) {
	logging.InitWithCores(nil)
	req, _ := http.NewRequest("POST", promReadURL, nil)
	res := httptest.NewRecorder()
	handler, err := NewHandler()
	require.Nil(t, err, "unable to setup handler")
	handler.RegisterRoutes()
	handler.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}
