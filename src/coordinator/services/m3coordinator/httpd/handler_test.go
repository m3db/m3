package httpd

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/m3db/m3coordinator/executor"
	"github.com/m3db/m3coordinator/policy/resolver"
	"github.com/m3db/m3coordinator/services/m3coordinator/handler"
	"github.com/m3db/m3coordinator/storage/local"
	"github.com/m3db/m3coordinator/util/logging"

	"github.com/m3db/m3metrics/policy"
	xtime "github.com/m3db/m3x/time"

	"github.com/stretchr/testify/require"
)

func TestPromReadGet(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("GET", handler.PromReadURL, nil)
	res := httptest.NewRecorder()
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))

	h, err := NewHandler(storage, executor.NewEngine(storage), nil)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusMethodNotAllowed, "GET method not defined")
}

func TestPromReadPost(t *testing.T) {
	logging.InitWithCores(nil)

	req, _ := http.NewRequest("POST", handler.PromReadURL, nil)
	res := httptest.NewRecorder()
	storage := local.NewStorage(nil, "metrics", resolver.NewStaticResolver(policy.NewStoragePolicy(time.Second, xtime.Second, time.Hour*48)))

	h, err := NewHandler(storage, executor.NewEngine(storage), nil)
	require.NoError(t, err, "unable to setup handler")
	h.RegisterRoutes()
	h.Router.ServeHTTP(res, req)
	require.Equal(t, res.Code, http.StatusBadRequest, "Empty request")
}
