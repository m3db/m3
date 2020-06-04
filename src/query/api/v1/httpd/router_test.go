package httpd

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestHanlersSwitch(t *testing.T) {
	promqlCalled := 0
	promqlHandler := func(w http.ResponseWriter, req *http.Request) {
		promqlCalled++
	}

	m3qCalled := 0
	m3qHandler := func(w http.ResponseWriter, req *http.Request) {
		m3qCalled++
	}

	router := NewQueryRouter()
	router.Setup(QueryRouterOptions{
		DefaultQueryEngine: "prometheus",
		PromqlHandler:      promqlHandler,
		M3QueryHandler:     m3qHandler,
	})
	rr := httptest.NewRecorder()

	req, err := http.NewRequest("GET", "/query?query=sum(metric)", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 0, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)&engine=m3query", nil)
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 1, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(EngineHeaderName, "m3query")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 2, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(EngineHeaderName, "M3QUERY")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 1, promqlCalled)
	assert.Equal(t, 3, m3qCalled)

	req, err = http.NewRequest("GET", "/query?query=sum(metric)", nil)
	req.Header.Add(EngineHeaderName, "prometheus")
	require.NoError(t, err)
	router.ServeHTTP(rr, req)
	assert.Equal(t, 2, promqlCalled)
	assert.Equal(t, 3, m3qCalled)
}
