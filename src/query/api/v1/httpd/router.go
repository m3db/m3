package httpd

import (
	"net/http"
	"strings"

	"github.com/m3db/m3/src/query/api/v1/options"
)

type QueryRouter interface {
	Setup(opts QueryRouterOptions)
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

type QueryRouterOptions struct {
	DefaultQueryEngine options.QueryEngine
	PromqlHandler      func(http.ResponseWriter, *http.Request)
	M3QueryHandler     func(http.ResponseWriter, *http.Request)
}

type router struct {
	promqlHandler      func(http.ResponseWriter, *http.Request)
	m3QueryHandler     func(http.ResponseWriter, *http.Request)
	defaultQueryEngine options.QueryEngine
}

func NewQueryRouter() QueryRouter {
	return &router{}
}

func (r *router) Setup(opts QueryRouterOptions) {
	defaultEngine := opts.DefaultQueryEngine
	if defaultEngine != options.PrometheusEngine && defaultEngine != options.M3QueryEngine {
		defaultEngine = options.PrometheusEngine
	}

	r.defaultQueryEngine = defaultEngine
	r.promqlHandler = opts.PromqlHandler
	r.m3QueryHandler = opts.M3QueryHandler
}

func (r *router) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	engine := strings.ToLower(req.Header.Get(EngineHeaderName))
	urlParam := strings.ToLower(req.URL.Query().Get(EngineURLParam))

	if len(urlParam) > 0 {
		engine = urlParam
	}

	if !options.IsQueryEngineSet(engine) {
		engine = string(r.defaultQueryEngine)
	}

	w.Header().Add(EngineHeaderName, engine)

	if engine == string(options.M3QueryEngine) {
		r.m3QueryHandler(w, req)
		return
	}

	r.promqlHandler(w, req)
}
