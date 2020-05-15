package prom

import (
	"errors"
	"fmt"
	"net/http"

	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/native"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/remote"
	"github.com/m3db/m3/src/query/api/v1/options"
	"github.com/m3db/m3/src/query/storage/prometheus"
	"github.com/prometheus/prometheus/promql"
)

const prometheusHandlerPrefix = "/prometheus"

// Options areoptions for the prometheus read handlers.
type Options struct {
	PromQLEngine *promql.Engine
}

// NewPrometheusReadHandlers creates new prometheus query and query_range
// handlers.
func NewPrometheusReadHandlers(opts Options) ([]options.CustomHandler, error) {
	return []options.CustomHandler{
		newPrometheusReadHandler(opts, true),
		newPrometheusReadHandler(opts, false),
		&prometheusListHandler{},
		&prometheusTagValuesHandler{},
		&prometheusMatchHandler{},
	}, nil
}

func (o Options) validate() error {
	if o.PromQLEngine == nil {
		return errors.New("promql query engine not provided")
	}
	return nil
}

type promHandler struct {
	opts    Options
	instant bool
}

func newPrometheusReadHandler(opts Options, instant bool) options.CustomHandler {
	return &promHandler{opts, instant}
}

func (h *promHandler) Route() string {
	if h.instant {
		return fmt.Sprintf("%s%s", prometheusHandlerPrefix, native.PromReadInstantURL)
	}
	return fmt.Sprintf("%s%s", prometheusHandlerPrefix, native.PromReadURL)
}

func (h *promHandler) Methods() []string {
	if h.instant {
		return native.PromReadInstantHTTPMethods
	}
	return native.PromReadHTTPMethods
}

func (h *promHandler) Handler(
	opts options.HandlerOptions,
) (http.Handler, error) {
	fmt.Printf("STORAGE: %+v %T\n", opts.Storage(), opts.Storage())
	queryable := prometheus.NewPrometheusQueryable(
		prometheus.PrometheusOptions{
			Storage:           opts.Storage(),
			InstrumentOptions: opts.InstrumentOpts(),
		})
	if h.instant {
		return newReadInstantHandler(h.opts, opts, queryable), nil
	}
	return newReadHandler(h.opts, opts, queryable), nil
}

// query list handler follows
type prometheusListHandler struct{}

func (*prometheusListHandler) Route() string {
	return fmt.Sprintf("%s%s", prometheusHandlerPrefix, native.ListTagsURL)
}

func (*prometheusListHandler) Methods() []string {
	return native.ListTagsHTTPMethods
}

func (*prometheusListHandler) Handler(
	opts options.HandlerOptions,
) (http.Handler, error) {
	return native.NewListTagsHandler(opts), nil
}

// query list tag value handler follows
type prometheusTagValuesHandler struct{}

func (*prometheusTagValuesHandler) Route() string {
	return fmt.Sprintf("%s%s", prometheusHandlerPrefix, remote.TagValuesURL)
}

func (*prometheusTagValuesHandler) Methods() []string {
	return []string{remote.TagValuesHTTPMethod}
}

func (*prometheusTagValuesHandler) Handler(
	opts options.HandlerOptions,
) (http.Handler, error) {
	return remote.NewTagValuesHandler(opts), nil
}

// query match handler follows
type prometheusMatchHandler struct{}

func (*prometheusMatchHandler) Route() string {
	return fmt.Sprintf("%s%s", prometheusHandlerPrefix, remote.PromSeriesMatchURL)
}

func (*prometheusMatchHandler) Methods() []string {
	return remote.PromSeriesMatchHTTPMethods
}

func (*prometheusMatchHandler) Handler(
	opts options.HandlerOptions,
) (http.Handler, error) {
	return remote.NewPromSeriesMatchHandler(opts), nil
}
