// Copyright (c) 2019 Uber Technologies, Inc.
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

// Package options configures query http handlers.
package options

import (
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/prometheus/prometheus/promql"
	"google.golang.org/protobuf/runtime/protoiface"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	placementhandleroptions "github.com/m3db/m3/src/cluster/placementhandler/handleroptions"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/encoding"
	dbnamespace "github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/middleware"
	"github.com/m3db/m3/src/query/api/v1/validators"
	"github.com/m3db/m3/src/query/executor"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"
)

// QueryEngine is a type of query engine.
type QueryEngine string

const (
	// PrometheusEngine is the prometheus query engine type.
	PrometheusEngine QueryEngine = "prometheus"
	// M3QueryEngine is M3 query engine type.
	M3QueryEngine QueryEngine = "m3query"
)

// PromQLEngineFn constructs promql.Engine with the given lookbackDuration. promql.Engine uses
// a fixed lookback, so we have to create multiple engines for different lookback values.
//
// TODO(vilius): there's a conversation at Prometheus mailing list about making lookback dynamic
//   https://groups.google.com/g/prometheus-developers/c/9wzuobfLMV8
type PromQLEngineFn func(lookbackDuration time.Duration) (*promql.Engine, error)

// OptionTransformFn transforms given handler options.
type OptionTransformFn func(opts HandlerOptions) HandlerOptions

// CustomHandlerOptions is a list of custom handler options.
type CustomHandlerOptions struct {
	CustomHandlers    []CustomHandler
	OptionTransformFn OptionTransformFn
}

// CustomHandler allows for custom third party http handlers.
type CustomHandler interface {
	// Route is the custom handler route.
	Route() string
	// Methods is the list of http methods this handler services.
	Methods() []string
	// Handler is the custom handler itself.
	// prev is optional argument for getting already registered handler for the same route.
	// If there is nothing to override, prev will be nil.
	Handler(handlerOptions HandlerOptions, prev http.Handler) (http.Handler, error)
	// MiddlewareOverride is a function to override the global middleware configuration for the route.
	// If this CustomHandler is overriding an existing handler, the MiddlewareOverride for the existing handler is first
	// applied before applying this function.
	MiddlewareOverride() middleware.OverrideOptions
}

// QueryRouter is responsible for routing queries between promql and m3query.
type QueryRouter interface {
	Setup(opts QueryRouterOptions)
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

// QueryRouterOptions defines options for QueryRouter
type QueryRouterOptions struct {
	DefaultQueryEngine QueryEngine
	PromqlHandler      func(http.ResponseWriter, *http.Request)
	M3QueryHandler     func(http.ResponseWriter, *http.Request)
}

// GraphiteRenderRouter is responsible for routing graphite render queries.
type GraphiteRenderRouter interface {
	Setup(opts GraphiteRenderRouterOptions)
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

// GraphiteRenderRouterOptions defines options for the graphite render router.
type GraphiteRenderRouterOptions struct {
	RenderHandler func(http.ResponseWriter, *http.Request)
}

// GraphiteFindRouter is responsible for routing graphite find queries.
type GraphiteFindRouter interface {
	Setup(opts GraphiteFindRouterOptions)
	ServeHTTP(w http.ResponseWriter, req *http.Request)
}

// GraphiteFindRouterOptions defines options for graphite find router
type GraphiteFindRouterOptions struct {
	FindHandler func(http.ResponseWriter, *http.Request)
}

// RemoteReadRenderer renders remote read output.
type RemoteReadRenderer func(io.Writer, []*ts.Series,
	models.RequestParams, bool)

// HandlerOptions represents handler options.
type HandlerOptions interface {
	// CreatedAt returns the time the options were created.
	CreatedAt() time.Time

	// Storage returns the set storage.
	Storage() storage.Storage
	// SetStorage sets the set storage.
	SetStorage(s storage.Storage) HandlerOptions

	// DownsamplerAndWriter returns the set downsampler and writer.
	DownsamplerAndWriter() ingest.DownsamplerAndWriter
	// SetDownsamplerAndWriter sets the set downsampler and writer.
	SetDownsamplerAndWriter(d ingest.DownsamplerAndWriter) HandlerOptions

	// Engine returns the engine.
	Engine() executor.Engine
	// SetEngine sets the engine.
	SetEngine(e executor.Engine) HandlerOptions

	// PrometheusEngineFn returns the function for Prometheus engine creation.
	PrometheusEngineFn() PromQLEngineFn
	// SetPrometheusEngineFn sets the function for Prometheus engine creation.
	SetPrometheusEngineFn(fn PromQLEngineFn) HandlerOptions

	// Clusters returns the clusters.
	Clusters() m3.Clusters
	// SetClusters sets the clusters.
	SetClusters(c m3.Clusters) HandlerOptions

	// ClusterClient returns the cluster client.
	ClusterClient() clusterclient.Client
	// SetClusterClient sets the cluster client.
	SetClusterClient(c clusterclient.Client) HandlerOptions

	// Config returns the config.
	Config() config.Configuration
	// SetConfig sets the config.
	SetConfig(c config.Configuration) HandlerOptions

	// EmbeddedDBCfg returns the embedded db config.
	EmbeddedDBCfg() *dbconfig.DBConfiguration
	// SetEmbeddedDBCfg sets the embedded db config.
	SetEmbeddedDBCfg(c *dbconfig.DBConfiguration) HandlerOptions

	// TagOptions returns the tag options.
	TagOptions() models.TagOptions
	// SetTagOptions sets the tag options.
	SetTagOptions(opts models.TagOptions) HandlerOptions

	// FetchOptionsBuilder returns the fetch options builder.
	FetchOptionsBuilder() handleroptions.FetchOptionsBuilder
	// SetFetchOptionsBuilder sets the fetch options builder.
	SetFetchOptionsBuilder(b handleroptions.FetchOptionsBuilder) HandlerOptions

	// QueryContextOptions returns the query context options.
	QueryContextOptions() models.QueryContextOptions
	// SetQueryContextOptions sets the query context options.
	SetQueryContextOptions(o models.QueryContextOptions) HandlerOptions

	// CPUProfileDuration returns the cpu profile duration.
	CPUProfileDuration() time.Duration
	// SetCPUProfileDuration sets the cpu profile duration.
	SetCPUProfileDuration(c time.Duration) HandlerOptions

	// PlacementServiceNames returns the placement service names.
	PlacementServiceNames() []string
	// SetPlacementServiceNames sets the placement service names.
	SetPlacementServiceNames(n []string) HandlerOptions

	// ServiceOptionDefaults returns the service option defaults.
	ServiceOptionDefaults() []placementhandleroptions.ServiceOptionsDefault
	// SetServiceOptionDefaults sets the service option defaults.
	SetServiceOptionDefaults(s []placementhandleroptions.ServiceOptionsDefault) HandlerOptions

	// NowFn returns the now function.
	NowFn() clock.NowFn
	// SetNowFn sets the now function.
	SetNowFn(f clock.NowFn) HandlerOptions

	// InstrumentOpts returns the instrumentation options.
	InstrumentOpts() instrument.Options
	// SetInstrumentOpts sets instrumentation options.
	SetInstrumentOpts(opts instrument.Options) HandlerOptions

	// DefaultQueryEngine returns the default query engine.
	DefaultQueryEngine() QueryEngine
	// SetDefaultQueryEngine returns the default query engine.
	SetDefaultQueryEngine(value QueryEngine) HandlerOptions

	// QueryRouter is a reference to the router which is responsible for routing
	// queries between PromQL and M3Query.
	QueryRouter() QueryRouter
	// SetQueryRouter sets query router.
	SetQueryRouter(value QueryRouter) HandlerOptions

	// InstantQueryRouter is a reference to the router which is responsible for
	// routing instant queries between PromQL and M3Query.
	InstantQueryRouter() QueryRouter
	// SetInstantQueryRouter sets query router for instant queries.
	SetInstantQueryRouter(value QueryRouter) HandlerOptions

	// GraphiteStorageOptions returns the Graphite storage options.
	GraphiteStorageOptions() graphite.M3WrappedStorageOptions
	// SetGraphiteStorageOptions sets the Graphite storage options.
	SetGraphiteStorageOptions(value graphite.M3WrappedStorageOptions) HandlerOptions

	// GraphiteFindFetchOptionsBuilder returns the Graphite find fetch options builder.
	GraphiteFindFetchOptionsBuilder() handleroptions.FetchOptionsBuilder
	// SetGraphiteFindFetchOptionsBuilder sets the Graphite find fetch options builder.
	SetGraphiteFindFetchOptionsBuilder(value handleroptions.FetchOptionsBuilder) HandlerOptions

	// GraphiteRenderFetchOptionsBuilder returns the Graphite render fetch options builder.
	GraphiteRenderFetchOptionsBuilder() handleroptions.FetchOptionsBuilder
	// SetGraphiteRenderFetchOptionsBuilder sets the Graphite render fetch options builder.
	SetGraphiteRenderFetchOptionsBuilder(value handleroptions.FetchOptionsBuilder) HandlerOptions

	// GraphiteRenderRouter is a reference to the router for graphite render queries.
	GraphiteRenderRouter() GraphiteRenderRouter
	// SetGraphiteRenderRouter sets the graphite render router.
	SetGraphiteRenderRouter(value GraphiteRenderRouter) HandlerOptions

	// GraphiteFindRouter is a reference to the router for graphite find queries.
	GraphiteFindRouter() GraphiteFindRouter
	// SetGraphiteFindRouter sets the graphite find router.
	SetGraphiteFindRouter(value GraphiteFindRouter) HandlerOptions

	// SetM3DBOptions sets the M3DB options.
	SetM3DBOptions(value m3.Options) HandlerOptions
	// M3DBOptions returns the M3DB options.
	M3DBOptions() m3.Options

	// SetStoreMetricsType enables/disables storing of metrics type.
	SetStoreMetricsType(value bool) HandlerOptions
	// StoreMetricsType returns true if storing of metrics type is enabled.
	StoreMetricsType() bool

	// SetNamespaceValidator sets the NamespaceValidator.
	SetNamespaceValidator(NamespaceValidator) HandlerOptions
	// NamespaceValidator returns the NamespaceValidator.
	NamespaceValidator() NamespaceValidator

	// SetKVStoreProtoParser sets the KVStoreProtoParser.
	SetKVStoreProtoParser(KVStoreProtoParser) HandlerOptions
	// KVStoreProtoParser returns the KVStoreProtoParser.
	KVStoreProtoParser() KVStoreProtoParser

	// SetRegisterMiddleware sets the function to construct the set of Middleware functions to run.
	SetRegisterMiddleware(value middleware.Register) HandlerOptions
	// RegisterMiddleware returns the function to construct the set of Middleware functions to run.
	RegisterMiddleware() middleware.Register

	// DefaultLookback returns the default value of lookback duration.
	DefaultLookback() time.Duration
	// SetDefaultLookback sets the default value of lookback duration.
	SetDefaultLookback(value time.Duration) HandlerOptions
}

// HandlerOptions represents handler options.
type handlerOptions struct {
	storage                           storage.Storage
	downsamplerAndWriter              ingest.DownsamplerAndWriter
	engine                            executor.Engine
	prometheusEngineFn                PromQLEngineFn
	defaultEngine                     QueryEngine
	clusters                          m3.Clusters
	clusterClient                     clusterclient.Client
	config                            config.Configuration
	embeddedDBCfg                     *dbconfig.DBConfiguration
	createdAt                         time.Time
	tagOptions                        models.TagOptions
	fetchOptionsBuilder               handleroptions.FetchOptionsBuilder
	queryContextOptions               models.QueryContextOptions
	instrumentOpts                    instrument.Options
	cpuProfileDuration                time.Duration
	placementServiceNames             []string
	serviceOptionDefaults             []placementhandleroptions.ServiceOptionsDefault
	nowFn                             clock.NowFn
	queryRouter                       QueryRouter
	instantQueryRouter                QueryRouter
	graphiteStorageOpts               graphite.M3WrappedStorageOptions
	graphiteFindFetchOptionsBuilder   handleroptions.FetchOptionsBuilder
	graphiteRenderFetchOptionsBuilder handleroptions.FetchOptionsBuilder
	m3dbOpts                          m3.Options
	namespaceValidator                NamespaceValidator
	storeMetricsType                  bool
	kvStoreProtoParser                KVStoreProtoParser
	registerMiddleware                middleware.Register
	graphiteRenderRouter              GraphiteRenderRouter
	graphiteFindRouter                GraphiteFindRouter
	defaultLookback                   time.Duration
}

// EmptyHandlerOptions returns  default handler options.
func EmptyHandlerOptions() HandlerOptions {
	return &handlerOptions{
		instrumentOpts: instrument.NewOptions(),
		nowFn:          time.Now,
		m3dbOpts:       m3.NewOptions(encoding.NewOptions()),
	}
}

// NewHandlerOptions builds a handler options.
func NewHandlerOptions(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	engine executor.Engine,
	prometheusEngineFn PromQLEngineFn,
	m3dbClusters m3.Clusters,
	clusterClient clusterclient.Client,
	cfg config.Configuration,
	embeddedDBCfg *dbconfig.DBConfiguration,
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder,
	graphiteFindFetchOptionsBuilder handleroptions.FetchOptionsBuilder,
	graphiteRenderFetchOptionsBuilder handleroptions.FetchOptionsBuilder,
	queryContextOptions models.QueryContextOptions,
	instrumentOpts instrument.Options,
	cpuProfileDuration time.Duration,
	placementServiceNames []string,
	serviceOptionDefaults []placementhandleroptions.ServiceOptionsDefault,
	queryRouter QueryRouter,
	instantQueryRouter QueryRouter,
	graphiteStorageOpts graphite.M3WrappedStorageOptions,
	m3dbOpts m3.Options,
	graphiteRenderRouter GraphiteRenderRouter,
	graphiteFindRouter GraphiteFindRouter,
	defaultLookback time.Duration,
) (HandlerOptions, error) {
	storeMetricsType := false
	if cfg.StoreMetricsType != nil {
		storeMetricsType = *cfg.StoreMetricsType
	}
	return &handlerOptions{
		storage:                           downsamplerAndWriter.Storage(),
		downsamplerAndWriter:              downsamplerAndWriter,
		engine:                            engine,
		prometheusEngineFn:                prometheusEngineFn,
		defaultEngine:                     getDefaultQueryEngine(cfg.Query.DefaultEngine),
		clusters:                          m3dbClusters,
		clusterClient:                     clusterClient,
		config:                            cfg,
		embeddedDBCfg:                     embeddedDBCfg,
		createdAt:                         time.Now(),
		tagOptions:                        tagOptions,
		fetchOptionsBuilder:               fetchOptionsBuilder,
		graphiteFindFetchOptionsBuilder:   graphiteFindFetchOptionsBuilder,
		graphiteRenderFetchOptionsBuilder: graphiteRenderFetchOptionsBuilder,
		queryContextOptions:               queryContextOptions,
		instrumentOpts:                    instrumentOpts,
		cpuProfileDuration:                cpuProfileDuration,
		placementServiceNames:             placementServiceNames,
		serviceOptionDefaults:             serviceOptionDefaults,
		nowFn:                             time.Now,
		queryRouter:                       queryRouter,
		instantQueryRouter:                instantQueryRouter,
		graphiteStorageOpts:               graphiteStorageOpts,
		m3dbOpts:                          m3dbOpts,
		storeMetricsType:                  storeMetricsType,
		namespaceValidator:                validators.NamespaceValidator,
		registerMiddleware:                middleware.Default,
		graphiteRenderRouter:              graphiteRenderRouter,
		graphiteFindRouter:                graphiteFindRouter,
		defaultLookback:                   defaultLookback,
	}, nil
}

func (o *handlerOptions) CreatedAt() time.Time {
	return o.createdAt
}

func (o *handlerOptions) Storage() storage.Storage {
	return o.storage
}

func (o *handlerOptions) SetStorage(s storage.Storage) HandlerOptions {
	opts := *o
	opts.storage = s
	return &opts
}

func (o *handlerOptions) DownsamplerAndWriter() ingest.DownsamplerAndWriter {
	return o.downsamplerAndWriter
}

func (o *handlerOptions) SetDownsamplerAndWriter(
	d ingest.DownsamplerAndWriter) HandlerOptions {
	opts := *o
	opts.downsamplerAndWriter = d
	return &opts
}

func (o *handlerOptions) Engine() executor.Engine {
	return o.engine
}

func (o *handlerOptions) SetEngine(e executor.Engine) HandlerOptions {
	opts := *o
	opts.engine = e
	return &opts
}

func (o *handlerOptions) PrometheusEngineFn() PromQLEngineFn {
	return o.prometheusEngineFn
}

func (o *handlerOptions) SetPrometheusEngineFn(fn PromQLEngineFn) HandlerOptions {
	opts := *o
	opts.prometheusEngineFn = fn
	return &opts
}

func (o *handlerOptions) Clusters() m3.Clusters {
	return o.clusters
}

func (o *handlerOptions) SetClusters(c m3.Clusters) HandlerOptions {
	opts := *o
	opts.clusters = c
	return &opts
}

func (o *handlerOptions) ClusterClient() clusterclient.Client {
	return o.clusterClient
}

func (o *handlerOptions) SetClusterClient(
	c clusterclient.Client) HandlerOptions {
	opts := *o
	opts.clusterClient = c
	return &opts
}

func (o *handlerOptions) Config() config.Configuration {
	return o.config
}

func (o *handlerOptions) SetConfig(c config.Configuration) HandlerOptions {
	opts := *o
	opts.config = c
	return &opts
}

func (o *handlerOptions) EmbeddedDBCfg() *dbconfig.DBConfiguration {
	return o.embeddedDBCfg
}

func (o *handlerOptions) SetEmbeddedDBCfg(
	c *dbconfig.DBConfiguration) HandlerOptions {
	opts := *o
	opts.embeddedDBCfg = c
	return &opts
}

func (o *handlerOptions) TagOptions() models.TagOptions {
	return o.tagOptions
}

func (o *handlerOptions) SetTagOptions(tags models.TagOptions) HandlerOptions {
	opts := *o
	opts.tagOptions = tags
	return &opts
}

func (o *handlerOptions) FetchOptionsBuilder() handleroptions.FetchOptionsBuilder {
	return o.fetchOptionsBuilder
}

func (o *handlerOptions) SetFetchOptionsBuilder(
	b handleroptions.FetchOptionsBuilder) HandlerOptions {
	opts := *o
	opts.fetchOptionsBuilder = b
	return &opts
}

func (o *handlerOptions) QueryContextOptions() models.QueryContextOptions {
	return o.queryContextOptions
}

func (o *handlerOptions) SetQueryContextOptions(
	q models.QueryContextOptions) HandlerOptions {
	opts := *o
	opts.queryContextOptions = q
	return &opts
}

func (o *handlerOptions) CPUProfileDuration() time.Duration {
	return o.cpuProfileDuration
}

func (o *handlerOptions) SetCPUProfileDuration(
	c time.Duration) HandlerOptions {
	opts := *o
	opts.cpuProfileDuration = c
	return &opts
}

func (o *handlerOptions) PlacementServiceNames() []string {
	return o.placementServiceNames
}

func (o *handlerOptions) SetPlacementServiceNames(
	n []string) HandlerOptions {
	opts := *o
	opts.placementServiceNames = n
	return &opts
}

func (o *handlerOptions) ServiceOptionDefaults() []placementhandleroptions.ServiceOptionsDefault {
	return o.serviceOptionDefaults
}

func (o *handlerOptions) SetServiceOptionDefaults(
	s []placementhandleroptions.ServiceOptionsDefault) HandlerOptions {
	opts := *o
	opts.serviceOptionDefaults = s
	return &opts
}

func (o *handlerOptions) InstrumentOpts() instrument.Options {
	return o.instrumentOpts
}

func (o *handlerOptions) SetInstrumentOpts(opts instrument.Options) HandlerOptions {
	options := *o
	options.instrumentOpts = opts
	return &options
}

func (o *handlerOptions) NowFn() clock.NowFn {
	return o.nowFn
}

func (o *handlerOptions) SetNowFn(n clock.NowFn) HandlerOptions {
	options := *o
	options.nowFn = n
	return &options
}

func (o *handlerOptions) DefaultQueryEngine() QueryEngine {
	return o.defaultEngine
}

func (o *handlerOptions) SetDefaultQueryEngine(value QueryEngine) HandlerOptions {
	options := *o
	options.defaultEngine = value
	return &options
}

func getDefaultQueryEngine(cfgEngine string) QueryEngine {
	engine := PrometheusEngine
	if strings.ToLower(cfgEngine) == string(M3QueryEngine) {
		engine = M3QueryEngine
	}
	return engine
}

// IsQueryEngineSet returns true if value contains query engine value. Otherwise returns false.
func IsQueryEngineSet(v string) bool {
	if strings.ToLower(v) == string(PrometheusEngine) ||
		strings.ToLower(v) == string(M3QueryEngine) {
		return true
	}
	return false
}

func (o *handlerOptions) QueryRouter() QueryRouter {
	return o.queryRouter
}

func (o *handlerOptions) SetQueryRouter(value QueryRouter) HandlerOptions {
	opts := *o
	opts.queryRouter = value
	return &opts
}

func (o *handlerOptions) InstantQueryRouter() QueryRouter {
	return o.instantQueryRouter
}

func (o *handlerOptions) SetInstantQueryRouter(value QueryRouter) HandlerOptions {
	opts := *o
	opts.instantQueryRouter = value
	return &opts
}

func (o *handlerOptions) GraphiteStorageOptions() graphite.M3WrappedStorageOptions {
	return o.graphiteStorageOpts
}

func (o *handlerOptions) SetGraphiteStorageOptions(value graphite.M3WrappedStorageOptions) HandlerOptions {
	opts := *o
	opts.graphiteStorageOpts = value
	return &opts
}

func (o *handlerOptions) GraphiteFindFetchOptionsBuilder() handleroptions.FetchOptionsBuilder {
	return o.graphiteFindFetchOptionsBuilder
}

func (o *handlerOptions) SetGraphiteFindFetchOptionsBuilder(value handleroptions.FetchOptionsBuilder) HandlerOptions {
	opts := *o
	opts.graphiteFindFetchOptionsBuilder = value
	return &opts
}

func (o *handlerOptions) GraphiteRenderFetchOptionsBuilder() handleroptions.FetchOptionsBuilder {
	return o.graphiteRenderFetchOptionsBuilder
}

func (o *handlerOptions) SetGraphiteRenderFetchOptionsBuilder(value handleroptions.FetchOptionsBuilder) HandlerOptions {
	opts := *o
	opts.graphiteRenderFetchOptionsBuilder = value
	return &opts
}

func (o *handlerOptions) GraphiteRenderRouter() GraphiteRenderRouter {
	return o.graphiteRenderRouter
}

func (o *handlerOptions) SetGraphiteRenderRouter(value GraphiteRenderRouter) HandlerOptions {
	opts := *o
	opts.graphiteRenderRouter = value
	return &opts
}

func (o *handlerOptions) GraphiteFindRouter() GraphiteFindRouter {
	return o.graphiteFindRouter
}

func (o *handlerOptions) SetGraphiteFindRouter(value GraphiteFindRouter) HandlerOptions {
	opts := *o
	opts.graphiteFindRouter = value
	return &opts
}

func (o *handlerOptions) SetM3DBOptions(value m3.Options) HandlerOptions {
	opts := *o
	opts.m3dbOpts = value
	return &opts
}

func (o *handlerOptions) M3DBOptions() m3.Options {
	return o.m3dbOpts
}

func (o *handlerOptions) SetStoreMetricsType(value bool) HandlerOptions {
	opts := *o
	opts.storeMetricsType = value
	return &opts
}

func (o *handlerOptions) StoreMetricsType() bool {
	return o.storeMetricsType
}

func (o *handlerOptions) SetNamespaceValidator(value NamespaceValidator) HandlerOptions {
	opts := *o
	opts.namespaceValidator = value
	return &opts
}

func (o *handlerOptions) NamespaceValidator() NamespaceValidator {
	return o.namespaceValidator
}

// NamespaceValidator defines namespace validation logics.
type NamespaceValidator interface {
	// ValidateNewNamespace gets invoked when creating a new namespace.
	ValidateNewNamespace(newNs dbnamespace.Metadata, existing []dbnamespace.Metadata) error
}

func (o *handlerOptions) SetKVStoreProtoParser(value KVStoreProtoParser) HandlerOptions {
	opts := *o
	opts.kvStoreProtoParser = value
	return &opts
}

func (o *handlerOptions) KVStoreProtoParser() KVStoreProtoParser {
	return o.kvStoreProtoParser
}

func (o *handlerOptions) RegisterMiddleware() middleware.Register {
	return o.registerMiddleware
}

func (o *handlerOptions) SetRegisterMiddleware(value middleware.Register) HandlerOptions {
	opts := *o
	opts.registerMiddleware = value
	return &opts
}

func (o *handlerOptions) DefaultLookback() time.Duration {
	return o.defaultLookback
}

func (o *handlerOptions) SetDefaultLookback(value time.Duration) HandlerOptions {
	opts := *o
	opts.defaultLookback = value
	return &opts
}

// KVStoreProtoParser parses protobuf messages based off specific keys.
type KVStoreProtoParser func(key string) (protoiface.MessageV1, error)
