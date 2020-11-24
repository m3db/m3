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

package options

import (
	"io"
	"net/http"
	"strings"
	"time"

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cmd/services/m3coordinator/ingest"
	dbconfig "github.com/m3db/m3/src/cmd/services/m3dbnode/config"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	dbnamespace "github.com/m3db/m3/src/dbnode/namespace"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/api/v1/validators"
	"github.com/m3db/m3/src/query/executor"
	graphite "github.com/m3db/m3/src/query/graphite/storage"
	"github.com/m3db/m3/src/query/models"
	"github.com/m3db/m3/src/query/storage"
	"github.com/m3db/m3/src/query/storage/m3"
	"github.com/m3db/m3/src/query/ts"
	"github.com/m3db/m3/src/query/ts/m3db"
	"github.com/m3db/m3/src/x/clock"
	"github.com/m3db/m3/src/x/instrument"

	"github.com/prometheus/prometheus/promql"
)

// QueryEngine is a type of query engine.
type QueryEngine string

const (
	// PrometheusEngine is the prometheus query engine type.
	PrometheusEngine QueryEngine = "prometheus"
	// M3QueryEngine is M3 query engine type.
	M3QueryEngine QueryEngine = "m3query"
)

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

	// PrometheusEngine returns the prometheus engine.
	PrometheusEngine() *promql.Engine
	// SetPrometheusEngine sets the prometheus engine.
	SetPrometheusEngine(e *promql.Engine) HandlerOptions

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

	// EmbeddedDbCfg returns the embedded db config.
	EmbeddedDbCfg() *dbconfig.DBConfiguration
	// SetEmbeddedDbCfg sets the embedded db config.
	SetEmbeddedDbCfg(c *dbconfig.DBConfiguration) HandlerOptions

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
	ServiceOptionDefaults() []handleroptions.ServiceOptionsDefault
	// SetServiceOptionDefaults sets the service option defaults.
	SetServiceOptionDefaults(s []handleroptions.ServiceOptionsDefault) HandlerOptions

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

	// SetM3DBOptions sets the M3DB options.
	SetM3DBOptions(value m3db.Options) HandlerOptions
	// M3DBOptions returns the M3DB options.
	M3DBOptions() m3db.Options

	// SetStoreMetricsType enables/disables storing of metrics type.
	SetStoreMetricsType(value bool) HandlerOptions
	// StoreMetricsType returns true if storing of metrics type is enabled.
	StoreMetricsType() bool

	// SetNamespaceValidator sets the NamespaceValidator.
	SetNamespaceValidator(NamespaceValidator) HandlerOptions
	// NamespaceValidator returns the NamespaceValidator.
	NamespaceValidator() NamespaceValidator
}

// HandlerOptions represents handler options.
type handlerOptions struct {
	storage               storage.Storage
	downsamplerAndWriter  ingest.DownsamplerAndWriter
	engine                executor.Engine
	prometheusEngine      *promql.Engine
	defaultEngine         QueryEngine
	clusters              m3.Clusters
	clusterClient         clusterclient.Client
	config                config.Configuration
	embeddedDbCfg         *dbconfig.DBConfiguration
	createdAt             time.Time
	tagOptions            models.TagOptions
	fetchOptionsBuilder   handleroptions.FetchOptionsBuilder
	queryContextOptions   models.QueryContextOptions
	instrumentOpts        instrument.Options
	cpuProfileDuration    time.Duration
	placementServiceNames []string
	serviceOptionDefaults []handleroptions.ServiceOptionsDefault
	nowFn                 clock.NowFn
	queryRouter           QueryRouter
	instantQueryRouter    QueryRouter
	graphiteStorageOpts   graphite.M3WrappedStorageOptions
	m3dbOpts              m3db.Options
	namespaceValidator    NamespaceValidator
	storeMetricsType      bool
}

// EmptyHandlerOptions returns  default handler options.
func EmptyHandlerOptions() HandlerOptions {
	return &handlerOptions{
		instrumentOpts: instrument.NewOptions(),
		nowFn:          time.Now,
		m3dbOpts:       m3db.NewOptions(),
	}
}

// NewHandlerOptions builds a handler options.
func NewHandlerOptions(
	downsamplerAndWriter ingest.DownsamplerAndWriter,
	tagOptions models.TagOptions,
	engine executor.Engine,
	prometheusEngine *promql.Engine,
	m3dbClusters m3.Clusters,
	clusterClient clusterclient.Client,
	cfg config.Configuration,
	embeddedDbCfg *dbconfig.DBConfiguration,
	fetchOptionsBuilder handleroptions.FetchOptionsBuilder,
	queryContextOptions models.QueryContextOptions,
	instrumentOpts instrument.Options,
	cpuProfileDuration time.Duration,
	placementServiceNames []string,
	serviceOptionDefaults []handleroptions.ServiceOptionsDefault,
	queryRouter QueryRouter,
	instantQueryRouter QueryRouter,
	graphiteStorageOpts graphite.M3WrappedStorageOptions,
	m3dbOpts m3db.Options,
) (HandlerOptions, error) {
	storeMetricsType := false
	if cfg.StoreMetricsType != nil {
		storeMetricsType = *cfg.StoreMetricsType
	}

	return &handlerOptions{
		storage:               downsamplerAndWriter.Storage(),
		downsamplerAndWriter:  downsamplerAndWriter,
		engine:                engine,
		prometheusEngine:      prometheusEngine,
		defaultEngine:         getDefaultQueryEngine(cfg.Query.DefaultEngine),
		clusters:              m3dbClusters,
		clusterClient:         clusterClient,
		config:                cfg,
		embeddedDbCfg:         embeddedDbCfg,
		createdAt:             time.Now(),
		tagOptions:            tagOptions,
		fetchOptionsBuilder:   fetchOptionsBuilder,
		queryContextOptions:   queryContextOptions,
		instrumentOpts:        instrumentOpts,
		cpuProfileDuration:    cpuProfileDuration,
		placementServiceNames: placementServiceNames,
		serviceOptionDefaults: serviceOptionDefaults,
		nowFn:                 time.Now,
		queryRouter:           queryRouter,
		instantQueryRouter:    instantQueryRouter,
		graphiteStorageOpts:   graphiteStorageOpts,
		m3dbOpts:              m3dbOpts,
		storeMetricsType:      storeMetricsType,
		namespaceValidator:    validators.NamespaceValidator,
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

func (o *handlerOptions) PrometheusEngine() *promql.Engine {
	return o.prometheusEngine
}

func (o *handlerOptions) SetPrometheusEngine(e *promql.Engine) HandlerOptions {
	opts := *o
	opts.prometheusEngine = e
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

func (o *handlerOptions) EmbeddedDbCfg() *dbconfig.DBConfiguration {
	return o.embeddedDbCfg
}

func (o *handlerOptions) SetEmbeddedDbCfg(
	c *dbconfig.DBConfiguration) HandlerOptions {
	opts := *o
	opts.embeddedDbCfg = c
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

func (o *handlerOptions) ServiceOptionDefaults() []handleroptions.ServiceOptionsDefault {
	return o.serviceOptionDefaults
}

func (o *handlerOptions) SetServiceOptionDefaults(
	s []handleroptions.ServiceOptionsDefault) HandlerOptions {
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

func (o *handlerOptions) SetM3DBOptions(value m3db.Options) HandlerOptions {
	opts := *o
	opts.m3dbOpts = value
	return &opts
}

func (o *handlerOptions) M3DBOptions() m3db.Options {
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
