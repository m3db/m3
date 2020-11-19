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

package config

import (
	"bytes"
	"errors"
	"fmt"
	"net/url"
	"path"
	"strings"
	"time"

	coordinatorcfg "github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/dbnode/client"
	"github.com/m3db/m3/src/dbnode/discovery"
	"github.com/m3db/m3/src/dbnode/environment"
	"github.com/m3db/m3/src/dbnode/storage/series"
	"github.com/m3db/m3/src/x/config/hostid"
	"github.com/m3db/m3/src/x/debug/config"
	"github.com/m3db/m3/src/x/instrument"
	xlog "github.com/m3db/m3/src/x/log"
	"github.com/m3db/m3/src/x/opentracing"

	"github.com/m3dbx/vellum/regexp"
	"go.etcd.io/etcd/embed"
	"go.etcd.io/etcd/pkg/transport"
	"go.etcd.io/etcd/pkg/types"
)

const (
	defaultEtcdDirSuffix  = "etcd"
	defaultEtcdListenHost = "http://0.0.0.0"
	defaultEtcdClientPort = 2379
	defaultEtcdServerPort = 2380
)

var (
	defaultLogging = xlog.Configuration{
		Level: "info",
	}
	defaultMetricsSanitization        = instrument.PrometheusMetricSanitization
	defaultMetricsExtendedMetricsType = instrument.DetailedExtendedMetrics
	defaultMetrics                    = instrument.MetricsConfiguration{
		PrometheusReporter: &instrument.PrometheusConfiguration{
			HandlerPath: "/metrics",
		},
		Sanitization:    &defaultMetricsSanitization,
		SamplingRate:    1.0,
		ExtendedMetrics: &defaultMetricsExtendedMetricsType,
	}
	defaultListenAddress            = "0.0.0.0:9000"
	defaultClusterListenAddress     = "0.0.0.0:9001"
	defaultHTTPNodeListenAddress    = "0.0.0.0:9002"
	defaultHTTPClusterListenAddress = "0.0.0.0:9003"
	defaultDebugListenAddress       = "0.0.0.0:9004"
	defaultHostIDValue              = "m3db_local"
	defaultHostID                   = hostid.Configuration{
		Resolver: hostid.ConfigResolver,
		Value:    &defaultHostIDValue,
	}
	defaultGCPercentage                  = 100
	defaultWriteNewSeriesAsync           = true
	defaultWriteNewSeriesBackoffDuration = 2 * time.Millisecond
	defaultCommitLogPolicy               = CommitLogPolicy{
		FlushMaxBytes: 524288,
		FlushEvery:    time.Second * 1,
		Queue: CommitLogQueuePolicy{
			Size:            2097152,
			CalculationType: CalculationTypeFixed,
		},
	}
	defaultDiscoveryType = discovery.M3DBSingleNodeType
	defaultDiscovery     = discovery.Configuration{
		Type: &defaultDiscoveryType,
	}
)

// Configuration is the top level configuration that includes both a DB
// node and a coordinator.
type Configuration struct {
	// DB is the configuration for a DB node (required).
	DB *DBConfiguration `yaml:"db"`

	// Coordinator is the configuration for the coordinator to run (optional).
	Coordinator *coordinatorcfg.Configuration `yaml:"coordinator"`
}

// Validate validates the Configuration. We use this method to validate fields
// where the validator package falls short.
func (c *Configuration) Validate() error {
	return c.DB.Validate()
}

// DBConfiguration is the configuration for a DB node.
type DBConfiguration struct {
	// Index configuration.
	Index IndexConfiguration `yaml:"index"`

	// Transforms configuration.
	Transforms TransformConfiguration `yaml:"transforms"`

	// Logging configuration.
	Logging *xlog.Configuration `yaml:"logging"`

	// Metrics configuration.
	Metrics *instrument.MetricsConfiguration `yaml:"metrics"`

	// The host and port on which to listen for the node service.
	ListenAddress *string `yaml:"listenAddress"`

	// The host and port on which to listen for the cluster service.
	ClusterListenAddress *string `yaml:"clusterListenAddress"`

	// The HTTP host and port on which to listen for the node service.
	HTTPNodeListenAddress *string `yaml:"httpNodeListenAddress"`

	// The HTTP host and port on which to listen for the cluster service.
	HTTPClusterListenAddress *string `yaml:"httpClusterListenAddress"`

	// The host and port on which to listen for debug endpoints.
	DebugListenAddress *string `yaml:"debugListenAddress"`

	// HostID is the local host ID configuration.
	HostID *hostid.Configuration `yaml:"hostID"`

	// Client configuration, used for inter-node communication and when used as a coordinator.
	Client client.Configuration `yaml:"client"`

	// The initial garbage collection target percentage.
	GCPercentage int `yaml:"gcPercentage" validate:"max=100"`

	// The tick configuration, omit this to use default settings.
	Tick *TickConfiguration `yaml:"tick"`

	// Bootstrap configuration.
	Bootstrap BootstrapConfiguration `yaml:"bootstrap"`

	// The block retriever policy.
	BlockRetrieve *BlockRetrievePolicy `yaml:"blockRetrieve"`

	// Cache configurations.
	Cache CacheConfigurations `yaml:"cache"`

	// The filesystem configuration for the node.
	Filesystem FilesystemConfiguration `yaml:"filesystem"`

	// The commit log policy for the node.
	CommitLog *CommitLogPolicy `yaml:"commitlog"`

	// The repair policy for repairing data within a cluster.
	Repair *RepairPolicy `yaml:"repair"`

	// The replication policy for replicating data between clusters.
	Replication *ReplicationPolicy `yaml:"replication"`

	// The pooling policy.
	PoolingPolicy *PoolingPolicy `yaml:"pooling"`

	// The discovery configuration.
	Discovery *discovery.Configuration `yaml:"discovery"`

	// The configuration for hashing
	Hashing HashingConfiguration `yaml:"hashing"`

	// Write new series asynchronously for fast ingestion of new ID bursts.
	WriteNewSeriesAsync *bool `yaml:"writeNewSeriesAsync"`

	// Write new series backoff between batches of new series insertions.
	WriteNewSeriesBackoffDuration *time.Duration `yaml:"writeNewSeriesBackoffDuration"`

	// Proto contains the configuration specific to running in the ProtoDataMode.
	Proto *ProtoConfiguration `yaml:"proto"`

	// Tracing configures opentracing. If not provided, tracing is disabled.
	Tracing *opentracing.TracingConfiguration `yaml:"tracing"`

	// Limits contains configuration for limits that can be applied to M3DB for the purposes
	// of applying back-pressure or protecting the db nodes.
	Limits LimitsConfiguration `yaml:"limits"`

	// WideConfig contains some limits for wide operations. These operations
	// differ from regular paths by optimizing for query completeness across
	// arbitary query ranges rather than speed.
	WideConfig *WideConfiguration `yaml:"wide"`

	// TChannel exposes TChannel config options.
	TChannel *TChannelConfiguration `yaml:"tchannel"`

	// Debug configuration.
	Debug config.DebugConfiguration `yaml:"debug"`
}

// LoggingOrDefault returns the logging configuration or defaults.
func (c *DBConfiguration) LoggingOrDefault() xlog.Configuration {
	if c.Logging == nil {
		return defaultLogging
	}

	return *c.Logging
}

// MetricsOrDefault returns metrics configuration or defaults.
func (c *DBConfiguration) MetricsOrDefault() *instrument.MetricsConfiguration {
	if c.Metrics == nil {
		return &defaultMetrics
	}

	return c.Metrics
}

// ListenAddressOrDefault returns the listen address or default.
func (c *DBConfiguration) ListenAddressOrDefault() string {
	if c.ListenAddress == nil {
		return defaultListenAddress
	}

	return *c.ListenAddress
}

// ClusterListenAddressOrDefault returns the listen address or default.
func (c *DBConfiguration) ClusterListenAddressOrDefault() string {
	if c.ClusterListenAddress == nil {
		return defaultClusterListenAddress
	}

	return *c.ClusterListenAddress
}

// HTTPNodeListenAddressOrDefault returns the listen address or default.
func (c *DBConfiguration) HTTPNodeListenAddressOrDefault() string {
	if c.HTTPNodeListenAddress == nil {
		return defaultHTTPNodeListenAddress
	}

	return *c.HTTPNodeListenAddress
}

// HTTPClusterListenAddressOrDefault returns the listen address or default.
func (c *DBConfiguration) HTTPClusterListenAddressOrDefault() string {
	if c.HTTPClusterListenAddress == nil {
		return defaultHTTPClusterListenAddress
	}

	return *c.HTTPClusterListenAddress
}

// DebugListenAddressOrDefault returns the listen address or default.
func (c *DBConfiguration) DebugListenAddressOrDefault() string {
	if c.DebugListenAddress == nil {
		return defaultDebugListenAddress
	}

	return *c.DebugListenAddress
}

// HostIDOrDefault returns the host ID or default.
func (c *DBConfiguration) HostIDOrDefault() hostid.Configuration {
	if c.HostID == nil {
		return defaultHostID
	}

	return *c.HostID
}

// CommitLogOrDefault returns the commit log policy or default.
func (c *DBConfiguration) CommitLogOrDefault() CommitLogPolicy {
	if c.CommitLog == nil {
		return defaultCommitLogPolicy
	}

	return *c.CommitLog
}

// GCPercentageOrDefault returns the GC percentage or default.
func (c *DBConfiguration) GCPercentageOrDefault() int {
	if c.GCPercentage == 0 {
		return defaultGCPercentage
	}

	return c.GCPercentage
}

// WriteNewSeriesAsyncOrDefault returns whether to write new series async or not.
func (c *DBConfiguration) WriteNewSeriesAsyncOrDefault() bool {
	if c.WriteNewSeriesAsync == nil {
		return defaultWriteNewSeriesAsync
	}

	return *c.WriteNewSeriesAsync
}

// WriteNewSeriesBackoffDurationOrDefault returns the backoff duration for new series inserts.
func (c *DBConfiguration) WriteNewSeriesBackoffDurationOrDefault() time.Duration {
	if c.WriteNewSeriesBackoffDuration == nil {
		return defaultWriteNewSeriesBackoffDuration
	}

	return *c.WriteNewSeriesBackoffDuration
}

// PoolingPolicyOrDefault returns the pooling policy or default.
func (c *DBConfiguration) PoolingPolicyOrDefault() (PoolingPolicy, error) {
	var policy PoolingPolicy
	if c.PoolingPolicy != nil {
		policy = *c.PoolingPolicy
	}

	if err := policy.InitDefaultsAndValidate(); err != nil {
		return PoolingPolicy{}, err
	}

	return policy, nil
}

// DiscoveryOrDefault returns the discovery configuration or defaults.
func (c *DBConfiguration) DiscoveryOrDefault() discovery.Configuration {
	if c.Discovery == nil {
		return defaultDiscovery
	}

	return *c.Discovery
}

// Validate validates the Configuration. We use this method to validate fields
// where the validator package falls short.
func (c *DBConfiguration) Validate() error {
	if err := c.Filesystem.Validate(); err != nil {
		return err
	}

	if _, err := c.PoolingPolicyOrDefault(); err != nil {
		return err
	}

	if err := c.Client.Validate(); err != nil {
		return err
	}

	if err := c.Proto.Validate(); err != nil {
		return err
	}

	if err := c.Transforms.Validate(); err != nil {
		return err
	}

	if c.Replication != nil {
		if err := c.Replication.Validate(); err != nil {
			return err
		}
	}

	return nil
}

// IndexConfiguration contains index-specific configuration.
type IndexConfiguration struct {
	// MaxQueryIDsConcurrency controls the maximum number of outstanding QueryID
	// requests that can be serviced concurrently. Limiting the concurrency is
	// important to prevent index queries from overloading the database entirely
	// as they are very CPU-intensive (regex and FST matching).
	MaxQueryIDsConcurrency int `yaml:"maxQueryIDsConcurrency" validate:"min=0"`

	// RegexpDFALimit is the limit on the max number of states used by a
	// regexp deterministic finite automaton. Default is 10,000 states.
	RegexpDFALimit *int `yaml:"regexpDFALimit"`

	// RegexpFSALimit is the limit on the max number of bytes used by the
	// finite state automaton. Default is 10mb (10 million as int).
	RegexpFSALimit *uint `yaml:"regexpFSALimit"`

	// ForwardIndexProbability determines the likelihood that an incoming write is
	// written to the next block, when arriving close to the block boundary.
	//
	// NB: this is an optimization which lessens pressure on the index around
	// block boundaries by eagerly writing the series to the next block
	// preemptively.
	ForwardIndexProbability float64 `yaml:"forwardIndexProbability" validate:"min=0.0,max=1.0"`

	// ForwardIndexThreshold determines the threshold for forward writes, as a
	// fraction of the given namespace's bufferFuture.
	//
	// NB: this is an optimization which lessens pressure on the index around
	// block boundaries by eagerly writing the series to the next block
	// preemptively.
	ForwardIndexThreshold float64 `yaml:"forwardIndexThreshold" validate:"min=0.0,max=1.0"`
}

// RegexpDFALimitOrDefault returns the deterministic finite automaton states
// limit or default.
func (c IndexConfiguration) RegexpDFALimitOrDefault() int {
	if c.RegexpDFALimit == nil {
		return regexp.StateLimit()
	}
	return *c.RegexpDFALimit
}

// RegexpFSALimitOrDefault returns the finite state automaton size
// limit or default.
func (c IndexConfiguration) RegexpFSALimitOrDefault() uint {
	if c.RegexpFSALimit == nil {
		return regexp.DefaultLimit()
	}
	return *c.RegexpFSALimit
}

// TransformConfiguration contains configuration options that can transform
// incoming writes.
type TransformConfiguration struct {
	// TruncateBy determines what type of truncatation is applied to incoming
	// writes.
	TruncateBy series.TruncateType `yaml:"truncateBy"`
	// ForcedValue determines what to set all incoming write values to.
	ForcedValue *float64 `yaml:"forceValue"`
}

// Validate validates the transform configuration.
func (c *TransformConfiguration) Validate() error {
	if c == nil {
		return nil
	}

	return c.TruncateBy.Validate()
}

// TickConfiguration is the tick configuration for background processing of
// series as blocks are rotated from mutable to immutable and out of order
// writes are merged.
type TickConfiguration struct {
	// Tick series batch size is the batch size to process series together
	// during a tick before yielding and sleeping the per series duration
	// multiplied by the batch size.
	// The higher this value is the more variable CPU utilization will be
	// but the shorter ticks will ultimately be.
	SeriesBatchSize int `yaml:"seriesBatchSize"`

	// Tick per series sleep at the completion of a tick batch.
	PerSeriesSleepDuration time.Duration `yaml:"perSeriesSleepDuration"`

	// Tick minimum interval controls the minimum tick interval for the node.
	MinimumInterval time.Duration `yaml:"minimumInterval"`
}

// BlockRetrievePolicy is the block retrieve policy.
type BlockRetrievePolicy struct {
	// FetchConcurrency is the concurrency to fetch blocks from disk. For
	// spinning disks it is highly recommended to set this value to 1.
	FetchConcurrency *int `yaml:"fetchConcurrency" validate:"min=1"`

	// CacheBlocksOnRetrieve globally enables/disables callbacks used to cache blocks fetched
	// from disk.
	CacheBlocksOnRetrieve *bool `yaml:"cacheBlocksOnRetrieve"`
}

// CommitLogPolicy is the commit log policy.
type CommitLogPolicy struct {
	// The max size the commit log will flush a segment to disk after buffering.
	FlushMaxBytes int `yaml:"flushMaxBytes" validate:"nonzero"`

	// The maximum amount of time the commit log will wait to flush to disk.
	FlushEvery time.Duration `yaml:"flushEvery" validate:"nonzero"`

	// The queue the commit log will keep in front of the current commit log segment.
	// Modifying values in this policy will control how many pending writes can be
	// in the commitlog queue before M3DB will begin rejecting writes.
	Queue CommitLogQueuePolicy `yaml:"queue" validate:"nonzero"`

	// The actual Golang channel that implements the commit log queue. We separate this
	// from the Queue field for historical / legacy reasons. Generally speaking, the
	// values in this config should not need to be modified, but we leave it in for
	// tuning purposes. Unlike the Queue field, values in this policy control the size
	// of the channel that backs the queue. Since writes to the commitlog are batched,
	// setting the size of this policy will control how many batches can be queued, and
	// indrectly how many writes can be queued, but that is dependent on the batch size
	// of the client. As a result, we recommend that users avoid tuning this field and
	// modify the Queue size instead which maps directly to the number of writes. This
	// works in most cases because the default size of the QueueChannel should be large
	// enough for almost all workloads assuming a reasonable batch size is used.
	QueueChannel *CommitLogQueuePolicy `yaml:"queueChannel"`
}

// CalculationType is a type of configuration parameter.
type CalculationType string

const (
	// CalculationTypeFixed is a fixed parameter not to be scaled of any parameter.
	CalculationTypeFixed CalculationType = "fixed"
	// CalculationTypePerCPU is a parameter that needs to be scaled by number of CPUs.
	CalculationTypePerCPU CalculationType = "percpu"
)

// CommitLogQueuePolicy is the commit log queue policy.
type CommitLogQueuePolicy struct {
	// The type of calculation for the size.
	CalculationType CalculationType `yaml:"calculationType"`

	// The size of the commit log, calculated according to the calculation type.
	Size int `yaml:"size" validate:"nonzero"`
}

// RepairPolicy is the repair policy.
type RepairPolicy struct {
	// Enabled or disabled.
	Enabled bool `yaml:"enabled"`

	// The repair throttle.
	Throttle time.Duration `yaml:"throttle"`

	// The repair check interval.
	CheckInterval time.Duration `yaml:"checkInterval"`

	// Whether debug shadow comparisons are enabled.
	DebugShadowComparisonsEnabled bool `yaml:"debugShadowComparisonsEnabled"`

	// If enabled, what percentage of metadata should perform a detailed debug
	// shadow comparison.
	DebugShadowComparisonsPercentage float64 `yaml:"debugShadowComparisonsPercentage"`
}

// ReplicationPolicy is the replication policy.
type ReplicationPolicy struct {
	Clusters []ReplicatedCluster `yaml:"clusters"`
}

// Validate validates the replication policy.
func (r *ReplicationPolicy) Validate() error {
	names := map[string]bool{}
	for _, c := range r.Clusters {
		if err := c.Validate(); err != nil {
			return err
		}

		if _, ok := names[c.Name]; ok {
			return fmt.Errorf(
				"replicated cluster names must be unique, but %s was repeated",
				c.Name)
		}
		names[c.Name] = true
	}

	return nil
}

// ReplicatedCluster defines a cluster to replicate data from.
type ReplicatedCluster struct {
	Name          string                `yaml:"name"`
	RepairEnabled bool                  `yaml:"repairEnabled"`
	Client        *client.Configuration `yaml:"client"`
}

// Validate validates the configuration for a replicated cluster.
func (r *ReplicatedCluster) Validate() error {
	if r.Name == "" {
		return errors.New("replicated cluster must be assigned a name")
	}

	if r.RepairEnabled && r.Client == nil {
		return fmt.Errorf(
			"replicated cluster: %s has repair enabled but not client configuration", r.Name)
	}

	return nil
}

// HashingConfiguration is the configuration for hashing.
type HashingConfiguration struct {
	// Murmur32 seed value.
	Seed uint32 `yaml:"seed"`
}

// ProtoConfiguration is the configuration for running with ProtoDataMode enabled.
type ProtoConfiguration struct {
	// Enabled specifies whether proto is enabled.
	Enabled        bool                            `yaml:"enabled"`
	SchemaRegistry map[string]NamespaceProtoSchema `yaml:"schema_registry"`
}

// WideConfiguration contains configuration for wide operations. These
// differ from regular paths by optimizing for query completeness across
// arbitary query ranges rather than speed.
type WideConfiguration struct {
	// BatchSize represents batch size for wide operations. This size corresponds
	// to how many series are processed within a single "chunk"; larger batch
	// sizes will complete the query faster, but increase memory consumption.
	BatchSize int `yaml:"batchSize"`
}

// NamespaceProtoSchema is the namespace protobuf schema.
type NamespaceProtoSchema struct {
	// For application m3db client integration test convenience (where a local dbnode is started as a docker container),
	// we allow loading user schema from local file into schema registry.
	SchemaFilePath string `yaml:"schemaFilePath"`
	MessageName    string `yaml:"messageName"`
}

// Validate validates the NamespaceProtoSchema.
func (c NamespaceProtoSchema) Validate() error {
	if c.SchemaFilePath == "" {
		return errors.New("schemaFilePath is required for Proto data mode")
	}

	if c.MessageName == "" {
		return errors.New("messageName is required for Proto data mode")
	}

	return nil
}

// Validate validates the ProtoConfiguration.
func (c *ProtoConfiguration) Validate() error {
	if c == nil || !c.Enabled {
		return nil
	}

	for _, schema := range c.SchemaRegistry {
		if err := schema.Validate(); err != nil {
			return err
		}
	}
	return nil
}

// NewEtcdEmbedConfig creates a new embedded etcd config from kv config.
func NewEtcdEmbedConfig(cfg DBConfiguration) (*embed.Config, error) {
	newKVCfg := embed.NewConfig()

	hostID, err := cfg.HostIDOrDefault().Resolve()
	if err != nil {
		return nil, fmt.Errorf("failed resolving hostID %w", err)
	}

	discoveryCfg := cfg.DiscoveryOrDefault()
	envCfg, err := discoveryCfg.EnvironmentConfig(hostID)
	if err != nil {
		return nil, fmt.Errorf("failed getting env config from discovery config %w", err)
	}

	kvCfg := envCfg.SeedNodes
	newKVCfg.Name = hostID

	dir := kvCfg.RootDir
	if dir == "" {
		dir = path.Join(cfg.Filesystem.FilePathPrefixOrDefault(), defaultEtcdDirSuffix)
	}
	newKVCfg.Dir = dir

	LPUrls, err := convertToURLsWithDefault(kvCfg.ListenPeerUrls, newURL(defaultEtcdListenHost, defaultEtcdServerPort))
	if err != nil {
		return nil, err
	}
	newKVCfg.LPUrls = LPUrls

	LCUrls, err := convertToURLsWithDefault(kvCfg.ListenClientUrls, newURL(defaultEtcdListenHost, defaultEtcdClientPort))
	if err != nil {
		return nil, err
	}
	newKVCfg.LCUrls = LCUrls

	host, endpoint, err := getHostAndEndpointFromID(kvCfg.InitialCluster, hostID)
	if err != nil {
		return nil, err
	}

	if host.ClusterState != "" {
		newKVCfg.ClusterState = host.ClusterState
	}

	APUrls, err := convertToURLsWithDefault(kvCfg.InitialAdvertisePeerUrls, newURL(endpoint, defaultEtcdServerPort))
	if err != nil {
		return nil, err
	}
	newKVCfg.APUrls = APUrls

	ACUrls, err := convertToURLsWithDefault(kvCfg.AdvertiseClientUrls, newURL(endpoint, defaultEtcdClientPort))
	if err != nil {
		return nil, err
	}
	newKVCfg.ACUrls = ACUrls

	newKVCfg.InitialCluster = initialClusterString(kvCfg.InitialCluster)

	copySecurityDetails := func(tls *transport.TLSInfo, ysc *environment.SeedNodeSecurityConfig) {
		tls.TrustedCAFile = ysc.CAFile
		tls.CertFile = ysc.CertFile
		tls.KeyFile = ysc.KeyFile
		tls.ClientCertAuth = ysc.CertAuth
		tls.TrustedCAFile = ysc.TrustedCAFile
	}
	copySecurityDetails(&newKVCfg.ClientTLSInfo, &kvCfg.ClientTransportSecurity)
	copySecurityDetails(&newKVCfg.PeerTLSInfo, &kvCfg.PeerTransportSecurity)
	newKVCfg.ClientAutoTLS = kvCfg.ClientTransportSecurity.AutoTLS
	newKVCfg.PeerAutoTLS = kvCfg.PeerTransportSecurity.AutoTLS

	return newKVCfg, nil
}

func newURL(host string, port int) string {
	return fmt.Sprintf("%s:%d", host, port)
}

func convertToURLsWithDefault(urlStrs []string, def ...string) ([]url.URL, error) {
	if len(urlStrs) == 0 {
		urlStrs = def
	}

	urls, err := types.NewURLs(urlStrs)
	if err != nil {
		return nil, err
	}

	return []url.URL(urls), nil
}

func initialClusterString(initialCluster []environment.SeedNode) string {
	var buffer bytes.Buffer

	for i, seedNode := range initialCluster {
		buffer.WriteString(seedNode.HostID)
		buffer.WriteString("=")
		buffer.WriteString(seedNode.Endpoint)

		if i < len(initialCluster)-1 {
			buffer.WriteString(",")
		}
	}

	return buffer.String()
}

func getHostAndEndpointFromID(initialCluster []environment.SeedNode, hostID string) (environment.SeedNode, string, error) {
	emptySeedNode := environment.SeedNode{}

	if len(initialCluster) == 0 {
		return emptySeedNode, "", errors.New("zero seed nodes in initialCluster")
	}

	for _, seedNode := range initialCluster {
		if hostID == seedNode.HostID {
			endpoint := seedNode.Endpoint

			colonIdx := strings.LastIndex(endpoint, ":")
			if colonIdx == -1 {
				return emptySeedNode, "", errors.New("invalid initialCluster format")
			}

			return seedNode, endpoint[:colonIdx], nil
		}
	}

	return emptySeedNode, "", errors.New("host not in initialCluster list")
}

// InitialClusterEndpoints returns the endpoints of the initial cluster
func InitialClusterEndpoints(initialCluster []environment.SeedNode) ([]string, error) {
	endpoints := make([]string, 0, len(initialCluster))

	for _, seedNode := range initialCluster {
		endpoint := seedNode.Endpoint

		colonIdx := strings.LastIndex(endpoint, ":")
		if colonIdx == -1 {
			return nil, errors.New("invalid initialCluster format")
		}

		endpoints = append(endpoints, newURL(endpoint[:colonIdx], defaultEtcdClientPort))
	}

	return endpoints, nil
}

// IsSeedNode returns whether the given hostID is an etcd node.
func IsSeedNode(initialCluster []environment.SeedNode, hostID string) bool {
	for _, seedNode := range initialCluster {
		if seedNode.HostID == hostID {
			return true
		}
	}

	return false
}

// TChannelConfiguration holds TChannel config options.
type TChannelConfiguration struct {
	// MaxIdleTime is the maximum idle time.
	MaxIdleTime time.Duration `yaml:"maxIdleTime"`
	// IdleCheckInterval is the idle check interval.
	IdleCheckInterval time.Duration `yaml:"idleCheckInterval"`
}
