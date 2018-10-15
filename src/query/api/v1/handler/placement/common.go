// Copyright (c) 2018 Uber Technologies, Inc.
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

package placement

import (
	"errors"
	"fmt"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/util/logging"
	"github.com/m3db/m3/src/x/net/http"
	clusterclient "github.com/m3db/m3cluster/client"
	"github.com/m3db/m3cluster/generated/proto/placementpb"
	"github.com/m3db/m3cluster/placement"
	"github.com/m3db/m3cluster/placement/algo"
	"github.com/m3db/m3cluster/services"
	"github.com/m3db/m3cluster/shard"

	"github.com/gorilla/mux"
)

type allowedServicesSet map[string]bool

func (a allowedServicesSet) String() []string {
	s := make([]string, 0, len(a))
	for key := range a {
		s = append(s, key)
	}
	return s
}

const (
	// M3DBServiceName is the service name for M3DB.
	M3DBServiceName = "m3db"
	// M3AggregatorServiceName is the service name for M3Aggregator.
	M3AggregatorServiceName = "m3aggregator"
	// M3CoordinatorServiceName is the service name for M3Coordinator.
	M3CoordinatorServiceName = "m3coordinator"
	// ServicesPathName is the services part of the API path.
	ServicesPathName = "services"
	// PlacementPathName is the placement part of the API path.
	PlacementPathName = "placement"

	// DefaultServiceEnvironment is the default service ID environment.
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone.
	DefaultServiceZone = "embedded"

	// HeaderClusterEnvironmentName is the header used to specify the environment name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
	// HeaderDryRun is the header used to specify whether this should be a dry run.
	HeaderDryRun = "Dry-Run"

	defaultM3AggMaxAggregationWindowSize = time.Minute
	// defaultM3AggWarmupDuration configures the buffer to account for the delay
	// of propagating aggregator placement to clients, usually needed when there is
	// a large amount of clients sending traffic to m3aggregator.
	defaultM3AggWarmupDuration = 0

	m3AggregatorPlacementNamespace = "/placement"
)

var (
	errServiceNameIsRequired        = errors.New("service name is required")
	errServiceEnvironmentIsRequired = errors.New("service environment is required")
	errServiceZoneIsRequired        = errors.New("service zone is required")
	errUnableToParseService         = errors.New("unable to parse service")
	errM3AggServiceOptionsRequired  = errors.New("m3agg service options are required")

	allowedServices = allowedServicesSet{
		M3DBServiceName:          true,
		M3AggregatorServiceName:  true,
		M3CoordinatorServiceName: true,
	}

	// M3DBServicePlacementPathName is the M3DB service placement API path.
	M3DBServicePlacementPathName = path.Join(ServicesPathName, M3DBServiceName, PlacementPathName)
	// M3AggServicePlacementPathName is the M3Agg service placement API path.
	M3AggServicePlacementPathName = path.Join(ServicesPathName, M3AggregatorServiceName, PlacementPathName)
	// M3CoordinatorServicePlacementPathName is the M3Coordinator service placement API path.
	M3CoordinatorServicePlacementPathName = path.Join(ServicesPathName, M3CoordinatorServiceName, PlacementPathName)
)

// HandlerOptions is the options struct for the handler.
type HandlerOptions struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	ClusterClient clusterclient.Client
	Config        config.Configuration

	M3AggServiceOptions *M3AggServiceOptions
}

// NewHandlerOptions is the constructor function for HandlerOptions.
func NewHandlerOptions(
	client clusterclient.Client,
	cfg config.Configuration,
	m3AggOpts *M3AggServiceOptions,
) HandlerOptions {
	return HandlerOptions{
		ClusterClient:       client,
		Config:              cfg,
		M3AggServiceOptions: m3AggOpts,
	}
}

// Handler represents a generic handler for placement endpoints.
type Handler struct {
	HandlerOptions

	// nolint: structcheck
	nowFn func() time.Time
}

// ServiceOptions are the options for Service.
type ServiceOptions struct {
	ServiceName        string
	ServiceEnvironment string
	ServiceZone        string

	M3Agg *M3AggServiceOptions

	DryRun bool
}

// M3AggServiceOptions contains the service options that are
// specific to the M3Agg service.
type M3AggServiceOptions struct {
	MaxAggregationWindowSize time.Duration
	WarmupDuration           time.Duration
}

// NewServiceOptions returns a ServiceOptions based on the provided
// values.
func NewServiceOptions(
	serviceName string, headers http.Header, m3AggOpts *M3AggServiceOptions) ServiceOptions {
	opts := ServiceOptions{
		ServiceName:        serviceName,
		ServiceEnvironment: DefaultServiceEnvironment,
		ServiceZone:        DefaultServiceZone,

		DryRun: false,

		M3Agg: &M3AggServiceOptions{
			MaxAggregationWindowSize: defaultM3AggMaxAggregationWindowSize,
			WarmupDuration:           defaultM3AggWarmupDuration,
		},
	}

	if v := strings.TrimSpace(headers.Get(HeaderClusterEnvironmentName)); v != "" {
		opts.ServiceEnvironment = v
	}
	if v := strings.TrimSpace(headers.Get(HeaderClusterZoneName)); v != "" {
		opts.ServiceZone = v
	}
	if v := strings.TrimSpace(headers.Get(HeaderDryRun)); v == "true" {
		opts.DryRun = true
	}

	if m3AggOpts != nil {
		if m3AggOpts.MaxAggregationWindowSize > 0 {
			opts.M3Agg.MaxAggregationWindowSize = m3AggOpts.MaxAggregationWindowSize
		}

		if m3AggOpts.WarmupDuration > 0 {
			opts.M3Agg.WarmupDuration = m3AggOpts.MaxAggregationWindowSize
		}
	}

	return opts
}

// Service gets a placement service from m3cluster client
func Service(
	clusterClient clusterclient.Client,
	opts ServiceOptions,
	now time.Time,
) (placement.Service, error) {
	ps, _, err := ServiceWithAlgo(clusterClient, opts, now)
	return ps, err
}

// ServiceWithAlgo gets a placement service from m3cluster client and
// additionally returns an algorithm instance for callers that need fine-grained
// control over placement updates.
func ServiceWithAlgo(
	clusterClient clusterclient.Client,
	opts ServiceOptions,
	now time.Time,
) (placement.Service, placement.Algorithm, error) {
	overrides := services.NewOverrideOptions()
	switch opts.ServiceName {
	case M3AggregatorServiceName:
		overrides = overrides.
			SetNamespaceOptions(
				overrides.NamespaceOptions().
					SetPlacementNamespace(m3AggregatorPlacementNamespace),
			)
	}

	cs, err := clusterClient.Services(overrides)
	if err != nil {
		return nil, nil, err
	}

	if _, ok := allowedServices[opts.ServiceName]; !ok {
		return nil, nil, fmt.Errorf(
			"invalid service name: %s, must be one of: %s",
			opts.ServiceName, allowedServices.String())
	}
	if opts.ServiceName == "" {
		return nil, nil, errServiceNameIsRequired
	}
	if opts.ServiceEnvironment == "" {
		return nil, nil, errServiceEnvironmentIsRequired
	}
	if opts.ServiceZone == "" {
		return nil, nil, errServiceZoneIsRequired
	}
	if opts.ServiceName == M3AggregatorServiceName && opts.M3Agg == nil {
		return nil, nil, errM3AggServiceOptionsRequired
	}

	sid := services.NewServiceID().
		SetName(opts.ServiceName).
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)

	pOpts := placement.NewOptions().
		SetValidZone(opts.ServiceZone).
		SetIsSharded(true).
		SetDryrun(opts.DryRun)

	switch opts.ServiceName {
	case M3CoordinatorServiceName:
		pOpts = pOpts.
			SetIsSharded(false)
	case M3AggregatorServiceName:
		var (
			maxAggregationWindowSize = opts.M3Agg.MaxAggregationWindowSize
			warmupDuration           = opts.M3Agg.WarmupDuration
			// For now these are not configurable, but we include them to
			// make the code match r2admin for ease of debugging / migration.
			placementCutoverOpts = m3aggregatorPlacementOpts{}
		)
		pOpts = pOpts.
			// M3Agg expects a mirrored and staged placement.
			SetIsMirrored(true).
			SetIsStaged(true).
			// placementCutover controls when the new placement will begin to be considered
			// the new placement. Since we're trying to do goal-based placement, we set it
			// such that it takes effect immediately.
			SetPlacementCutoverNanosFn(newPlacementCutoverNanosFn(
				now, placementCutoverOpts)).
			// shardCutover controls when the clients (who have received the new placement)
			// will begin dual-writing to the new shards. We could set it to take effect
			// immediately, but right now we use the same logic as r2admin for consistency.
			SetShardCutoverNanosFn(newShardCutoverNanosFn(
				now, maxAggregationWindowSize, warmupDuration, placementCutoverOpts)).
			// Cutoffs control when Leaving shards stop receiving writes.
			SetShardCutoffNanosFn(newShardCutOffNanosFn(
				now, maxAggregationWindowSize, warmupDuration, placementCutoverOpts)).
			SetIsShardCutoverFn(newShardCutOverValidationFn(now)).
			SetIsShardCutoffFn(newShardCutOffValidationFn(now, maxAggregationWindowSize))
	}

	ps, err := cs.PlacementService(sid, pOpts)
	if err != nil {
		return nil, nil, err
	}

	alg := algo.NewAlgorithm(pOpts)

	return ps, alg, nil
}

// ConvertInstancesProto converts a slice of protobuf `Instance`s to `placement.Instance`s
func ConvertInstancesProto(instancesProto []*placementpb.Instance) ([]placement.Instance, error) {
	res := make([]placement.Instance, 0, len(instancesProto))

	for _, instanceProto := range instancesProto {
		shards, err := shard.NewShardsFromProto(instanceProto.Shards)
		if err != nil {
			return nil, err
		}

		instance := placement.NewInstance().
			SetEndpoint(instanceProto.Endpoint).
			SetHostname(instanceProto.Hostname).
			SetID(instanceProto.Id).
			SetPort(instanceProto.Port).
			SetIsolationGroup(instanceProto.IsolationGroup).
			SetShards(shards).
			SetShardSetID(instanceProto.ShardSetId).
			SetWeight(instanceProto.Weight).
			SetZone(instanceProto.Zone)

		res = append(res, instance)
	}

	return res, nil
}

// RegisterRoutes registers the placement routes
func RegisterRoutes(r *mux.Router, opts HandlerOptions) {
	// Init
	var (
		initHandler      = NewInitHandler(opts)
		deprecatedInitFn = applyDeprecatedMiddleware(initHandler.ServeHTTP)
		initFn           = applyMiddleware(initHandler.ServeHTTP)
	)
	r.HandleFunc(DeprecatedM3DBInitURL, deprecatedInitFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3DBInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3AggInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3CoordinatorInitURL, initFn).Methods(InitHTTPMethod)

	// Get
	var (
		getHandler      = NewGetHandler(opts)
		deprecatedGetFn = applyDeprecatedMiddleware(getHandler.ServeHTTP)
		getFn           = applyMiddleware(getHandler.ServeHTTP)
	)
	r.HandleFunc(DeprecatedM3DBGetURL, deprecatedGetFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3DBGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3AggGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3CoordinatorGetURL, getFn).Methods(GetHTTPMethod)

	// Delete all
	var (
		deleteAllHandler      = NewDeleteAllHandler(opts)
		deprecatedDeleteAllFn = applyDeprecatedMiddleware(deleteAllHandler.ServeHTTP)
		deleteAllFn           = applyMiddleware(deleteAllHandler.ServeHTTP)
	)
	r.HandleFunc(DeprecatedM3DBDeleteAllURL, deprecatedDeleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3DBDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3AggDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3CoordinatorDeleteAllURL, getFn).Methods(GetHTTPMethod)

	// Add
	var (
		addHandler      = NewAddHandler(opts)
		deprecatedAddFn = applyDeprecatedMiddleware(addHandler.ServeHTTP)
		addFn           = applyMiddleware(addHandler.ServeHTTP)
	)
	r.HandleFunc(DeprecatedM3DBAddURL, deprecatedAddFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3DBAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3AggAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3CoordinatorAddURL, getFn).Methods(GetHTTPMethod)

	// Delete
	var (
		deleteHandler      = NewDeleteHandler(opts)
		deprecatedDeleteFn = applyDeprecatedMiddleware(deleteHandler.ServeHTTP)
		deleteFn           = applyMiddleware(deleteHandler.ServeHTTP)
	)
	r.HandleFunc(DeprecatedM3DBDeleteURL, deprecatedDeleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3DBDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3AggDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3CoordinatorDeleteURL, getFn).Methods(GetHTTPMethod)
}

func newPlacementCutoverNanosFn(
	now time.Time, cutoverOpts m3aggregatorPlacementOpts) placement.TimeNanosFn {
	return func() int64 {
		return placementCutoverTime(now, cutoverOpts).UnixNano()
	}
}

func placementCutoverTime(
	now time.Time, opts m3aggregatorPlacementOpts) time.Time {
	return now.
		Add(opts.maxPositiveSkew).
		Add(opts.maxNegativeSkew).
		Add(opts.propagationDelay)
}

func newShardCutOffNanosFn(
	now time.Time,
	maxAggregationWindowSize,
	warmup time.Duration,
	cutoverOpts m3aggregatorPlacementOpts) placement.TimeNanosFn {
	return newShardCutoverNanosFn(
		now, maxAggregationWindowSize, warmup, cutoverOpts)
}

func newShardCutoverNanosFn(
	now time.Time,
	maxAggregationWindowSize,
	warmUpDuration time.Duration,
	cutoverOpts m3aggregatorPlacementOpts) placement.TimeNanosFn {
	return func() int64 {
		var (
			windowSize = maxAggregationWindowSize
			cutover    = placementCutoverTime(now, cutoverOpts).Add(warmUpDuration)
			truncated  = cutover.Truncate(windowSize)
		)
		if truncated.Before(cutover) {
			return truncated.Add(windowSize).UnixNano()
		}
		return truncated.UnixNano()
	}
}

func newShardCutOverValidationFn(now time.Time) placement.ShardValidationFn {
	return func(s shard.Shard) error {
		switch s.State() {
		case shard.Initializing:
			if s.CutoverNanos() > now.UnixNano() {
				return fmt.Errorf("could not mark shard %d available before cutover time %v", s.ID(), time.Unix(0, s.CutoverNanos()))
			}
			return nil
		default:
			return fmt.Errorf("could not mark shard %d available, invalid state %s", s.ID(), s.State().String())
		}
	}
}

func newShardCutOffValidationFn(now time.Time, maxAggregationWindowSize time.Duration) placement.ShardValidationFn {
	return func(s shard.Shard) error {
		switch s.State() {
		case shard.Leaving:
			if s.CutoffNanos() > now.UnixNano()-maxAggregationWindowSize.Nanoseconds() {
				return fmt.Errorf("could not return leaving shard %d with cutoff time %v, max aggregation window %v",
					s.ID(), time.Unix(0, s.CutoffNanos()), maxAggregationWindowSize)
			}
			return nil
		default:
			return fmt.Errorf("could not mark shard %d available, invalid state %s", s.ID(), s.State().String())
		}
	}
}

type m3aggregatorPlacementOpts struct {
	maxPositiveSkew  time.Duration
	maxNegativeSkew  time.Duration
	propagationDelay time.Duration
}

func validateAllAvailable(p placement.Placement) error {
	badInsts := []string{}
	for _, inst := range p.Instances() {
		if !inst.IsAvailable() {
			badInsts = append(badInsts, inst.ID())
		}
	}
	if len(badInsts) > 0 {
		return unsafeAddError{
			hosts: strings.Join(badInsts, ","),
		}
	}
	return nil
}

func applyMiddleware(f func(serviceName string, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeLoggingFunc(
		parseServiceMiddleware(
			f))
}

func applyDeprecatedMiddleware(f func(serviceName string, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeLoggingFunc(
		func(w http.ResponseWriter, r *http.Request) {
			f(M3DBServiceName, w, r)
		})
}

func parseServiceMiddleware(
	next func(serviceName string, w http.ResponseWriter, r *http.Request),
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		serviceName, err := parseServiceFromRequest(r)
		if err != nil {
			xhttp.Error(w, err, http.StatusBadRequest)
			return
		}

		next(serviceName, w, r)
	}
}

func parseServiceFromRequest(r *http.Request) (string, error) {
	path := r.URL.Path
	components := strings.Split(path, "/")
	for i, c := range components {
		if c == "services" && i+1 < len(components) {
			service := components[i+1]
			if _, ok := allowedServices[service]; ok {
				return service, nil
			}
			return "", fmt.Errorf("unknown service: %s", service)
		}
	}

	return "", errUnableToParseService
}
