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
	"strings"
	"time"

	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"
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
	// M3DBServiceName is the service name for M3DB
	M3DBServiceName = "m3db"
	// M3AggServiceName is the service name for M3Agg
	M3AggServiceName = "m3agg"

	// DefaultServiceEnvironment is the default service ID environment
	DefaultServiceEnvironment = "default_env"
	// DefaultServiceZone is the default service ID zone
	DefaultServiceZone = "embedded"

	// HeaderClusterEnvironmentName is the header used to specify the environment name.
	HeaderClusterEnvironmentName = "Cluster-Environment-Name"
	// HeaderClusterZoneName is the header used to specify the zone name.
	HeaderClusterZoneName = "Cluster-Zone-Name"
	// HeaderDryRun is the header used to specify whether this should be a dry run.
	HeaderDryRun = "Dry-Run"
)

var (
	errServiceNameIsRequired        = errors.New("service name is required")
	errServiceEnvironmentIsRequired = errors.New("service environment is required")
	errServiceZoneIsRequired        = errors.New("service zone is required")
	errUnableToParseService         = errors.New("unable to parse service")
	errM3AggServiceOptionsRequired  = errors.New("m3agg service options are required")

	allowedServices = allowedServicesSet{
		M3DBServiceName:  true,
		M3AggServiceName: true,
	}
)

// Handler represents a generic handler for placement endpoints.
type Handler struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	client clusterclient.Client
	cfg    config.Configuration
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

// NewServiceOptions returns a ServiceOptions with default options.
func NewServiceOptions(serviceName string) ServiceOptions {
	return ServiceOptions{
		ServiceName:        serviceName,
		ServiceEnvironment: DefaultServiceEnvironment,
		ServiceZone:        DefaultServiceZone,

		DryRun: false,
	}
}

// NewServiceOptionsFromHeaders returns a ServiceOptions based on the
// provided headers, if present.
func NewServiceOptionsFromHeaders(serviceName string, headers http.Header) ServiceOptions {
	opts := NewServiceOptions(serviceName)
	if v := strings.TrimSpace(headers.Get(HeaderClusterEnvironmentName)); v != "" {
		opts.ServiceEnvironment = v
	}
	if v := strings.TrimSpace(headers.Get(HeaderClusterZoneName)); v != "" {
		opts.ServiceZone = v
	}
	if v := strings.TrimSpace(headers.Get(HeaderDryRun)); v == "true" {
		opts.DryRun = true
	}
	return opts
}

// NewServiceOptionsWithDefaultM3AggValues returns a ServiceOptions with
// default M3Agg values to be used in the situation where the M3Agg values
// don't matter.
func NewServiceOptionsWithDefaultM3AggValues(headers http.Header) ServiceOptions {
	opts := NewServiceOptionsFromHeaders(M3AggServiceName, headers)
	opts.M3Agg = &M3AggServiceOptions{
		MaxAggregationWindowSize: time.Hour,
		WarmupDuration:           5 * time.Minute,
	}
	return opts
}

// Service gets a placement service from m3cluster client
func Service(clusterClient clusterclient.Client, opts ServiceOptions) (placement.Service, error) {
	ps, _, err := ServiceWithAlgo(clusterClient, opts)
	return ps, err
}

// ServiceWithAlgo gets a placement service from m3cluster client and
// additionally returns an algorithm instance for callers that need fine-grained
// control over placement updates.
func ServiceWithAlgo(clusterClient clusterclient.Client, opts ServiceOptions) (placement.Service, placement.Algorithm, error) {
	cs, err := clusterClient.Services(services.NewOverrideOptions())
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
	if opts.ServiceName == M3AggServiceName && opts.M3Agg == nil {
		return nil, nil, errM3AggServiceOptionsRequired
	}

	sid := services.NewServiceID().
		SetName(opts.ServiceName).
		SetEnvironment(opts.ServiceEnvironment).
		SetZone(opts.ServiceZone)

	pOpts := placement.NewOptions().
		SetValidZone(opts.ServiceZone).
		SetIsSharded(true).
		// Can use goal-based placement for both M3DB and M3Agg
		SetIsStaged(false).
		SetDryrun(opts.DryRun)

	if opts.ServiceName == M3AggServiceName {
		var (
			maxAggregationWindowSize = opts.M3Agg.MaxAggregationWindowSize
			warmupDuration           = opts.M3Agg.WarmupDuration
			now                      = time.Now()
		)
		pOpts = pOpts.
			SetIsMirrored(true).
			// TODO(rartoul): Do we need to set placement cutover time? Seems like that would
			// be covered by shardCutOverNanosFn
			// Cutovers control when new shards will begin receiving writes, so we set them to take
			// effect immediately as we're trying to achieve goal-based placement.
			SetPlacementCutoverNanosFn(immediateTimeNanosFn).
			SetShardCutoverNanosFn(immediateTimeNanosFn).
			// Cutoffs control when Leaving shards stop receiving writes.
			SetShardCutoffNanosFn(newShardCutOffNanosFn(now, maxAggregationWindowSize, warmupDuration)).
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
func RegisterRoutes(r *mux.Router, client clusterclient.Client, cfg config.Configuration) {
	// Init
	initFn := applyMiddleware(NewInitHandler(client, cfg).ServeHTTP)
	r.HandleFunc(DeprecatedM3DBInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3DBInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3AggInitURL, initFn).Methods(InitHTTPMethod)

	// Get
	getFn := applyMiddleware(NewGetHandler(client, cfg).ServeHTTP)
	r.HandleFunc(DeprecatedM3DBGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3DBGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3AggGetURL, getFn).Methods(GetHTTPMethod)

	// Delete all
	deleteAllFn := applyMiddleware(NewDeleteAllHandler(client, cfg).ServeHTTP)
	r.HandleFunc(DeprecatedM3DBDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3DBDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3AggDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)

	// Add
	addFn := applyMiddleware(NewAddHandler(client, cfg).ServeHTTP)
	r.HandleFunc(DeprecatedM3DBAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3DBAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3AggAddURL, addFn).Methods(AddHTTPMethod)

	// Delete
	deleteFn := applyMiddleware(NewDeleteHandler(client, cfg).ServeHTTP)
	r.HandleFunc(DeprecatedM3DBDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3DBDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3AggDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
}

// immediateTimeNanosFn returns the earliest possible unix nano timestamp to indicate
// that changes should take effect immediately.
func immediateTimeNanosFn() int64 {
	return 0
}

// We want to generate a function that will return the cutoff such that it is always at the beginning
// of an aggregation window size, but also later than or equal to the current time + warmup. We accomplish
// this by adding the warmup time and the max aggregation window size to the current time, and then truncating
// to the max aggregation window size. This ensure that we always return a time that is at the beginning of an
// aggregation window size, but is also later than now.Add(warmup).
func newShardCutOffNanosFn(now time.Time, maxAggregationWindowSize, warmup time.Duration) placement.TimeNanosFn {
	return func() int64 {
		return now.
			Add(warmup).
			Add(maxAggregationWindowSize).
			Truncate(maxAggregationWindowSize).
			Unix()
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
			// TODO(rartoul): This seems overly cautious, basically it requires an entire maxAggregationWindowSize
			// to elapse before "leaving" shards can be cleaned up.
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

// ServeHTTPWithService is the interface for serving HTTP requests after
// parsing the service name from the URL.
type ServeHTTPWithService interface {
	ServeHTTP(serviceName string, w http.ResponseWriter, r *http.Request)
}

func applyMiddleware(f func(serviceName string, w http.ResponseWriter, r *http.Request)) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeLoggingFunc(
		parseServiceMiddleware(
			f))
}

func parseServiceMiddleware(
	next func(serviceName string, w http.ResponseWriter, r *http.Request),
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		serviceName, err := parseServiceFromRequest(r)
		if err != nil {
			handler.Error(w, err, http.StatusBadRequest)
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
			switch service {
			case M3DBServiceName:
				return M3DBServiceName, nil
			case M3AggServiceName:
				return M3AggServiceName, nil
			default:
				return "", fmt.Errorf("unknown service: %s", service)
			}
		}
	}

	return "", errUnableToParseService
}
