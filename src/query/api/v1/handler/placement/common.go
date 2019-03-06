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

	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/algo"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler"
	"github.com/m3db/m3/src/query/util/logging"
	xhttp "github.com/m3db/m3/src/x/net/http"

	"github.com/gorilla/mux"
)

const (
	// ServicesPathName is the services part of the API path.
	ServicesPathName = "services"
	// PlacementPathName is the placement part of the API path.
	PlacementPathName = "placement"

	m3AggregatorPlacementNamespace = "/placement"
)

var (
	// M3DBServicePlacementPathName is the M3DB service placement API path.
	M3DBServicePlacementPathName = path.Join(ServicesPathName, handler.M3DBServiceName, PlacementPathName)
	// M3AggServicePlacementPathName is the M3Agg service placement API path.
	M3AggServicePlacementPathName = path.Join(ServicesPathName, handler.M3AggregatorServiceName, PlacementPathName)
	// M3CoordinatorServicePlacementPathName is the M3Coordinator service placement API path.
	M3CoordinatorServicePlacementPathName = path.Join(ServicesPathName, handler.M3CoordinatorServiceName, PlacementPathName)

	errUnableToParseService = errors.New("unable to parse service")
)

// HandlerOptions is the options struct for the handler.
type HandlerOptions struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	ClusterClient clusterclient.Client
	Config        config.Configuration

	M3AggServiceOptions *handler.M3AggServiceOptions
}

// NewHandlerOptions is the constructor function for HandlerOptions.
func NewHandlerOptions(
	client clusterclient.Client,
	cfg config.Configuration,
	m3AggOpts *handler.M3AggServiceOptions,
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

// Service gets a placement service from m3cluster client
func Service(
	clusterClient clusterclient.Client,
	opts handler.ServiceOptions,
	now time.Time,
	validationFn placement.ValidateFn,
) (placement.Service, error) {
	ps, _, err := ServiceWithAlgo(clusterClient, opts, now, validationFn)
	return ps, err
}

// ServiceWithAlgo gets a placement service from m3cluster client and
// additionally returns an algorithm instance for callers that need fine-grained
// control over placement updates.
func ServiceWithAlgo(
	clusterClient clusterclient.Client,
	opts handler.ServiceOptions,
	now time.Time,
	validationFn placement.ValidateFn,
) (placement.Service, placement.Algorithm, error) {
	overrides := services.NewOverrideOptions()
	switch opts.ServiceName {
	case handler.M3AggregatorServiceName:
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

	if err := opts.Validate(); err != nil {
		return nil, nil, err
	}

	if !handler.IsAllowedService(opts.ServiceName) {
		return nil, nil, fmt.Errorf(
			"invalid service name: %s, must be one of: %v",
			opts.ServiceName, handler.AllowedServices())
	}

	sid := opts.ServiceID()
	pOpts := placement.NewOptions().
		SetValidZone(opts.ServiceZone).
		SetIsSharded(true).
		SetDryrun(opts.DryRun)

	switch opts.ServiceName {
	case handler.M3CoordinatorServiceName:
		pOpts = pOpts.
			SetIsSharded(false)
	case handler.M3AggregatorServiceName:
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

	if validationFn != nil {
		pOpts = pOpts.SetValidateFnBeforeUpdate(validationFn)
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

	// Replace
	var (
		replaceHandler = NewReplaceHandler(opts)
		replaceFn      = applyMiddleware(replaceHandler.ServeHTTP)
	)
	r.HandleFunc(M3DBReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)
	r.HandleFunc(M3AggReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)
	r.HandleFunc(M3CoordinatorReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)
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

func newShardCutOverValidationFn(now time.Time) placement.ShardValidateFn {
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

func newShardCutOffValidationFn(now time.Time, maxAggregationWindowSize time.Duration) placement.ShardValidateFn {
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

type unsafeAddError struct {
	hosts string
}

func (e unsafeAddError) Error() string {
	return fmt.Sprintf("instances [%s] do not have all shards available", e.hosts)
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

func applyMiddleware(
	f func(serviceName string, w http.ResponseWriter, r *http.Request),
) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeAndPanicErrorLoggingFunc(
		parseServiceMiddleware(f),
	).ServeHTTP
}

func applyDeprecatedMiddleware(
	f func(serviceName string, w http.ResponseWriter, r *http.Request),
) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeAndPanicErrorLoggingFunc(
		func(w http.ResponseWriter, r *http.Request) {
			f(handler.M3DBServiceName, w, r)
		},
	).ServeHTTP
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
			if handler.IsAllowedService(service) {
				return service, nil
			}
			return "", fmt.Errorf("unknown service: %s", service)
		}
	}

	return "", errUnableToParseService
}

func isStateless(serviceName string) bool {
	switch serviceName {
	case handler.M3CoordinatorServiceName:
		return true
	}
	return false
}
