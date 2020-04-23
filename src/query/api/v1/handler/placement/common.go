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

	"github.com/m3db/m3/src/aggregator/aggregator"
	clusterclient "github.com/m3db/m3/src/cluster/client"
	"github.com/m3db/m3/src/cluster/generated/proto/placementpb"
	"github.com/m3db/m3/src/cluster/kv"
	"github.com/m3db/m3/src/cluster/placement"
	"github.com/m3db/m3/src/cluster/placement/algo"
	"github.com/m3db/m3/src/cluster/services"
	"github.com/m3db/m3/src/cluster/shard"
	"github.com/m3db/m3/src/cmd/services/m3query/config"
	"github.com/m3db/m3/src/query/api/v1/handler/prometheus/handleroptions"
	"github.com/m3db/m3/src/query/util/logging"
	xerrors "github.com/m3db/m3/src/x/errors"
	"github.com/m3db/m3/src/x/instrument"
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
	M3DBServicePlacementPathName = path.Join(ServicesPathName,
		handleroptions.M3DBServiceName, PlacementPathName)
	// M3AggServicePlacementPathName is the M3Agg service placement API path.
	M3AggServicePlacementPathName = path.Join(ServicesPathName,
		handleroptions.M3AggregatorServiceName, PlacementPathName)
	// M3CoordinatorServicePlacementPathName is the M3Coordinator
	// service placement API path.
	M3CoordinatorServicePlacementPathName = path.Join(ServicesPathName,
		handleroptions.M3CoordinatorServiceName, PlacementPathName)

	errUnableToParseService    = errors.New("unable to parse service")
	errInstrumentOptionsNotSet = errors.New("instrument options not set")
)

// HandlerOptions is the options struct for the handler.
type HandlerOptions struct {
	// This is used by other placement Handlers
	// nolint: structcheck
	clusterClient clusterclient.Client
	config        config.Configuration

	m3AggServiceOptions *handleroptions.M3AggServiceOptions
	instrumentOptions   instrument.Options
}

// NewHandlerOptions is the constructor function for HandlerOptions.
func NewHandlerOptions(
	client clusterclient.Client,
	cfg config.Configuration,
	m3AggOpts *handleroptions.M3AggServiceOptions,
	instrumentOpts instrument.Options,
) (HandlerOptions, error) {
	if instrumentOpts == nil {
		return HandlerOptions{}, errInstrumentOptionsNotSet
	}
	return HandlerOptions{
		clusterClient:       client,
		config:              cfg,
		m3AggServiceOptions: m3AggOpts,
		instrumentOptions:   instrumentOpts,
	}, nil
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
	opts handleroptions.ServiceOptions,
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
	opts handleroptions.ServiceOptions,
	now time.Time,
	validationFn placement.ValidateFn,
) (placement.Service, placement.Algorithm, error) {
	overrides := services.NewOverrideOptions()
	switch opts.ServiceName {
	case handleroptions.M3AggregatorServiceName:
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

	if !handleroptions.IsAllowedService(opts.ServiceName) {
		return nil, nil, fmt.Errorf(
			"invalid service name: %s, must be one of: %v",
			opts.ServiceName, handleroptions.AllowedServices())
	}

	sid := opts.ServiceID()
	pOpts := placement.NewOptions().
		SetValidZone(opts.ServiceZone).
		SetIsSharded(true).
		SetDryrun(opts.DryRun)

	switch opts.ServiceName {
	case handleroptions.M3CoordinatorServiceName:
		pOpts = pOpts.
			SetIsSharded(false)
	case handleroptions.M3AggregatorServiceName:
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
		instance, err := placement.NewInstanceFromProto(instanceProto)
		if err != nil {
			return nil, err
		}
		res = append(res, instance)
	}

	return res, nil
}

// RegisterRoutes registers the placement routes
func RegisterRoutes(
	r *mux.Router,
	defaults []handleroptions.ServiceOptionsDefault,
	opts HandlerOptions,
) {
	// Init
	var (
		initHandler      = NewInitHandler(opts)
		deprecatedInitFn = applyDeprecatedMiddleware(initHandler.ServeHTTP, defaults, opts.instrumentOptions)
		initFn           = applyMiddleware(initHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(DeprecatedM3DBInitURL, deprecatedInitFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3DBInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3AggInitURL, initFn).Methods(InitHTTPMethod)
	r.HandleFunc(M3CoordinatorInitURL, initFn).Methods(InitHTTPMethod)

	// Get
	var (
		getHandler      = NewGetHandler(opts)
		deprecatedGetFn = applyDeprecatedMiddleware(getHandler.ServeHTTP, defaults, opts.instrumentOptions)
		getFn           = applyMiddleware(getHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(DeprecatedM3DBGetURL, deprecatedGetFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3DBGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3AggGetURL, getFn).Methods(GetHTTPMethod)
	r.HandleFunc(M3CoordinatorGetURL, getFn).Methods(GetHTTPMethod)

	// Delete all
	var (
		deleteAllHandler      = NewDeleteAllHandler(opts)
		deprecatedDeleteAllFn = applyDeprecatedMiddleware(deleteAllHandler.ServeHTTP, defaults, opts.instrumentOptions)
		deleteAllFn           = applyMiddleware(deleteAllHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(DeprecatedM3DBDeleteAllURL, deprecatedDeleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3DBDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3AggDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)
	r.HandleFunc(M3CoordinatorDeleteAllURL, deleteAllFn).Methods(DeleteAllHTTPMethod)

	// Add
	var (
		addHandler      = NewAddHandler(opts)
		deprecatedAddFn = applyDeprecatedMiddleware(addHandler.ServeHTTP, defaults, opts.instrumentOptions)
		addFn           = applyMiddleware(addHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(DeprecatedM3DBAddURL, deprecatedAddFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3DBAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3AggAddURL, addFn).Methods(AddHTTPMethod)
	r.HandleFunc(M3CoordinatorAddURL, addFn).Methods(AddHTTPMethod)

	// Delete
	var (
		deleteHandler      = NewDeleteHandler(opts)
		deprecatedDeleteFn = applyDeprecatedMiddleware(deleteHandler.ServeHTTP, defaults, opts.instrumentOptions)
		deleteFn           = applyMiddleware(deleteHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(DeprecatedM3DBDeleteURL, deprecatedDeleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3DBDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3AggDeleteURL, deleteFn).Methods(DeleteHTTPMethod)
	r.HandleFunc(M3CoordinatorDeleteURL, deleteFn).Methods(DeleteHTTPMethod)

	// Replace
	var (
		replaceHandler = NewReplaceHandler(opts)
		replaceFn      = applyMiddleware(replaceHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(M3DBReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)
	r.HandleFunc(M3AggReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)
	r.HandleFunc(M3CoordinatorReplaceURL, replaceFn).Methods(ReplaceHTTPMethod)

	// Set
	var (
		setHandler = NewSetHandler(opts)
		setFn      = applyMiddleware(setHandler.ServeHTTP, defaults, opts.instrumentOptions)
	)
	r.HandleFunc(M3DBSetURL, setFn).Methods(SetHTTPMethod)
	r.HandleFunc(M3AggSetURL, setFn).Methods(SetHTTPMethod)
	r.HandleFunc(M3CoordinatorSetURL, setFn).Methods(SetHTTPMethod)
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
	f func(svc handleroptions.ServiceNameAndDefaults, w http.ResponseWriter, r *http.Request),
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeAndPanicErrorLoggingFunc(
		parseServiceMiddleware(f, defaults),
		instrumentOpts,
	).ServeHTTP
}

func applyDeprecatedMiddleware(
	f func(svc handleroptions.ServiceNameAndDefaults, w http.ResponseWriter, r *http.Request),
	defaults []handleroptions.ServiceOptionsDefault,
	instrumentOpts instrument.Options,
) func(w http.ResponseWriter, r *http.Request) {
	return logging.WithResponseTimeAndPanicErrorLoggingFunc(
		func(w http.ResponseWriter, r *http.Request) {
			svc := handleroptions.ServiceNameAndDefaults{
				ServiceName: handleroptions.M3DBServiceName,
				Defaults:    defaults,
			}
			f(svc, w, r)
		},
		instrumentOpts,
	).ServeHTTP
}

func parseServiceMiddleware(
	next func(svc handleroptions.ServiceNameAndDefaults, w http.ResponseWriter, r *http.Request),
	defaults []handleroptions.ServiceOptionsDefault,
) func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var (
			svc = handleroptions.ServiceNameAndDefaults{Defaults: defaults}
			err error
		)
		svc.ServiceName, err = parseServiceFromRequest(r)
		if err != nil {
			xhttp.Error(w, err, http.StatusBadRequest)
			return
		}

		next(svc, w, r)
	}
}

func parseServiceFromRequest(r *http.Request) (string, error) {
	path := r.URL.Path
	components := strings.Split(path, "/")
	for i, c := range components {
		if c == "services" && i+1 < len(components) {
			service := components[i+1]
			if handleroptions.IsAllowedService(service) {
				return service, nil
			}
			return "", fmt.Errorf("unknown service: %s", service)
		}
	}

	return "", errUnableToParseService
}

func isStateless(serviceName string) bool {
	switch serviceName {
	case handleroptions.M3CoordinatorServiceName:
		return true
	}
	return false
}

func deleteAggregatorShardSetIDRelatedKeys(
	svc handleroptions.ServiceNameAndDefaults,
	svcOpts handleroptions.ServiceOptions,
	clusterClient clusterclient.Client,
	shardSetIDs []uint32,
) error {
	if svc.ServiceName != handleroptions.M3AggregatorServiceName {
		return fmt.Errorf("error deleting aggregator instance keys, bad service: %s",
			svc.ServiceName)
	}

	kvOpts := kv.NewOverrideOptions().
		SetEnvironment(svcOpts.ServiceEnvironment).
		SetZone(svcOpts.ServiceZone)

	kvStore, err := clusterClient.Store(kvOpts)
	if err != nil {
		return fmt.Errorf("cannot get KV store to delete aggregator keys: %v", err)
	}

	var (
		flushTimesMgrOpts = aggregator.NewFlushTimesManagerOptions()
		electionMgrOpts   = aggregator.NewElectionManagerOptions()
		multiErr          = xerrors.NewMultiError()
	)
	for _, shardSetID := range shardSetIDs {
		// Check if flush times key exists, if so delete.
		flushTimesKey := fmt.Sprintf(flushTimesMgrOpts.FlushTimesKeyFmt(),
			shardSetID)
		_, flushTimesKeyErr := kvStore.Get(flushTimesKey)
		if flushTimesKeyErr != nil && flushTimesKeyErr != kv.ErrNotFound {
			multiErr = multiErr.Add(fmt.Errorf(
				"error check flush times key exists for deleted instance: %v",
				flushTimesKeyErr))
		}
		if flushTimesKeyErr == nil {
			// Need to delete the flush times key.
			if _, err := kvStore.Delete(flushTimesKey); err != nil {
				multiErr = multiErr.Add(fmt.Errorf(
					"error delete flush times key for deleted instance: %v", err))
			}
		}

		// Check if election manager lock key exists, if so delete.
		electionKey := fmt.Sprintf(electionMgrOpts.ElectionKeyFmt(),
			shardSetID)
		_, electionKeyErr := kvStore.Get(electionKey)
		if electionKeyErr != nil && electionKeyErr != kv.ErrNotFound {
			multiErr = multiErr.Add(fmt.Errorf(
				"error checking election key exists for deleted instance: %v", err))
		}
		if electionKeyErr == nil {
			// Need to delete the election key.
			if _, err := kvStore.Delete(flushTimesKey); err != nil {
				multiErr = multiErr.Add(fmt.Errorf(
					"error delete election key for deleted instance: %v", err))
			}
		}
	}

	return multiErr.FinalError()
}
