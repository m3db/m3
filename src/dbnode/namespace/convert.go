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

package namespace

import (
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	nsproto "github.com/m3db/m3/src/dbnode/generated/proto/namespace"
	"github.com/m3db/m3/src/dbnode/retention"
	"github.com/m3db/m3/src/x/ident"
	xtime "github.com/m3db/m3/src/x/time"

	"github.com/gogo/protobuf/proto"
	protobuftypes "github.com/gogo/protobuf/types"
)

var (
	errRetentionNil       = errors.New("retention options must be set")
	errNamespaceNil       = errors.New("namespace options must be set")
	errExtendedOptionsNil = errors.New("extendedOptions.Options must be set")

	dynamicExtendedOptionsConverters = sync.Map{}
)

// FromNanos converts nanoseconds to a namespace-compatible duration.
func FromNanos(n int64) time.Duration {
	return xtime.FromNormalizedDuration(n, time.Nanosecond)
}

// ToRetention converts nsproto.RetentionOptions to retention.Options
func ToRetention(
	ro *nsproto.RetentionOptions,
) (retention.Options, error) {
	if ro == nil {
		return nil, errRetentionNil
	}

	ropts := retention.NewOptions().
		SetRetentionPeriod(FromNanos(ro.RetentionPeriodNanos)).
		SetFutureRetentionPeriod(FromNanos(ro.FutureRetentionPeriodNanos)).
		SetBlockSize(FromNanos(ro.BlockSizeNanos)).
		SetBufferFuture(FromNanos(ro.BufferFutureNanos)).
		SetBufferPast(FromNanos(ro.BufferPastNanos)).
		SetBlockDataExpiry(ro.BlockDataExpiry).
		SetBlockDataExpiryAfterNotAccessedPeriod(
			FromNanos(ro.BlockDataExpiryAfterNotAccessPeriodNanos))

	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	return ropts, nil
}

// ToIndexOptions converts nsproto.IndexOptions to IndexOptions
func ToIndexOptions(
	io *nsproto.IndexOptions,
	defaultBlockSize time.Duration,
) (IndexOptions, error) {
	iopts := NewIndexOptions().
		SetBlockSize(defaultBlockSize).
		SetEnabled(false)
	if io == nil {
		return iopts, nil
	}

	iopts = iopts.SetEnabled(io.Enabled).
		SetBlockSize(FromNanos(io.BlockSizeNanos))

	return iopts, nil
}

// ToRuntimeOptions converts nsproto.NamespaceRuntimeOptions to RuntimeOptions.
func ToRuntimeOptions(
	opts *nsproto.NamespaceRuntimeOptions,
) (RuntimeOptions, error) {
	runtimeOpts := NewRuntimeOptions()
	if opts == nil {
		return runtimeOpts, nil
	}
	if v := opts.WriteIndexingPerCPUConcurrency; v != nil {
		newValue := v.Value
		runtimeOpts = runtimeOpts.SetWriteIndexingPerCPUConcurrency(&newValue)
	}
	if v := opts.FlushIndexingPerCPUConcurrency; v != nil {
		newValue := v.Value
		runtimeOpts = runtimeOpts.SetFlushIndexingPerCPUConcurrency(&newValue)
	}
	return runtimeOpts, nil
}

// ExtendedOptsConverter is function for converting from protobuf message to ExtendedOptions.
type ExtendedOptsConverter func(p *protobuftypes.Struct) (ExtendedOptions, error)

// RegisterExtendedOptionsConverter registers conversion function from protobuf message to ExtendedOptions.
func RegisterExtendedOptionsConverter(_type string, converter ExtendedOptsConverter) {
	dynamicExtendedOptionsConverters.Store(_type, converter)
}

// ToExtendedOptions converts protobuf message to ExtendedOptions.
func ToExtendedOptions(
	extendedOptsProto *nsproto.ExtendedOptions,
) (ExtendedOptions, error) {
	var extendedOpts ExtendedOptions
	if extendedOptsProto == nil {
		return extendedOpts, nil
	}

	converter, ok := dynamicExtendedOptionsConverters.Load(extendedOptsProto.Type)
	if !ok {
		return nil, fmt.Errorf("dynamic ExtendedOptions converter not registered for type %s", extendedOptsProto.Type)
	}

	if extendedOptsProto.Options == nil {
		return nil, errExtendedOptionsNil
	}

	extendedOpts, err := converter.(ExtendedOptsConverter)(extendedOptsProto.Options)
	if err != nil {
		return nil, err
	}

	if err = extendedOpts.Validate(); err != nil {
		return nil, err
	}

	return extendedOpts, nil
}

// ToMetadata converts nsproto.Options to Metadata
func ToMetadata(
	id string,
	opts *nsproto.NamespaceOptions,
) (Metadata, error) {
	if opts == nil {
		return nil, errNamespaceNil
	}

	rOpts, err := ToRetention(opts.RetentionOptions)
	if err != nil {
		return nil, err
	}

	iOpts, err := ToIndexOptions(opts.IndexOptions,
		// Default to the retention block size if no index options are specified.
		rOpts.BlockSize(),
	)
	if err != nil {
		return nil, err
	}

	sr, err := LoadSchemaHistory(opts.GetSchemaOptions())
	if err != nil {
		return nil, err
	}

	runtimeOpts, err := ToRuntimeOptions(opts.RuntimeOptions)
	if err != nil {
		return nil, err
	}

	extendedOpts, err := ToExtendedOptions(opts.ExtendedOptions)
	if err != nil {
		return nil, err
	}

	aggOpts, err := ToAggregationOptions(opts.AggregationOptions)
	if err != nil {
		return nil, err
	}

	stagingState, err := ToStagingState(opts.StagingState)
	if err != nil {
		return nil, err
	}

	mOpts := NewOptions().
		SetBootstrapEnabled(opts.BootstrapEnabled).
		SetFlushEnabled(opts.FlushEnabled).
		SetCleanupEnabled(opts.CleanupEnabled).
		SetRepairEnabled(opts.RepairEnabled).
		SetWritesToCommitLog(opts.WritesToCommitLog).
		SetSnapshotEnabled(opts.SnapshotEnabled).
		SetSchemaHistory(sr).
		SetRetentionOptions(rOpts).
		SetIndexOptions(iOpts).
		SetColdWritesEnabled(opts.ColdWritesEnabled).
		SetRuntimeOptions(runtimeOpts).
		SetExtendedOptions(extendedOpts).
		SetAggregationOptions(aggOpts).
		SetStagingState(stagingState)

	if opts.CacheBlocksOnRetrieve != nil {
		mOpts = mOpts.SetCacheBlocksOnRetrieve(opts.CacheBlocksOnRetrieve.Value)
	}

	if err := mOpts.Validate(); err != nil {
		return nil, err
	}

	return NewMetadata(ident.StringID(id), mOpts)
}

// ToStagingState converts nsproto.StagingState to StagingState.
func ToStagingState(state *nsproto.StagingState) (StagingState, error) {
	if state == nil {
		return StagingState{}, nil
	}

	return NewStagingState(state.Status)
}

// ToAggregationOptions converts nsproto.AggregationOptions to AggregationOptions.
func ToAggregationOptions(opts *nsproto.AggregationOptions) (AggregationOptions, error) {
	aggOpts := NewAggregationOptions()
	if opts == nil || len(opts.Aggregations) == 0 {
		return aggOpts, nil
	}
	aggregations := make([]Aggregation, 0, len(opts.Aggregations))
	for _, agg := range opts.Aggregations {
		if agg.Aggregated {
			if agg.Attributes == nil {
				return nil, errors.New("must set Attributes when aggregated is true")
			}

			var dsOpts DownsampleOptions
			if agg.Attributes.DownsampleOptions == nil {
				dsOpts = NewDownsampleOptions(true)
			} else {
				dsOpts = NewDownsampleOptions(agg.Attributes.DownsampleOptions.All)
			}

			attrs, err := NewAggregatedAttributes(time.Duration(agg.Attributes.ResolutionNanos), dsOpts)
			if err != nil {
				return nil, err
			}
			aggregations = append(aggregations, NewAggregatedAggregation(attrs))
		} else {
			aggregations = append(aggregations, NewUnaggregatedAggregation())
		}
	}
	return aggOpts.SetAggregations(aggregations), nil
}

// ToProto converts Map to nsproto.Registry
func ToProto(m Map) (*nsproto.Registry, error) {
	reg := nsproto.Registry{
		Namespaces: make(map[string]*nsproto.NamespaceOptions, len(m.Metadatas())),
	}

	for _, md := range m.Metadatas() {
		protoMsg, err := OptionsToProto(md.Options())
		if err != nil {
			return nil, err
		}
		reg.Namespaces[md.ID().String()] = protoMsg
	}

	return &reg, nil
}

// FromProto converts nsproto.Registry -> Map
func FromProto(protoRegistry nsproto.Registry) (Map, error) {
	metadatas := make([]Metadata, 0, len(protoRegistry.Namespaces))
	for ns, opts := range protoRegistry.Namespaces {
		md, err := ToMetadata(ns, opts)
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, md)
	}
	return NewMap(metadatas)
}

// OptionsToProto converts Options -> nsproto.NamespaceOptions
func OptionsToProto(opts Options) (*nsproto.NamespaceOptions, error) {
	var extendedOpts *nsproto.ExtendedOptions
	if extOpts := opts.ExtendedOptions(); extOpts != nil {
		extOptsType, extOptsStruct := extOpts.ToProto()
		extendedOpts = &nsproto.ExtendedOptions{
			Type:    extOptsType,
			Options: extOptsStruct,
		}
	}

	ropts := opts.RetentionOptions()
	iopts := opts.IndexOptions()

	stagingState, err := toProtoStagingState(opts.StagingState())
	if err != nil {
		return nil, err
	}

	nsOpts := &nsproto.NamespaceOptions{
		BootstrapEnabled:  opts.BootstrapEnabled(),
		FlushEnabled:      opts.FlushEnabled(),
		CleanupEnabled:    opts.CleanupEnabled(),
		SnapshotEnabled:   opts.SnapshotEnabled(),
		RepairEnabled:     opts.RepairEnabled(),
		WritesToCommitLog: opts.WritesToCommitLog(),
		SchemaOptions:     toSchemaOptions(opts.SchemaHistory()),
		RetentionOptions: &nsproto.RetentionOptions{
			BlockSizeNanos:                           ropts.BlockSize().Nanoseconds(),
			RetentionPeriodNanos:                     ropts.RetentionPeriod().Nanoseconds(),
			FutureRetentionPeriodNanos:               ropts.FutureRetentionPeriod().Nanoseconds(),
			BufferFutureNanos:                        ropts.BufferFuture().Nanoseconds(),
			BufferPastNanos:                          ropts.BufferPast().Nanoseconds(),
			BlockDataExpiry:                          ropts.BlockDataExpiry(),
			BlockDataExpiryAfterNotAccessPeriodNanos: ropts.BlockDataExpiryAfterNotAccessedPeriod().Nanoseconds(),
		},
		IndexOptions: &nsproto.IndexOptions{
			Enabled:        iopts.Enabled(),
			BlockSizeNanos: iopts.BlockSize().Nanoseconds(),
		},
		ColdWritesEnabled:     opts.ColdWritesEnabled(),
		RuntimeOptions:        toRuntimeOptions(opts.RuntimeOptions()),
		CacheBlocksOnRetrieve: &protobuftypes.BoolValue{Value: opts.CacheBlocksOnRetrieve()},
		ExtendedOptions:       extendedOpts,
		AggregationOptions:    toProtoAggregationOptions(opts.AggregationOptions()),
		StagingState:          stagingState,
	}

	return nsOpts, nil
}

func toProtoStagingState(state StagingState) (*nsproto.StagingState, error) {
	var protoStatus nsproto.StagingStatus
	switch state.Status() {
	case UnknownStagingStatus:
		protoStatus = nsproto.StagingStatus_UNKNOWN
	case InitializingStagingStatus:
		protoStatus = nsproto.StagingStatus_INITIALIZING
	case ReadyStagingStatus:
		protoStatus = nsproto.StagingStatus_READY
	default:
		return nil, fmt.Errorf("invalid StagingState: %v", state.Status())
	}

	return &nsproto.StagingState{Status: protoStatus}, nil
}

func toProtoAggregationOptions(aggOpts AggregationOptions) *nsproto.AggregationOptions {
	if aggOpts == nil || len(aggOpts.Aggregations()) == 0 {
		return nil
	}
	protoAggs := make([]*nsproto.Aggregation, 0, len(aggOpts.Aggregations()))
	for _, agg := range aggOpts.Aggregations() {
		protoAgg := nsproto.Aggregation{Aggregated: agg.Aggregated}
		if agg.Aggregated {
			protoAgg.Attributes = &nsproto.AggregatedAttributes{
				ResolutionNanos:   agg.Attributes.Resolution.Nanoseconds(),
				DownsampleOptions: &nsproto.DownsampleOptions{All: agg.Attributes.DownsampleOptions.All},
			}
		}
		protoAggs = append(protoAggs, &protoAgg)
	}
	return &nsproto.AggregationOptions{Aggregations: protoAggs}
}

// toRuntimeOptions returns the corresponding RuntimeOptions proto.
func toRuntimeOptions(opts RuntimeOptions) *nsproto.NamespaceRuntimeOptions {
	if opts == nil || opts.IsDefault() {
		return nil
	}
	var (
		writeIndexingPerCPUConcurrency *protobuftypes.DoubleValue
		flushIndexingPerCPUConcurrency *protobuftypes.DoubleValue
	)
	if v := opts.WriteIndexingPerCPUConcurrency(); v != nil {
		writeIndexingPerCPUConcurrency = &protobuftypes.DoubleValue{
			Value: *v,
		}
	}
	if v := opts.FlushIndexingPerCPUConcurrency(); v != nil {
		flushIndexingPerCPUConcurrency = &protobuftypes.DoubleValue{
			Value: *v,
		}
	}
	return &nsproto.NamespaceRuntimeOptions{
		WriteIndexingPerCPUConcurrency: writeIndexingPerCPUConcurrency,
		FlushIndexingPerCPUConcurrency: flushIndexingPerCPUConcurrency,
	}
}

func typeUrlForMessage(typeURLPrefix string, msg proto.Message) string {
	if !strings.HasSuffix(typeURLPrefix, "/") {
		typeURLPrefix += "/"
	}
	return typeURLPrefix + proto.MessageName(msg)
}
