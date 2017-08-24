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

package convert

import (
	"errors"
	"time"

	nsproto "github.com/m3db/m3db/generated/proto/namespace"
	"github.com/m3db/m3db/retention"
	"github.com/m3db/m3db/storage/namespace"
	"github.com/m3db/m3db/ts"
	xtime "github.com/m3db/m3x/time"
)

var (
	errRetentionNil = errors.New("retention options must be set")
	errNamespaceNil = errors.New("namespace options must be set")
)

// ToRetention converts nsproto.RetentionOptions to retention.Options
func ToRetention(
	ro *nsproto.RetentionOptions,
) (retention.Options, error) {
	if ro == nil {
		return nil, errRetentionNil
	}

	fromMins := func(n int64) time.Duration {
		return xtime.FromNormalizedDuration(n, time.Minute)
	}

	ropts := retention.NewOptions().
		SetRetentionPeriod(fromMins(ro.RetentionPeriodMins)).
		SetBlockSize(fromMins(ro.BlockSizeMins)).
		SetBufferFuture(fromMins(ro.BufferFutureMins)).
		SetBufferPast(fromMins(ro.BufferPastMins)).
		SetBlockDataExpiry(ro.BlockDataExpiry).
		SetBlockDataExpiryAfterNotAccessedPeriod(
			fromMins(ro.BlockDataExpiryAfterNotAccessPeriodMins))

	if err := ropts.Validate(); err != nil {
		return nil, err
	}

	return ropts, nil
}

// ToMetadata converts nsproto.Options to Metadata
func ToMetadata(
	id string,
	opts *nsproto.NamespaceOptions,
) (namespace.Metadata, error) {
	if opts == nil {
		return nil, errNamespaceNil
	}

	ropts, err := ToRetention(opts.RetentionOptions)
	if err != nil {
		return nil, err
	}

	mopts := namespace.NewOptions().
		SetNeedsBootstrap(opts.NeedsBootstrap).
		SetNeedsFlush(opts.NeedsFlush).
		SetNeedsFilesetCleanup(opts.NeedsFilesetCleanup).
		SetNeedsRepair(opts.NeedsRepair).
		SetWritesToCommitLog(opts.WritesToCommitLog).
		SetRetentionOptions(ropts)

	return namespace.NewMetadata(ts.StringID(id), mopts)
}

// ToProto converts namespace.Map to nsproto.Registry
func ToProto(m namespace.Map) *nsproto.Registry {
	reg := nsproto.Registry{
		Namespaces: make(map[string]*nsproto.NamespaceOptions, len(m.Metadatas())),
	}

	toMins := func(t time.Duration) int64 {
		return xtime.ToNormalizedDuration(t, time.Minute)
	}

	for _, md := range m.Metadatas() {
		ropts := md.Options().RetentionOptions()
		reg.Namespaces[md.ID().String()] = &nsproto.NamespaceOptions{
			NeedsBootstrap:      md.Options().NeedsBootstrap(),
			NeedsFlush:          md.Options().NeedsFlush(),
			NeedsFilesetCleanup: md.Options().NeedsFilesetCleanup(),
			NeedsRepair:         md.Options().NeedsRepair(),
			WritesToCommitLog:   md.Options().WritesToCommitLog(),
			RetentionOptions: &nsproto.RetentionOptions{
				BlockSizeMins:                           toMins(ropts.BlockSize()),
				RetentionPeriodMins:                     toMins(ropts.RetentionPeriod()),
				BufferFutureMins:                        toMins(ropts.BufferFuture()),
				BufferPastMins:                          toMins(ropts.BufferPast()),
				BlockDataExpiry:                         ropts.BlockDataExpiry(),
				BlockDataExpiryAfterNotAccessPeriodMins: toMins(ropts.BlockDataExpiryAfterNotAccessedPeriod()),
			},
		}
	}

	return &reg
}

// FromProto converts nsproto.Registry -> namespace.Map
func FromProto(protoRegistry nsproto.Registry) (namespace.Map, error) {
	metadatas := make([]namespace.Metadata, 0, len(protoRegistry.Namespaces))
	for ns, opts := range protoRegistry.Namespaces {
		md, err := ToMetadata(ns, opts)
		if err != nil {
			return nil, err
		}
		metadatas = append(metadatas, md)
	}
	return namespace.NewMap(metadatas)
}
