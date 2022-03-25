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

package remote

import (
	"fmt"

	"google.golang.org/grpc/resolver"
)

const (
	_staticResolverSchema = "static"
	// staticResolverURL is the same regardless of which endpoints are being dialed. The expectation
	// is that callers use grpc.WithResolver on a per grpc.Dial basis in order to get correct routing.
	_staticResolverURL = _staticResolverSchema + ":///"
)

// Assert we implement resolver.Builder
var _ resolver.Builder = (*staticResolverBuilder)(nil)

// staticResolverBuilder implements resolver.Builder.
// It constructs a staticResolver, which:
//  - Always routes to a static list of endpoints (never updates)
//  - Performs round-robin load balancing between them.
type staticResolverBuilder struct {
	addresses []string
}

func newStaticResolverBuilder(addresses []string) *staticResolverBuilder {
	return &staticResolverBuilder{addresses: addresses}
}

func (b *staticResolverBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	addrs := make([]resolver.Address, 0, len(b.addresses))
	for _, a := range b.addresses {
		addrs = append(addrs, resolver.Address{Addr: a})
	}
	r := staticResolver{}

	// N.B.: we are configuring the service config using as described in
	// https://github.com/grpc/grpc/blob/master/doc/service_config.md.
	// Technically speaking, this JSON is defined by the proto structure ServiceConfig in:
	// https://github.com/grpc/grpc-proto/blob/master/grpc/service_config/service_config.proto
	// However, the grpc-go package doesn't include an exposed, generated version of that protobuf type,
	// despite the fact that it very clearly relies on a specific version of it (e.g. it would fail if
	// we had a separate version of the .proto file).
	// Therefore, follow the example of:
	// https://github.com/grpc/grpc-go/blob/master/examples/features/load_balancing/client/main.go#L75-L75
	// and use a static config JSON representation here.
	const pfc = `{"loadBalancingConfig": [{"round_robin":{}}]}`
	scpr := cc.ParseServiceConfig(pfc)
	if scpr.Err != nil {
		return nil, fmt.Errorf(
			"failed to parse static service config for GRPC: %w. Config was: %q",
			scpr.Err,
			pfc,
		)
	}
	if err := cc.UpdateState(resolver.State{
		Addresses:     addrs,
		ServiceConfig: scpr,
	}); err != nil {
		return nil, fmt.Errorf("failed to update connection state while building resolver: %w", err)
	}
	return r, nil
}

func (b *staticResolverBuilder) Scheme() string { return _staticResolverSchema }

var _ resolver.Resolver = staticResolver{}

// staticResolver implements resolver.Resolver
type staticResolver struct{}

func (r staticResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r staticResolver) Close() {}
