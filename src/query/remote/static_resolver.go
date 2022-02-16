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
	"google.golang.org/grpc/resolver"
)

const _schema = "static"

type (
	// staticResolverBuilder implements resolver.Builder
	staticResolverBuilder struct {
		addresses []string
	}
	// staticResolver implements resolver.Resolver
	staticResolver struct {
		target    resolver.Target
		cc        resolver.ClientConn
		addresses []resolver.Address
	}
)

func (b *staticResolverBuilder) Build(
	target resolver.Target,
	cc resolver.ClientConn,
	opts resolver.BuildOptions,
) (resolver.Resolver, error) {
	addrs := make([]resolver.Address, len(b.addresses))
	for _, a := range b.addresses {
		addrs = append(addrs, resolver.Address{Addr: a})
	}
	r := &staticResolver{
		target:    target,
		cc:        cc,
		addresses: addrs,
	}
	r.cc.UpdateState(resolver.State{Addresses: addrs})
	return r, nil
}

func (b *staticResolverBuilder) Scheme() string { return _schema }

func (r *staticResolver) ResolveNow(resolver.ResolveNowOptions) {}

func (r *staticResolver) Close() {}

func registerStaticResolver(addresses []string) {
	b := &staticResolverBuilder{
		addresses: addresses,
	}
	resolver.Register(b)
}
