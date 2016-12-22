// Copyright (c) 2016 Uber Technologies, Inc.
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

package msgpack

import (
	"errors"

	"github.com/m3db/m3metrics/pool"
	xpool "github.com/m3db/m3x/pool"
)

const (
	// Whether the iterator should ignore higher-than-supported version
	// by default for unaggregated metrics
	defaultUnaggregatedIgnoreHigherVersion = true

	// Whether the iterator should ignore higher-than-supported version
	// by default for aggregated metrics
	defaultAggregatedIgnoreHigherVersion = true
)

var (
	errNoFloatsPool   = errors.New("no floats pool")
	errNoPoliciesPool = errors.New("no policies pool")
)

type unaggregatedIteratorOptions struct {
	ignoreHigherVersion bool
	floatsPool          xpool.FloatsPool
	policiesPool        pool.PoliciesPool
}

// NewUnaggregatedIteratorOptions creates a new set of unaggregated iterator options
func NewUnaggregatedIteratorOptions() UnaggregatedIteratorOptions {
	floatsPool := xpool.NewFloatsPool(nil, nil)
	floatsPool.Init()

	policiesPool := pool.NewPoliciesPool(nil, nil)
	policiesPool.Init()

	return unaggregatedIteratorOptions{
		ignoreHigherVersion: defaultUnaggregatedIgnoreHigherVersion,
		floatsPool:          floatsPool,
		policiesPool:        policiesPool,
	}
}

func (o unaggregatedIteratorOptions) SetIgnoreHigherVersion(value bool) UnaggregatedIteratorOptions {
	opts := o
	opts.ignoreHigherVersion = value
	return opts
}

func (o unaggregatedIteratorOptions) IgnoreHigherVersion() bool {
	return o.ignoreHigherVersion
}

func (o unaggregatedIteratorOptions) SetFloatsPool(value xpool.FloatsPool) UnaggregatedIteratorOptions {
	opts := o
	opts.floatsPool = value
	return opts
}

func (o unaggregatedIteratorOptions) FloatsPool() xpool.FloatsPool {
	return o.floatsPool
}

func (o unaggregatedIteratorOptions) SetPoliciesPool(value pool.PoliciesPool) UnaggregatedIteratorOptions {
	opts := o
	opts.policiesPool = value
	return opts
}

func (o unaggregatedIteratorOptions) PoliciesPool() pool.PoliciesPool {
	return o.policiesPool
}

func (o unaggregatedIteratorOptions) Validate() error {
	if o.floatsPool == nil {
		return errNoFloatsPool
	}
	if o.policiesPool == nil {
		return errNoPoliciesPool
	}
	return nil
}

type aggregatedIteratorOptions struct {
	ignoreHigherVersion bool
}

// NewAggregatedIteratorOptions creates a new set of aggregated iterator options
func NewAggregatedIteratorOptions() AggregatedIteratorOptions {
	return aggregatedIteratorOptions{
		ignoreHigherVersion: defaultAggregatedIgnoreHigherVersion,
	}
}

func (o aggregatedIteratorOptions) SetIgnoreHigherVersion(value bool) AggregatedIteratorOptions {
	opts := o
	opts.ignoreHigherVersion = value
	return opts
}

func (o aggregatedIteratorOptions) IgnoreHigherVersion() bool {
	return o.ignoreHigherVersion
}
